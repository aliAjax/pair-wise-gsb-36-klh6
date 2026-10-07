import os
import tempfile
import threading
import unittest
from pathlib import Path

from app import Database, DomainError
import app as appmod


def _make_db() -> tuple[Database, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory()
    return Database(Path(tmp.name) / "test.db"), tmp


def _account(db, name, region, quota, priority=1):
    return db.create_account("editor", {
        "name": name, "region": region, "holder": name + "持有人",
        "priority": priority, "valid_from": "2026-01-01",
        "valid_to": "2026-12-31", "quota": quota,
    }, "editor")["id"]


class DispatchLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "dispatch.db")
        self.n1 = _account(self.db, "北一", "north", 1000)
        self.n2 = _account(self.db, "北二", "north", 1000, priority=1)
        self.s1 = _account(self.db, "南一", "south", 800, priority=2)

    def tearDown(self):
        self.tmp.cleanup()

    def test_publish_records_region_cap_moment_and_recomputes_availability(self):
        order = self.db.publish_order("dispatcher-1", {
            "region": "north", "cap": 1000,
            "effective_at": "2026-04-01T08:00:00+00:00",
        }, "dispatcher")
        self.assertEqual(order["state"], "published")
        self.assertEqual(order["version"], 1)
        self.assertTrue(order["order_no"].startswith("F-"))
        self.assertEqual(order["cap"], 1000)
        self.assertEqual(order["region"], "north")
        self.assertTrue(order["effective_at"].startswith("2026-04-01T08:00:00"))
        # 区域限额按许可额度比例分到账户：各 500。
        a1 = self.db.available(self.n1)
        a2 = self.db.available(self.n2)
        self.assertEqual(a1["dispatched_limit"], 500)
        self.assertEqual(a2["dispatched_limit"], 500)
        self.assertEqual(a1["basis_order_id"], order["id"])
        # 未受影响区域仍按许可额度。
        self.assertEqual(self.db.available(self.s1)["dispatched_limit"], 800)
        self.assertIsNone(self.db.available(self.s1)["basis_order_id"])

    def test_pending_transfer_re_reserved_against_published_version(self):
        transfer = self.db.create_transfer("editor", {
            "from_account_id": self.n1, "to_account_id": self.s1,
            "amount": 300, "effective_date": "2026-06-01",
        }, "editor")
        # 新限额 500，已占用 300，只剩 200，再发起 250 会被版本化占用挡住。
        self.db.publish_order("d", {"region": "north", "cap": 1000}, "dispatcher")
        a = self.db.available(self.n1)
        self.assertEqual(a["reserved_outgoing"], 300)
        self.assertEqual(a["available"], 200)
        with self.assertRaisesRegex(DomainError, "可用额度不足"):
            self.db.create_transfer("editor", {
                "from_account_id": self.n1, "to_account_id": self.s1,
                "amount": 250, "effective_date": "2026-06-02",
            }, "editor")
        # 待审转让退回后，版本化占用释放。
        self.db.reject_transfer(transfer["id"], "reviewer-1", "reviewer")
        self.assertEqual(self.db.available(self.n1)["reserved_outgoing"], 0)

    def test_approved_unexecuted_frozen_and_blocked_from_executing(self):
        transfer = self.db.create_transfer("editor2", {
            "from_account_id": self.n1, "to_account_id": self.s1,
            "amount": 200, "effective_date": "2026-06-01",
        }, "editor")
        self.db.approve_transfer(transfer["id"], "reviewer-1", "reviewer")
        order = self.db.publish_order("d", {"region": "north", "cap": 1000}, "dispatcher")
        a = self.db.available(self.n1)
        self.assertEqual(a["frozen_outgoing"], 200)
        self.assertEqual(a["reserved_outgoing"], 0)
        with self.db.connect() as conn:
            freeze = conn.execute(
                "SELECT * FROM dispatch_freezes WHERE order_id=? AND transfer_id=?",
                (order["id"], transfer["id"])).fetchone()
        self.assertIsNotNone(freeze)
        self.assertEqual(freeze["state"], "frozen")
        with self.assertRaisesRegex(DomainError, "冻结"):
            self.db.execute_transfer(transfer["id"], "operator", "editor")
        # 撤销调度令后冻结释放，可以执行。
        self.db.revoke_order(order["id"], "supervisor-1", "supervisor")
        self.assertEqual(self.db.available(self.n1)["frozen_outgoing"], 0)
        executed = self.db.execute_transfer(transfer["id"], "operator", "editor")
        self.assertEqual(executed["status"], "executed")

    def test_executed_and_used_keep_basis_shortfall_goes_to_review(self):
        # 区域内执行一笔 700：许可从 1000/1000 变为 300/1700。
        transfer = self.db.create_transfer("editor", {
            "from_account_id": self.n2, "to_account_id": self.n1,
            "amount": 700, "effective_date": "2026-02-01",
        }, "editor")
        self.db.approve_transfer(transfer["id"], "reviewer-1", "reviewer")
        self.db.execute_transfer(transfer["id"], "operator", "editor")
        self.db.record_usage("meter", {
            "account_id": self.n1, "amount": 400,
            "meter_event_id": "U1", "occurred_at": "2026-03-01",
        }, "meter")
        order = self.db.publish_order("d", {"region": "north", "cap": 600}, "dispatcher")
        reviews = self.db.list_reviews("pending")
        # 限额按现许可比例：北一 510、北二 90；北一取水 400 不超，北二已转出 700 超 610。
        self.assertEqual(len(reviews), 1)
        review = reviews[0]
        self.assertAlmostEqual(review["amount"], 610.0)
        self.assertEqual(review["source_table"], "transfer")
        self.assertEqual(review["source_id"], transfer["id"])
        self.assertEqual(review["order_id"], order["id"])
        # 保留当时依据：已执行/已取水仍锚定发布前基线，而不是新令。
        baseline = next(o for o in self.db.list_orders()
                        if o["region"] == "north" and o["state"] == "baseline")
        with self.db.connect() as conn:
            tb = conn.execute("SELECT basis_order_id FROM transfers WHERE id=?",
                              (transfer["id"],)).fetchone()["basis_order_id"]
            ub = conn.execute("SELECT basis_order_id FROM usage_records WHERE meter_event_id='U1'").fetchone()["basis_order_id"]
        self.assertEqual(tb, baseline["id"])
        self.assertEqual(ub, baseline["id"])
        self.assertEqual(review["basis_order_id"], baseline["id"])
        # 差额可被主管复核。
        resolved = self.db.resolve_review(review["id"], "supervisor-1", "supervisor", "confirmed")
        self.assertEqual(resolved["status"], "confirmed")

    def test_concurrent_publish_first_wins_loser_keeps_conflict_draft(self):
        results = []

        def publish(user, cap):
            try:
                order = self.db.publish_order(user, {
                    "region": "north", "cap": cap, "base_version": 0,
                }, "dispatcher")
                results.append(("published", user, order["version"]))
            except DomainError as exc:
                results.append(("conflict", user, exc.status, exc.extra))

        t1 = threading.Thread(target=publish, args=("dispatcher-1", 500))
        t2 = threading.Thread(target=publish, args=("dispatcher-2", 800))
        t1.start(); t2.start(); t1.join(); t2.join()
        statuses = sorted(r[0] for r in results)
        self.assertEqual(statuses, ["conflict", "published"])
        published = [r for r in results if r[0] == "published"]
        conflict = [r for r in results if r[0] == "conflict"][0]
        self.assertEqual(published[0][2], 1)
        self.assertEqual(conflict[2], 409)
        self.assertEqual(conflict[3]["current_version"], 1)
        draft = conflict[3]["draft"]
        self.assertEqual(draft["state"], "draft")
        self.assertIsNotNone(draft["conflict_of"])
        self.assertTrue(draft["order_no"].startswith("D-"))
        live = [o for o in self.db.list_orders()
                if o["state"] == "published" and o["region"] == "north"]
        self.assertEqual(len(live), 1)
        self.assertIn(live[0]["cap"], (500.0, 800.0))

    def test_revoke_authority_and_concurrent_revoke_conflict(self):
        order = self.db.publish_order("d", {"region": "north", "cap": 500}, "dispatcher")
        with self.assertRaisesRegex(DomainError, "调度主管"):
            self.db.revoke_order(order["id"], "ordinary", "editor")
        with self.assertRaisesRegex(DomainError, "调度主管"):
            self.db.revoke_order(order["id"], "dispatcher-1", "dispatcher")
        # 依据版本过期 -> 保留撤销草稿并看到冲突。
        with self.assertRaises(DomainError) as cm:
            self.db.revoke_order(order["id"], "supervisor-1", "supervisor", base_version=99)
        self.assertEqual(cm.exception.status, 409)
        self.assertEqual(cm.exception.extra["draft"]["kind"], "revoke")
        revoked = self.db.revoke_order(order["id"], "supervisor-1", "supervisor")
        self.assertEqual(revoked["state"], "revoked")
        with self.assertRaisesRegex(DomainError, "不能重复撤销"):
            self.db.revoke_order(order["id"], "supervisor-1", "supervisor")
        # 撤销后区域回到许可额度口径。
        self.assertEqual(self.db.available(self.n1)["dispatched_limit"], 1000)

    def test_recover_from_checkpoint_replays_without_duplicate_audit_or_freeze(self):
        transfer = self.db.create_transfer("editor2", {
            "from_account_id": self.n1, "to_account_id": self.s1,
            "amount": 100, "effective_date": "2026-06-01",
        }, "editor")
        self.db.approve_transfer(transfer["id"], "reviewer-1", "reviewer")
        fault = Path(appmod.FAULT_FILE)
        fault.write_text("after_checkpoint")
        try:
            with self.assertRaises(RuntimeError):
                self.db.publish_order("d", {"region": "north", "cap": 400}, "dispatcher")
        finally:
            if fault.exists():
                fault.unlink()
        order = next(o for o in self.db.list_orders()
                     if o["state"] == "published" and o["region"] == "north")
        with self.db.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT applied FROM dispatch_checkpoints WHERE order_id=?",
                (order["id"],)).fetchone()["applied"], 0)
        # 重新打开服务：从检查点恢复。
        recovered_db = Database(Path(self.tmp.name) / "dispatch.db")
        recovered = recovered_db.recover_pending_orders()
        self.assertEqual(recovered, [])  # 构造时已恢复
        with recovered_db.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT applied FROM dispatch_checkpoints WHERE order_id=?",
                (order["id"],)).fetchone()["applied"], 1)
            allocations = conn.execute(
                "SELECT COUNT(*) c FROM dispatch_allocations WHERE order_id=?",
                (order["id"],)).fetchone()["c"]
            freezes = conn.execute(
                "SELECT COUNT(*) c FROM dispatch_freezes WHERE order_id=?",
                (order["id"],)).fetchone()["c"]
            applied_audit = conn.execute(
                "SELECT COUNT(*) c FROM audit_log WHERE idempotency_key=?",
                (f"order:{order['id']}:applied",)).fetchone()["c"]
        self.assertEqual(allocations, 2)
        self.assertEqual(freezes, 1)
        self.assertEqual(applied_audit, 1)
        # 再重放一次不新增冻结或审计。
        recovered_db.recover_pending_orders()
        with recovered_db.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM dispatch_freezes WHERE order_id=?",
                (order["id"],)).fetchone()["c"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM audit_log WHERE idempotency_key=?",
                (f"order:{order['id']}:applied",)).fetchone()["c"], 1)
        # 恢复后账户按令重算：400 平分 200，冻结 100。
        a = recovered_db.available(self.n1)
        self.assertEqual(a["dispatched_limit"], 200)
        self.assertEqual(a["frozen_outgoing"], 100)
        self.assertEqual(a["available"], 100)


class BaselineBackfillTest(unittest.TestCase):
    def test_legacy_data_backfilled_with_pre_publish_baseline(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "legacy.db"
        self._build_legacy_db(db_path)
        db = Database(db_path)
        with db.connect() as conn:
            baseline = conn.execute(
                "SELECT * FROM dispatch_orders WHERE state='baseline'").fetchone()
            self.assertEqual(baseline["order_no"], "PRE-WEST-0000")
            self.assertEqual(baseline["cap"], 300)
            tb = conn.execute(
                "SELECT basis_order_id FROM transfers").fetchall()
            self.assertTrue(all(r[0] == baseline["id"] for r in tb))
            usage = conn.execute("SELECT basis_order_id FROM usage_records").fetchall()
            self.assertTrue(all(r[0] == baseline["id"] for r in usage))
            self.assertIn("basis_order_id",
                          {r["name"] for r in conn.execute("PRAGMA table_info(transfers)")})
        # 回填幂等：再开一次不会新增基线或审计。
        Database(db_path)
        with db.connect() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM dispatch_orders WHERE state='baseline'").fetchone()["c"], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) c FROM audit_log WHERE action='dispatch.backfilled'").fetchone()["c"], 1)
        # 旧账在发布前口径下照常放行。
        info = db.available(1)
        self.assertEqual(info["dispatched_limit"], 300)

    @staticmethod
    def _build_legacy_db(path: Path) -> None:
        import sqlite3
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,region TEXT,
                holder TEXT,priority INTEGER,valid_from TEXT,valid_to TEXT,quota REAL,
                used REAL DEFAULT 0,created_at TEXT);
            CREATE TABLE transfers(id INTEGER PRIMARY KEY AUTOINCREMENT,from_account_id INTEGER,
                to_account_id INTEGER,amount REAL,effective_date TEXT,status TEXT DEFAULT 'pending',
                created_by TEXT,approved_by TEXT,created_at TEXT,approved_at TEXT);
            CREATE TABLE usage_records(id INTEGER PRIMARY KEY AUTOINCREMENT,account_id INTEGER,
                meter_event_id TEXT,amount REAL,occurred_at TEXT,actor TEXT,created_at TEXT,
                UNIQUE(account_id,meter_event_id));
            CREATE TABLE season_rules(id INTEGER PRIMARY KEY AUTOINCREMENT,region TEXT,month INTEGER,
                max_fraction REAL,note TEXT DEFAULT '');
            CREATE TABLE impact_rules(id INTEGER PRIMARY KEY AUTOINCREMENT,source_region TEXT,
                target_region TEXT,min_source_fraction REAL,note TEXT DEFAULT '');
            CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT,action TEXT,
                entity_type TEXT,entity_id INTEGER,details TEXT,created_at TEXT);
            INSERT INTO accounts(name,region,holder,priority,valid_from,valid_to,quota,used,created_at)
                VALUES('旧账户','west','旧持有人',1,'2026-01-01','2026-12-31',300,20,'2026-01-01T00:00:00');
            INSERT INTO transfers(from_account_id,to_account_id,amount,effective_date,created_by,created_at)
                VALUES(1,1,30,'2026-06-01','old','2026-01-01T00:00:00');
            INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at)
                VALUES(1,'OLD-1',20,'2026-03-01','old','2026-03-02T00:00:00');
            """
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
