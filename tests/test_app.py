import tempfile
import threading
import unittest
from pathlib import Path

from app import Database, DomainError, seed_demo


class WaterRightsFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def test_transfer_approval_usage_and_drought(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        transfer = self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 400, "effective_date": "2026-06-01"}, "editor")
        self.assertEqual(self.db.available(source)["reserved_outgoing"], 400)
        approved = self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        self.assertEqual(approved["status"], "approved")
        usage = self.db.record_usage("meter-01", {"account_id": source, "amount": 150, "meter_event_id": "UP-JUL-1", "occurred_at": "2026-07-10"}, "meter")
        self.assertEqual(usage["amount"], 150)
        simulation = self.db.simulate_drought(1000, 0.3)
        self.assertAlmostEqual(sum(x["allocation"] for x in simulation["allocations"]) + simulation["unallocated"], 700)

    def test_duplicate_meter_event_and_pending_reservation(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 500, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaises(DomainError):
            self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 1, "effective_date": "2026-06-01"}, "editor")
        self.db.record_usage("meter-01", {"account_id": source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")
        with self.assertRaisesRegex(DomainError, "不能重复计水"):
            self.db.record_usage("meter-01", {"account_id": source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")

    def test_third_party_and_self_approval_conflicts(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        with self.assertRaisesRegex(DomainError, "最小留存"):
            self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 501, "effective_date": "2026-06-01"}, "editor")
        transfer = self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 100, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaisesRegex(DomainError, "不能批准自己"):
            self.db.approve_transfer(transfer["id"], "alice", "reviewer")


class DispatchLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source, self.target = self.accounts["北区水库"], self.accounts["河口灌区"]

    def tearDown(self):
        self.tmp.cleanup()

    def _order(self, limit=850, region="upstream", effective="2026-01-01T00:00:00"):
        return self.db.create_dispatch(
            "carol", {"region": region, "limit_amount": limit, "effective_at": effective}, "dispatcher")

    def _transfer(self, amount, status="approved"):
        t = self.db.create_transfer(
            "alice", {"from_account_id": self.source, "to_account_id": self.target,
                      "amount": amount, "effective_date": "2026-06-01"}, "editor")
        if status in {"approved", "executed"}:
            self.db.approve_transfer(t["id"], "bob", "reviewer")
        if status == "executed":
            self.db.execute_transfer(t["id"], "alice", "editor")
        return t["id"]

    def _transfer_row(self, transfer_id):
        return next(t for t in self.db.list_transfers() if t["id"] == transfer_id)

    def test_publish_records_scope_and_recalculates_available(self):
        approved_id = self._transfer(400)
        pending_id = self._transfer(100, status="pending")
        order = self._order(limit=850)
        published = self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        # 发布时记录区域、限额和生效时刻
        self.assertEqual(published["region"], "upstream")
        self.assertEqual(published["limit_amount"], 850)
        self.assertEqual(published["effective_at"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(published["version"], 1)
        # 已批准未执行的先冻结，待审转让按发布版本重新占用
        self.assertEqual(self._transfer_row(approved_id)["status"], "frozen")
        self.assertEqual(self._transfer_row(pending_id)["dispatch_version"], 1)
        # 受影响账户可用量立即重算：min(1000,850) - 100已用 - 500占用
        avail = self.db.available(self.source)
        self.assertEqual(avail["available"], 250)
        self.assertEqual(avail["dispatch_limit"], 850)
        self.assertEqual(avail["dispatch_version"], 1)

    def test_publish_caps_usage_and_transfer_checks(self):
        order = self._order(limit=150)
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        # 已用 100，限额 150，剩余 50：超额取水和转让都不能再按旧依据放行
        with self.assertRaisesRegex(DomainError, "超过可用额度"):
            self.db.record_usage("meter-01", {"account_id": self.source, "amount": 60,
                                              "meter_event_id": "D-1", "occurred_at": "2026-08-01"}, "meter")
        with self.assertRaisesRegex(DomainError, "可用额度不足"):
            self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                              "amount": 60, "effective_date": "2026-06-01"}, "editor")
        ok = self.db.record_usage("meter-01", {"account_id": self.source, "amount": 50,
                                               "meter_event_id": "D-2", "occurred_at": "2026-08-01"}, "meter")
        self.assertEqual(ok["dispatch_version"], 1)

    def test_executed_and_usage_keep_basis_excess_goes_to_review(self):
        executed_id = self._transfer(400, status="executed")
        order = self._order(limit=50)  # 已取水 100 > 限额 50
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        # 已执行的转让保留当时依据，不被冻结
        row = self._transfer_row(executed_id)
        self.assertEqual(row["status"], "executed")
        # 已取水的差额 100 - 50 = 50 转待复核
        reviews = self.db.list_review_items()
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["entity_id"], self.source)
        self.assertAlmostEqual(reviews[0]["amount"], 50)
        self.assertEqual(reviews[0]["status"], "pending_review")

    def test_old_rows_backfilled_with_pre_publish_version(self):
        rejected_id = self._transfer(100, status="pending")
        self.db.reject_transfer(rejected_id, "bob", "reviewer")
        order = self._order(limit=900)
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        # 首次发布前没有调度版本，旧数据按发布前口径回填为 0
        self.assertEqual(self._transfer_row(rejected_id)["dispatch_version"], 0)
        with self.db.connect() as conn:
            usage = conn.execute("SELECT dispatch_version FROM usage_records WHERE meter_event_id='UP-2026-0001'").fetchone()
        self.assertEqual(usage["dispatch_version"], 0)
        # 再发一版时，新待审转让占用新版本，旧编号不被覆盖
        pending_id = self._transfer(50, status="pending")
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=1,
                                 updates={"limit_amount": 800})
        self.assertEqual(self._transfer_row(pending_id)["dispatch_version"], 2)
        self.assertEqual(self._transfer_row(rejected_id)["dispatch_version"], 0)

    def test_concurrent_publish_first_write_wins(self):
        order = self._order()
        barrier = threading.Barrier(2)
        results = {}

        def publish(name):
            barrier.wait()
            try:
                self.db.publish_dispatch(order["id"], name, "dispatcher", expected_version=0)
                results[name] = "ok"
            except DomainError as exc:
                results[name] = str(exc)

        threads = [threading.Thread(target=publish, args=(n,)) for n in ("carol", "dave")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 首份写入生效，另一位看到冲突
        self.assertEqual(sorted(results.values())[0], "ok")
        self.assertTrue(any("冲突" in v for v in results.values()))
        final = self.db.get_dispatch(order["id"])
        self.assertEqual(final["version"], 1)
        self.assertEqual(final["status"], "published")

    def test_stale_expected_version_conflict_keeps_draft(self):
        order = self._order()
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        with self.assertRaisesRegex(DomainError, "冲突"):
            self.db.publish_dispatch(order["id"], "dave", "dispatcher", expected_version=0,
                                     updates={"limit_amount": 10})
        # 冲突方的修改没有落库，首份内容保留
        final = self.db.get_dispatch(order["id"])
        self.assertEqual(final["limit_amount"], 850)
        self.assertEqual(final["version"], 1)

    def test_recover_after_failed_write_replays_without_duplicates(self):
        approved_id = self._transfer(400)
        order = self._order(limit=900)
        with self.assertRaisesRegex(DomainError, "写库失败"):
            self.db.publish_dispatch(order["id"], "carol", "dispatcher",
                                     expected_version=0, fail_after_checkpoint=True)
        # 检查点已写入但效果未应用
        self.assertEqual(self._transfer_row(approved_id)["status"], "approved")
        self.assertEqual(self.db.get_dispatch(order["id"])["checkpoint_applied"], 0)
        recovered = self.db.recover_dispatch(order["id"], "carol", "dispatcher")
        self.assertTrue(recovered["recovered"])
        self.assertEqual(self._transfer_row(approved_id)["status"], "frozen")
        # 重放不新增冻结记录或审计
        freezes = len(self.db.list_freeze_records())
        audits = len(self.db.audit())
        again = self.db.recover_dispatch(order["id"], "carol", "dispatcher")
        self.assertFalse(again["recovered"])
        self.assertEqual(len(self.db.list_freeze_records()), freezes)
        self.assertEqual(len(self.db.audit()), audits)

    def test_revoke_requires_supervisor_and_unfreezes(self):
        approved_id = self._transfer(400)
        order = self._order(limit=900)
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        self.assertEqual(self._transfer_row(approved_id)["status"], "frozen")
        # 普通账户越权撤销被拒绝
        for role in ("editor", "reviewer", "viewer", "dispatcher"):
            with self.assertRaisesRegex(DomainError, "调度主管"):
                self.db.revoke_dispatch(order["id"], "mallory", role, expected_version=1)
        revoked = self.db.revoke_dispatch(order["id"], "sara", "supervisor", expected_version=1)
        self.assertEqual(revoked["status"], "revoked")
        # 撤销后冻结解除、限额不再约束可用量
        self.assertEqual(self._transfer_row(approved_id)["status"], "approved")
        self.assertIsNone(self.db.available(self.source)["dispatch_limit"])
        # 并发撤销同样只有首份生效
        with self.assertRaisesRegex(DomainError, "未发布或已撤销"):
            self.db.revoke_dispatch(order["id"], "sara", "supervisor", expected_version=1)

    def test_execute_moves_quota_and_frozen_blocks_execution(self):
        transfer_id = self._transfer(400)
        executed = self.db.execute_transfer(transfer_id, "alice", "editor")
        self.assertEqual(executed["status"], "executed")
        accounts = {a["id"]: a for a in self.db.list_accounts()}
        self.assertEqual(accounts[self.source]["quota"], 600)
        self.assertEqual(accounts[self.target]["quota"], 900)
        with self.assertRaisesRegex(DomainError, "已批准未执行"):
            self.db.execute_transfer(transfer_id, "alice", "editor")
        frozen_id = self._transfer(100)
        order = self._order(limit=900)
        self.db.publish_dispatch(order["id"], "carol", "dispatcher", expected_version=0)
        with self.assertRaisesRegex(DomainError, "冻结"):
            self.db.execute_transfer(frozen_id, "alice", "editor")

    def test_dispatch_roles_and_future_effective(self):
        with self.assertRaisesRegex(DomainError, "只有调度员"):
            self.db.create_dispatch("alice", {"region": "upstream", "limit_amount": 1,
                                              "effective_at": "2026-01-01"}, "editor")
        order = self.db.create_dispatch("sara", {"region": "upstream", "limit_amount": 1,
                                                 "effective_at": "2099-01-01"}, "supervisor")
        self.db.publish_dispatch(order["id"], "sara", "supervisor", expected_version=0)
        # 生效时刻未到，可用量仍按原口径
        self.assertIsNone(self.db.available(self.source)["dispatch_limit"])


if __name__ == "__main__":
    unittest.main()