"""Cross-region water-right allocation, transfer and drought-dispatch ledger.

Standard library only. Dispatch orders (调度令), accounts, transfers, metered
usage and audit share one ledger: publishing an order immediately recomputes
affected account availability, re-reserves pending transfers against the
published version, freezes approved-but-not-executed transfers, and keeps the
basis used by already-executed/used amounts while sending any shortfall to
review. Publication is recovered from an order checkpoint, and replay never
adds duplicate freeze or audit records.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "water_rights.db"

BASELINE_TAG = "PRE"  # 发布前口径基线版本
EPS = 1e-9

# Fault injection: "before_order" breaks before the order/checkpoint txn,
# "after_checkpoint" after it commits but before the ledger effects replay.
FAULT_FILE = os.getenv("WATER_FAULT_FILE", str(ROOT / ".dispatch_fault"))


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_date(value: str, field: str = "日期") -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DomainError(f"{field}必须是 YYYY-MM-DD") from exc


def parse_moment(value: Any, field: str = "生效时刻") -> str:
    text = str(value or "").strip()
    if not text:
        return utcnow()
    try:
        if len(text) == 10:
            return date.fromisoformat(text).isoformat()
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except ValueError as exc:
        raise DomainError(f"{field}必须是 ISO 日期时间") from exc


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400, extra: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.extra = extra or {}


class Database:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = str(path)
        self._init_schema()
        self._migrate_schema()
        self.backfill_baselines()
        self.recover_pending_orders()

    # ------------------------------------------------------------------ schema
    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    region TEXT NOT NULL,
                    holder TEXT NOT NULL,
                    priority INTEGER NOT NULL CHECK(priority BETWEEN 1 AND 5),
                    valid_from TEXT NOT NULL,
                    valid_to TEXT NOT NULL,
                    quota REAL NOT NULL CHECK(quota >= 0),
                    used REAL NOT NULL DEFAULT 0 CHECK(used >= 0),
                    created_at TEXT NOT NULL,
                    CHECK(valid_from <= valid_to)
                );
                CREATE TABLE IF NOT EXISTS transfers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_account_id INTEGER NOT NULL REFERENCES accounts(id),
                    to_account_id INTEGER NOT NULL REFERENCES accounts(id),
                    amount REAL NOT NULL CHECK(amount > 0),
                    effective_date TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_by TEXT NOT NULL,
                    approved_by TEXT,
                    created_at TEXT NOT NULL,
                    approved_at TEXT,
                    executed_at TEXT,
                    basis_order_id INTEGER REFERENCES dispatch_orders(id)
                );
                CREATE TABLE IF NOT EXISTS usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    meter_event_id TEXT NOT NULL,
                    amount REAL NOT NULL CHECK(amount > 0),
                    occurred_at TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    basis_order_id INTEGER REFERENCES dispatch_orders(id),
                    UNIQUE(account_id, meter_event_id)
                );
                CREATE TABLE IF NOT EXISTS season_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region TEXT NOT NULL,
                    month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
                    max_fraction REAL NOT NULL CHECK(max_fraction > 0 AND max_fraction <= 1),
                    note TEXT NOT NULL DEFAULT '',
                    UNIQUE(region, month)
                );
                CREATE TABLE IF NOT EXISTS impact_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_region TEXT NOT NULL,
                    target_region TEXT NOT NULL,
                    min_source_fraction REAL NOT NULL CHECK(min_source_fraction >= 0 AND min_source_fraction <= 1),
                    note TEXT NOT NULL DEFAULT '',
                    UNIQUE(source_region, target_region)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dispatch_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT,
                    region TEXT NOT NULL,
                    cap REAL NOT NULL CHECK(cap >= 0),
                    effective_at TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    state TEXT NOT NULL DEFAULT 'draft',
                    kind TEXT NOT NULL DEFAULT 'order',
                    base_version INTEGER,
                    superseded_by INTEGER,
                    conflict_of INTEGER,
                    conflict_reason TEXT,
                    published_by TEXT,
                    published_at TEXT,
                    revoked_by TEXT,
                    revoked_at TEXT,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_order_no
                    ON dispatch_orders(order_no) WHERE order_no IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_baseline
                    ON dispatch_orders(region) WHERE state='baseline';
                CREATE TABLE IF NOT EXISTS dispatch_allocations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    licensed_quota REAL NOT NULL,
                    dispatched_limit REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(order_id, account_id)
                );
                CREATE TABLE IF NOT EXISTS dispatch_reservations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    transfer_id INTEGER NOT NULL REFERENCES transfers(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    amount REAL NOT NULL CHECK(amount >= 0),
                    state TEXT NOT NULL DEFAULT 'active',
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    UNIQUE(order_id, transfer_id)
                );
                CREATE TABLE IF NOT EXISTS dispatch_freezes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    transfer_id INTEGER NOT NULL REFERENCES transfers(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    amount REAL NOT NULL CHECK(amount > 0),
                    state TEXT NOT NULL DEFAULT 'frozen',
                    created_at TEXT NOT NULL,
                    released_at TEXT,
                    UNIQUE(order_id, transfer_id)
                );
                CREATE TABLE IF NOT EXISTS dispatch_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    source_table TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    basis_order_id INTEGER REFERENCES dispatch_orders(id),
                    amount REAL NOT NULL CHECK(amount > 0),
                    reason TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved_by TEXT,
                    resolved_at TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(order_id, source_table, source_id)
                );
                CREATE TABLE IF NOT EXISTS dispatch_checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    stage TEXT NOT NULL,
                    applied INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                """
            )

    def _migrate_schema(self) -> None:
        """Add dispatch-aware columns to legacy databases."""
        with self.connect() as conn:
            have = {r["name"] for r in conn.execute("PRAGMA table_info(transfers)")}
            for col, ddl in (("executed_at", "TEXT"), ("basis_order_id", "INTEGER")):
                if col not in have:
                    conn.execute(f"ALTER TABLE transfers ADD COLUMN {col} {ddl}")
            have = {r["name"] for r in conn.execute("PRAGMA table_info(usage_records)")}
            if "basis_order_id" not in have:
                conn.execute("ALTER TABLE usage_records ADD COLUMN basis_order_id INTEGER")
            have = {r["name"] for r in conn.execute("PRAGMA table_info(audit_log)")}
            if "idempotency_key" not in have:
                conn.execute("ALTER TABLE audit_log ADD COLUMN idempotency_key TEXT")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_idem "
                "ON audit_log(idempotency_key) WHERE idempotency_key IS NOT NULL"
            )

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any],
               idempotency_key: str | None = None) -> bool:
        """Insert an audit row. Returns False when the idempotency key already exists."""
        if idempotency_key:
            exists = conn.execute(
                "SELECT 1 FROM audit_log WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if exists:
                return False
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at,idempotency_key) "
            "VALUES(?,?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id,
             json.dumps(details, ensure_ascii=False), utcnow(), idempotency_key),
        )
        return True

    # ------------------------------------------------------------- dispatch core
    def _fault_point(self, stage: str) -> None:
        if Path(FAULT_FILE).exists():
            try:
                wanted = Path(FAULT_FILE).read_text().strip()
            except OSError:
                wanted = ""
            if not wanted or wanted == stage:
                raise RuntimeError(f"injected fault at {stage}")

    def _ensure_region_baseline(self, conn: sqlite3.Connection, region: str) -> int:
        row = conn.execute(
            "SELECT * FROM dispatch_orders WHERE region=? AND state='baseline'", (region,)
        ).fetchone()
        if row:
            licensed = float(
                conn.execute("SELECT COALESCE(SUM(quota),0) s FROM accounts WHERE region=?", (region,))
                .fetchone()["s"]
            )
            if abs(float(row["cap"]) - licensed) > EPS:
                conn.execute("UPDATE dispatch_orders SET cap=? WHERE id=?", (licensed, row["id"]))
            return int(row["id"])
        licensed = float(
            conn.execute("SELECT COALESCE(SUM(quota),0) s FROM accounts WHERE region=?", (region,))
            .fetchone()["s"]
        )
        now = utcnow()
        cur = conn.execute(
            """INSERT INTO dispatch_orders(order_no,region,cap,effective_at,version,state,kind,
                   published_by,published_at,note,created_at)
               VALUES(?,?,?,?,0,'baseline','order','system',?,'发布前口径基线',?)""",
            (self._baseline_no(region), region, licensed, now, now, now),
        )
        return int(cur.lastrowid)

    @staticmethod
    def _baseline_no(region: str) -> str:
        return f"{BASELINE_TAG}-{region.upper()}-0000"

    def backfill_baselines(self) -> None:
        """旧数据缺少编号时，按发布前口径回填基线令与依据编号。幂等。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            regions = [r["region"] for r in conn.execute(
                "SELECT DISTINCT region FROM accounts").fetchall()]
            baseline: dict[str, int] = {}
            for region in regions:
                baseline[region] = self._ensure_region_baseline(conn, region)
            # 历史调度令缺少编号时回填编号（草稿用 D- 前缀，已发布令用 F- 前缀）。
            for row in conn.execute(
                "SELECT * FROM dispatch_orders WHERE order_no IS NULL AND state<>'baseline'"
            ).fetchall():
                prefix = "D" if row["state"] == "draft" and not row["conflict_of"] else "F"
                conn.execute("UPDATE dispatch_orders SET order_no=? WHERE id=?",
                             (f"{prefix}-{int(row['id']):06d}", row["id"]))
            # 历史转让/取水缺少依据编号时，绑定到所在区域发布前基线。
            for table, region_sql, id_col in (
                ("transfers",
                 "SELECT t.id,t.basis_order_id,a.region FROM transfers t JOIN accounts a ON a.id=t.from_account_id",
                 "id"),
                ("usage_records",
                 "SELECT u.id,u.basis_order_id,a.region FROM usage_records u JOIN accounts a ON a.id=u.account_id",
                 "id"),
            ):
                for r in conn.execute(region_sql).fetchall():
                    if r["basis_order_id"] is None:
                        conn.execute(
                            f"UPDATE {table} SET basis_order_id=? WHERE {id_col}=?",
                            (baseline[r["region"]], r["id"]),
                        )
            self._audit(conn, "system", "dispatch.backfilled", "dispatch", None,
                        {"regions": regions}, "startup:backfill")
            conn.commit()

    def recover_pending_orders(self) -> list[int]:
        """写库失败后从调度令检查点恢复；重放不新增冻结记录或审计。"""
        with self.connect() as conn:
            pending = conn.execute(
                "SELECT * FROM dispatch_checkpoints WHERE applied=0 ORDER BY id"
            ).fetchall()
            recovered = []
            for cp in pending:
                self._apply_order(conn, int(cp["order_id"]))
                conn.execute("UPDATE dispatch_checkpoints SET applied=1 WHERE id=?", (cp["id"],))
                recovered.append(int(cp["order_id"]))
            conn.commit()
        return recovered

    def _active_order(self, conn: sqlite3.Connection, region: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM dispatch_orders WHERE region=? AND state='published' "
            "ORDER BY version DESC, id DESC LIMIT 1",
            (region,),
        ).fetchone()

    def _allocation(self, conn: sqlite3.Connection, account: sqlite3.Row,
                    active: sqlite3.Row | None) -> tuple[float, int | None]:
        """Return (effective cap, basis order id) for an account right now."""
        if active is None:
            return float(account["quota"]), None
        row = conn.execute(
            "SELECT dispatched_limit FROM dispatch_allocations WHERE order_id=? AND account_id=?",
            (active["id"], account["id"]),
        ).fetchone()
        if row:
            return float(row["dispatched_limit"]), int(active["id"])
        # Order published before the account existed: scale it in by licensed share.
        return self._scale_limit(conn, account, active), int(active["id"])

    @staticmethod
    def _scale_limit(conn: sqlite3.Connection, account: sqlite3.Row,
                     order: sqlite3.Row) -> float:
        total = float(conn.execute(
            "SELECT COALESCE(SUM(quota),0) s FROM accounts WHERE region=?", (account["region"],)
        ).fetchone()["s"])
        if total <= EPS:
            return 0.0
        return float(order["cap"]) * float(account["quota"]) / total

    def _held_outgoing(self, conn: sqlite3.Connection, account_id: int,
                       active: sqlite3.Row | None) -> tuple[float, float, float]:
        """Return (pending reserved, frozen approved, total held) against the live basis."""
        if active is None:
            pending = float(conn.execute(
                "SELECT COALESCE(SUM(amount),0) t FROM transfers WHERE from_account_id=? AND status='pending'",
                (account_id,)).fetchone()["t"])
            # 批准后曾被调度令冻结、随后又随令释放的转让，不再占用当前口径。
            frozen = float(conn.execute(
                """SELECT COALESCE(SUM(t.amount),0) t FROM transfers t
                       WHERE t.from_account_id=? AND t.status='approved'
                         AND NOT EXISTS (SELECT 1 FROM dispatch_freezes f
                                          WHERE f.transfer_id=t.id AND f.state='released')""",
                (account_id,)).fetchone()["t"])
            return pending, frozen, pending + frozen
        pending = float(conn.execute(
            "SELECT COALESCE(SUM(amount),0) t FROM dispatch_reservations "
            "WHERE order_id=? AND account_id=? AND state='active'",
            (active["id"], account_id)).fetchone()["t"])
        frozen = float(conn.execute(
            "SELECT COALESCE(SUM(amount),0) t FROM dispatch_freezes "
            "WHERE order_id=? AND account_id=? AND state='frozen'",
            (active["id"], account_id)).fetchone()["t"])
        return pending, frozen, pending + frozen

    def _available_row(self, conn: sqlite3.Connection, account: sqlite3.Row,
                       active: sqlite3.Row | None) -> dict[str, Any]:
        cap, basis_id = self._allocation(conn, account, active)
        pending, frozen, held = self._held_outgoing(conn, int(account["id"]), active)
        used = float(account["used"])
        return {
            "account_id": int(account["id"]),
            "available": max(0.0, cap - used - held),
            "dispatched_limit": cap,
            "licensed_quota": float(account["quota"]),
            "used": used,
            "reserved_outgoing": pending,
            "frozen_outgoing": frozen,
            "held_outgoing": held,
            "basis_order_id": basis_id,
        }

    def create_draft(self, actor: str, payload: dict[str, Any], role: str = "dispatcher",
                     kind: str = "order") -> dict[str, Any]:
        if role not in {"dispatcher", "supervisor", "editor"}:
            raise DomainError("只有调度员可以起草调度令", 403)
        if kind == "order":
            region = str(payload.get("region", "")).strip()
            if not region:
                raise DomainError("调度区域不能为空")
            try:
                cap = float(payload.get("cap"))
            except (TypeError, ValueError) as exc:
                raise DomainError("区域限额必须是数值") from exc
            if cap < 0:
                raise DomainError("区域限额不能为负")
        else:
            region = str(payload.get("region", "")).strip()
            cap = 0.0
        effective_at = parse_moment(payload.get("effective_at"))
        with self.connect() as conn:
            now = utcnow()
            cur = conn.execute(
                """INSERT INTO dispatch_orders(region,cap,effective_at,version,state,kind,note,created_at)
                   VALUES(?,?,?,0,'draft',?,?,?,?)""",
                (region, cap, effective_at, kind, str(payload.get("note", "")), now),
            )
            order_id = int(cur.lastrowid)
            conn.execute("UPDATE dispatch_orders SET order_no=? WHERE id=?",
                         (f"D-{order_id:06d}", order_id))
            self._audit(conn, actor, "dispatch.draft_created", "dispatch_order", order_id,
                        {"region": region, "cap": cap, "kind": kind})
            row = conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone()
        return dict(row)

    def publish_order(self, actor: str, payload: dict[str, Any],
                      role: str = "dispatcher") -> dict[str, Any]:
        if role not in {"dispatcher", "supervisor", "editor"}:
            raise DomainError("只有调度员可以发布调度令", 403)
        region = str(payload.get("region", "")).strip()
        if not region:
            raise DomainError("调度区域不能为空")
        try:
            cap = float(payload.get("cap"))
        except (TypeError, ValueError) as exc:
            raise DomainError("区域限额必须是数值") from exc
        if cap < 0:
            raise DomainError("区域限额不能为负")
        base_version = payload.get("base_version")
        if base_version is not None:
            try:
                base_version = int(base_version)
            except (TypeError, ValueError) as exc:
                raise DomainError("依据版本必须是整数") from exc
        draft_id = payload.get("draft_id")
        effective_at = parse_moment(payload.get("effective_at"))
        note = str(payload.get("note", ""))

        self._fault_point("before_order")

        # 首份写入生效：版本判断与令的写入放在同一 IMMEDIATE 事务里。
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._active_order(conn, region)
            current_version = int(current["version"]) if current else 0
            conflict: sqlite3.Row | None = None
            if base_version is not None and base_version != current_version:
                conflict = current
            order_id: int | None = None
            if draft_id is not None:
                draft = conn.execute(
                    "SELECT * FROM dispatch_orders WHERE id=? AND state='draft' AND kind='order'",
                    (int(draft_id),),
                ).fetchone()
                if not draft:
                    raise DomainError("草稿不存在或已发布", 404)
                if draft["region"] != region:
                    raise DomainError("草稿区域与发布区域不一致", 409)
                order_id = int(draft["id"])
            if conflict is not None:
                # 另一位保留草稿并看到冲突。
                if order_id is None:
                    cur = conn.execute(
                        """INSERT INTO dispatch_orders(order_no,region,cap,effective_at,version,state,kind,
                               base_version,conflict_of,conflict_reason,note,created_at)
                           VALUES(?,?,?,?,0,'draft','order',?,?,?,?,?)""",
                        (None, region, cap, effective_at, base_version, int(conflict["id"]),
                         f"区域当前版本为 {current_version}，依据版本 {base_version} 已过期",
                         note, utcnow()),
                    )
                    order_id = int(cur.lastrowid)
                    conn.execute("UPDATE dispatch_orders SET order_no=? WHERE id=?",
                                 (f"D-{order_id:06d}", order_id))
                else:
                    conn.execute(
                        "UPDATE dispatch_orders SET base_version=?,conflict_of=?,conflict_reason=? WHERE id=?",
                        (base_version, int(conflict["id"]),
                         f"区域当前版本为 {current_version}，依据版本 {base_version} 已过期",
                         order_id),
                    )
                self._audit(conn, actor, "publish.conflicted", "dispatch_order", order_id,
                            {"region": region, "base_version": base_version,
                             "current_version": current_version})
                conflicted = dict(conn.execute(
                    "SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone())
                conn.commit()
                raise DomainError("调度令已被对方更新，草稿已保留", 409,
                                  {"draft": conflicted, "current_version": current_version})
            if order_id is None:
                cur = conn.execute(
                    """INSERT INTO dispatch_orders(order_no,region,cap,effective_at,version,state,kind,
                           base_version,published_by,published_at,note,created_at)
                       VALUES(?,?,?,?,?,'published','order',?,?,?,?,?)""",
                    (None, region, cap, effective_at, current_version + 1,
                     current_version, actor, utcnow(), note, utcnow()),
                )
                order_id = int(cur.lastrowid)
            else:
                conn.execute(
                    """UPDATE dispatch_orders SET cap=?,effective_at=?,version=?,state='published',
                           base_version=?,published_by=?,published_at=?,note=?,conflict_of=NULL,conflict_reason=NULL
                       WHERE id=?""",
                    (cap, effective_at, current_version + 1, current_version,
                     actor, utcnow(), note, order_id),
                )
            conn.execute("UPDATE dispatch_orders SET order_no=? WHERE id=? AND order_no IS NULL",
                         (f"F-{order_id:06d}", order_id))
            now = utcnow()
            conn.execute(
                "INSERT INTO dispatch_checkpoints(order_id,stage,applied,created_at) VALUES(?,?,0,?)",
                (order_id, "published", now),
            )
            self._audit(conn, actor, "dispatch.published", "dispatch_order", order_id,
                        {"region": region, "cap": cap, "version": current_version + 1,
                         "effective_at": effective_at},
                        f"order:{order_id}:publish")
            conn.commit()

        self._fault_point("after_checkpoint")

        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._apply_order(conn, order_id)
            cp = conn.execute(
                "SELECT id FROM dispatch_checkpoints WHERE order_id=? AND applied=0", (order_id,)
            ).fetchone()
            if cp:
                conn.execute("UPDATE dispatch_checkpoints SET applied=1 WHERE id=?", (cp["id"],))
            conn.commit()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone()
        return dict(row)

    def _apply_order(self, conn: sqlite3.Connection, order_id: int) -> None:
        """Replayable ledger effects of a published order (idempotent)."""
        order = conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["state"] != "published":
            return
        now = utcnow()
        region = order["region"]

        # 1) 旧版本让位（其占用与冻结随之失效）。
        conn.execute(
            "UPDATE dispatch_orders SET state='superseded' WHERE region=? AND state='published' AND id<>?",
            (region, order_id),
        )

        # 2) 受影响账户可用量立即重算：按许可额度比例把区域限额分到账户。
        accounts = conn.execute(
            "SELECT * FROM accounts WHERE region=? ORDER BY id", (region,)).fetchall()
        total_licensed = sum(float(a["quota"]) for a in accounts)
        for acc in accounts:
            licensed = float(acc["quota"])
            limit = float(order["cap"]) * licensed / total_licensed if total_licensed > EPS else 0.0
            conn.execute(
                """INSERT INTO dispatch_allocations(order_id,account_id,licensed_quota,dispatched_limit,created_at)
                   VALUES(?,?,?,?,?) ON CONFLICT(order_id,account_id) DO UPDATE SET
                     licensed_quota=excluded.licensed_quota,dispatched_limit=excluded.dispatched_limit""",
                (order_id, acc["id"], licensed, limit, now),
            )

        # 3) 待审转让按发布版本重新占用（含跨区域转出方）。
        for t in conn.execute(
            """SELECT t.*, a.region AS src_region FROM transfers t
                   JOIN accounts a ON a.id=t.from_account_id
                   WHERE t.status='pending' AND a.region=?""",
            (region,),
        ).fetchall():
            exists = conn.execute(
                "SELECT 1 FROM dispatch_reservations WHERE order_id=? AND transfer_id=?",
                (order_id, t["id"]),
            ).fetchone()
            if not exists:
                conn.execute(
                    """INSERT INTO dispatch_reservations(order_id,transfer_id,account_id,amount,state,note,created_at)
                       VALUES(?,?,?,?,'active','按发布版本重新占用',?)""",
                    (order_id, t["id"], t["from_account_id"], float(t["amount"]), now),
                )

        # 4) 已批准未执行的先冻结。UNIQUE(order,transfer) 保证重放不新增冻结。
        for t in conn.execute(
            """SELECT t.* FROM transfers t JOIN accounts a ON a.id=t.from_account_id
                   WHERE t.status='approved' AND t.executed_at IS NULL AND a.region=?""",
            (region,),
        ).fetchall():
            conn.execute(
                """INSERT INTO dispatch_freezes(order_id,transfer_id,account_id,amount,state,created_at)
                   VALUES(?,?,?,?,'frozen',?) ON CONFLICT(order_id,transfer_id) DO NOTHING""",
                (order_id, t["id"], t["from_account_id"], float(t["amount"]), now),
            )

        # 5) 已执行或已取水保留当时依据，把超出新限额的差额转待复核。
        # 用“该令是否已生成复核”作跳过条件（而非 allocation），避免崩溃在本步中途时
        # 重放整步跳过；每条复核自带 (order,table,source) 唯一约束，逐条幂等。
        reviews_done = conn.execute(
            "SELECT 1 FROM dispatch_reviews WHERE order_id=? LIMIT 1", (order_id,)).fetchone()
        if not reviews_done:
            fresh = conn.execute(
                "SELECT * FROM accounts WHERE region=? ORDER BY id", (region,)).fetchall()
            # 执行/取水发生在发布之前的记录，依据统一锚定发布前基线。
            baseline = conn.execute(
                "SELECT id FROM dispatch_orders WHERE region=? AND state='baseline'", (region,)
            ).fetchone()
            if baseline:
                conn.execute(
                    """UPDATE transfers SET basis_order_id=? WHERE basis_order_id IS NULL
                           AND from_account_id IN (SELECT id FROM accounts WHERE region=?)""",
                    (baseline["id"], region))
                conn.execute(
                    """UPDATE usage_records SET basis_order_id=? WHERE basis_order_id IS NULL
                           AND account_id IN (SELECT id FROM accounts WHERE region=?)""",
                    (baseline["id"], region))
            self._create_reviews(conn, order, fresh)

        self._audit(conn, order["published_by"] or "system", "dispatch.applied",
                    "dispatch_order", order_id,
                    {"region": region, "cap": order["cap"], "version": order["version"]},
                    f"order:{order_id}:applied")

    def _create_reviews(self, conn: sqlite3.Connection, order: sqlite3.Row,
                        accounts: list[sqlite3.Row]) -> None:
        now = utcnow()
        for acc in accounts:
            alloc = conn.execute(
                "SELECT dispatched_limit FROM dispatch_allocations WHERE order_id=? AND account_id=?",
                (order["id"], acc["id"]),
            ).fetchone()
            new_limit = float(alloc["dispatched_limit"])
            # 事件按当时依据保留：发布前已发生的取水/已执行转出均不改依据，按时间累计。
            events: list[tuple[str, int, str, float, str, int | None]] = []
            for u in conn.execute(
                "SELECT * FROM usage_records WHERE account_id=? ORDER BY occurred_at,id",
                (acc["id"],),
            ).fetchall():
                events.append(("usage", int(u["id"]), u["occurred_at"], float(u["amount"]),
                               "已取水超出发布后限额", u["basis_order_id"]))
            for t in conn.execute(
                """SELECT * FROM transfers WHERE from_account_id=? AND status='executed'
                       AND executed_at IS NOT NULL ORDER BY executed_at,id""",
                (acc["id"],),
            ).fetchall():
                events.append(("transfer", int(t["id"]), t["executed_at"], float(t["amount"]),
                               "已执行转让依据旧限额，差额待复核", t["basis_order_id"]))
            events.sort(key=lambda e: (e[2], e[1]))
            running = 0.0
            for table, source_id, _when, amount, reason, basis_id in events:
                running += amount
                excess = running - new_limit
                if excess > EPS:
                    conn.execute(
                        """INSERT INTO dispatch_reviews(order_id,account_id,source_table,source_id,
                               basis_order_id,amount,reason,status,created_at)
                           VALUES(?,?,?,?,?,?,?,'pending',?)
                           ON CONFLICT(order_id,source_table,source_id) DO NOTHING""",
                        (order["id"], acc["id"], table, source_id, basis_id, excess, reason, now),
                    )

    def revoke_order(self, order_id: int, actor: str, role: str = "supervisor",
                     base_version: int | None = None) -> dict[str, Any]:
        # 调度主管能撤销，普通账户越权撤销会被拒绝。
        if role not in {"supervisor"}:
            raise DomainError("只有调度主管可以撤销调度令", 403)
        self._fault_point("before_order")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute(
                "SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                raise DomainError("调度令不存在", 404)
            if order["state"] in {"superseded", "revoked"}:
                raise DomainError("调度令已失效，不能重复撤销", 409)
            if order["state"] == "draft":
                raise DomainError("草稿不能撤销，请直接发布或删除", 409)
            current = self._active_order(conn, order["region"])
            conflict = current is not None and int(current["id"]) != order_id
            if base_version is not None and current is not None and int(current["version"]) != int(base_version):
                conflict = True
            if conflict and current is not None:
                # 并发撤销：另一位保留撤销草稿并看到冲突。
                cur = conn.execute(
                    """INSERT INTO dispatch_orders(order_no,region,cap,effective_at,version,state,kind,
                           base_version,conflict_of,conflict_reason,published_by,published_at,note,created_at)
                       VALUES(?,?,?,?,0,'draft','revoke',?,?,?,?,?,?,?)""",
                    (None, order["region"], float(order["cap"]), utcnow(),
                     current["version"], int(current["id"]),
                     f"区域当前版本为 {current['version']}，撤销依据已过期",
                     actor, utcnow(), f"请求撤销令 {order_id}", utcnow()),
                )
                draft_id = int(cur.lastrowid)
                conn.execute("UPDATE dispatch_orders SET order_no=? WHERE id=?",
                             (f"D-{draft_id:06d}", draft_id))
                self._audit(conn, actor, "revoke.conflicted", "dispatch_order", draft_id,
                            {"region": order["region"], "target_order": order_id})
                draft = dict(conn.execute(
                    "SELECT * FROM dispatch_orders WHERE id=?", (draft_id,)).fetchone())
                conn.commit()
                raise DomainError("调度令已被对方更新，撤销草稿已保留", 409,
                                  {"draft": draft, "current_version": int(current["version"])})
            now = utcnow()
            conn.execute(
                "UPDATE dispatch_orders SET state='revoked',revoked_by=?,revoked_at=? WHERE id=?",
                (actor, now, order_id))
            # 该版本占用与冻结释放，账户回到许可额度口径（实时计算）。
            conn.execute(
                "UPDATE dispatch_reservations SET state='released' WHERE order_id=? AND state='active'",
                (order_id,))
            conn.execute(
                "UPDATE dispatch_freezes SET state='released',released_at=? WHERE order_id=? AND state='frozen'",
                (now, order_id))
            self._audit(conn, actor, "dispatch.revoked", "dispatch_order", order_id,
                        {"region": order["region"], "version": order["version"]},
                        f"order:{order_id}:revoke")
            conn.commit()
        with self.connect() as conn:
            return dict(conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone())

    def ledger(self, region: str | None = None) -> dict[str, Any]:
        """调度账：调度令、账户重算、占用、冻结、待复核汇成一份账。"""
        with self.connect() as conn:
            order_sql = "SELECT * FROM dispatch_orders"
            args: tuple[Any, ...] = ()
            if region:
                order_sql += " WHERE region=?"
                args = (region,)
            orders = [dict(r) for r in conn.execute(
                order_sql + " ORDER BY region,version DESC,id DESC", args).fetchall()]
            accounts = []
            acc_rows = conn.execute(
                "SELECT * FROM accounts" + (" WHERE region=?" if region else "") + " ORDER BY id",
                args).fetchall()
            for acc in acc_rows:
                active = self._active_order(conn, acc["region"])
                accounts.append(self._available_row(conn, acc, active))
            pending_reviews = [dict(r) for r in conn.execute(
                """SELECT r.* FROM dispatch_reviews r JOIN dispatch_orders o ON o.id=r.order_id
                   WHERE r.status='pending'"""
                + (" AND o.region=?" if region else "")
                + " ORDER BY r.id DESC", args).fetchall()]
        return {"orders": orders, "accounts": accounts, "reviews_pending": pending_reviews}

    def list_orders(self, include_drafts: bool = True) -> list[dict[str, Any]]:
        with self.connect() as conn:
            sql = "SELECT * FROM dispatch_orders"
            if not include_drafts:
                sql += " WHERE state<>'draft'"
            rows = conn.execute(sql + " ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ accounts
    def create_account(self, actor: str, payload: dict[str, Any], role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以创建账户", 403)
        name = str(payload.get("name", "")).strip()
        region = str(payload.get("region", "")).strip()
        holder = str(payload.get("holder", "")).strip()
        if not name or not region or not holder:
            raise DomainError("账户名称、地区和持有人不能为空")
        try:
            priority = int(payload.get("priority"))
            quota = float(payload.get("quota"))
        except (TypeError, ValueError) as exc:
            raise DomainError("优先级和额度必须是数值") from exc
        if not 1 <= priority <= 5 or quota < 0:
            raise DomainError("优先级应在 1 到 5 之间，额度不能为负")
        valid_from = parse_date(str(payload.get("valid_from", "")), "生效日期")
        valid_to = parse_date(str(payload.get("valid_to", "")), "失效日期")
        if valid_from > valid_to:
            raise DomainError("生效日期不能晚于失效日期")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_region_baseline(conn, region)
            try:
                cur = conn.execute(
                    "INSERT INTO accounts(name,region,holder,priority,valid_from,valid_to,quota,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (name, region, holder, priority, valid_from.isoformat(), valid_to.isoformat(), quota, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("账户名称已存在", 409) from exc
            self._audit(conn, actor, "account.created", "account", cur.lastrowid, {"name": name, "quota": quota})
            row = conn.execute("SELECT * FROM accounts WHERE id=?", (cur.lastrowid,)).fetchone()
            result = dict(row)
        return result

    def set_season_rule(self, actor: str, region: str, month: int, max_fraction: float,
                        note: str = "", role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以设置季节规则", 403)
        if not 1 <= int(month) <= 12 or not 0 < float(max_fraction) <= 1:
            raise DomainError("月份或季节比例不合法")
        with self.connect() as conn:
            self._ensure_region_baseline(conn, region.strip())
            conn.execute(
                """INSERT INTO season_rules(region,month,max_fraction,note) VALUES(?,?,?,?)
                   ON CONFLICT(region,month) DO UPDATE SET max_fraction=excluded.max_fraction,note=excluded.note""",
                (region.strip(), int(month), float(max_fraction), note),
            )
            self._audit(conn, actor, "season_rule.saved", "region", None, {"region": region, "month": month, "max_fraction": max_fraction})
        return {"region": region, "month": month, "max_fraction": max_fraction, "note": note}

    def set_impact_rule(self, actor: str, source_region: str, target_region: str, min_source_fraction: float,
                        note: str = "", role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以设置第三方影响规则", 403)
        if not 0 <= float(min_source_fraction) <= 1:
            raise DomainError("最小留存比例必须在 0 到 1 之间")
        with self.connect() as conn:
            self._ensure_region_baseline(conn, source_region)
            self._ensure_region_baseline(conn, target_region)
            conn.execute(
                """INSERT INTO impact_rules(source_region,target_region,min_source_fraction,note) VALUES(?,?,?,?)
                   ON CONFLICT(source_region,target_region) DO UPDATE SET min_source_fraction=excluded.min_source_fraction,note=excluded.note""",
                (source_region, target_region, float(min_source_fraction), note),
            )
            self._audit(conn, actor, "impact_rule.saved", "region", None, {"source": source_region, "target": target_region, "min_fraction": min_source_fraction})
        return {"source_region": source_region, "target_region": target_region, "min_source_fraction": min_source_fraction, "note": note}

    def _account_row(self, conn: sqlite3.Connection, account_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            raise DomainError("水权账户不存在", 404)
        return row

    def available(self, account_id: int, as_of: str | None = None) -> dict[str, Any]:
        if as_of:
            parse_date(as_of, "查询日期")
        with self.connect() as conn:
            account = self._account_row(conn, account_id)
            active = self._active_order(conn, account["region"])
            return self._available_row(conn, account, active)

    # ---------------------------------------------------------------- transfers
    def create_transfer(self, actor: str, payload: dict[str, Any], role: str = "editor") -> dict[str, Any]:
        if role not in {"editor", "dispatcher"}:
            raise DomainError("只有水权编辑人员可以发起转让", 403)
        try:
            source_id = int(payload.get("from_account_id"))
            target_id = int(payload.get("to_account_id"))
            amount = float(payload.get("amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("账户和转让量必须是数值") from exc
        if source_id == target_id or amount <= 0:
            raise DomainError("转让账户不能相同，转让量必须大于 0")
        effective = parse_date(str(payload.get("effective_date", "")), "生效日期")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source = self._account_row(conn, source_id)
            target = self._account_row(conn, target_id)
            self._ensure_region_baseline(conn, source["region"])
            self._ensure_region_baseline(conn, target["region"])
            if not (source["valid_from"] <= effective.isoformat() <= source["valid_to"]):
                raise DomainError("转出账户在生效日无效", 409)
            if not (target["valid_from"] <= effective.isoformat() <= target["valid_to"]):
                raise DomainError("转入账户在生效日无效", 409)
            active = self._active_order(conn, source["region"])
            info = self._available_row(conn, source, active)
            held = info["held_outgoing"]
            cap = info["dispatched_limit"]
            if amount > cap - float(source["used"]) - held + 1e-9:
                raise DomainError("可用额度不足，待审批转让会按当前调度版本预占额度", 409)
            # More critical users (smaller priority number) cannot transfer their
            # protected allocation to a less critical user.
            if int(source["priority"]) > int(target["priority"]):
                raise DomainError("不能把较低优先级水量转给更高优先级账户", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if cap - float(source["used"]) - held - amount + 1e-9 < minimum:
                    raise DomainError("转让会违反下游第三方最小留存约束", 409)
            _, basis_id = self._allocation(conn, source, active)
            cur = conn.execute(
                """INSERT INTO transfers(from_account_id,to_account_id,amount,effective_date,
                       created_by,created_at,basis_order_id) VALUES(?,?,?,?,?,?,?)""",
                (source_id, target_id, amount, effective.isoformat(), actor, utcnow(), basis_id),
            )
            transfer_id = int(cur.lastrowid)
            if active is not None:
                conn.execute(
                    """INSERT INTO dispatch_reservations(order_id,transfer_id,account_id,amount,state,note,created_at)
                       VALUES(?,?,?,?,'active','待审转让按当前发布版本占用',?)""",
                    (active["id"], transfer_id, source_id, amount, utcnow()),
                )
            self._audit(conn, actor, "transfer.created", "transfer", transfer_id,
                        {"source": source_id, "target": target_id, "amount": amount,
                         "effective_date": effective.isoformat(), "basis_order_id": basis_id})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            result = dict(row)
        return result

    def approve_transfer(self, transfer_id: int, actor: str, role: str = "reviewer") -> dict[str, Any]:
        # 批准不再划转额度；执行时才划转。批准即占用，生效调度令下直接冻结。
        if role != "reviewer":
            raise DomainError("只有审核人可以批准转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not transfer:
                raise DomainError("转让记录不存在", 404)
            if transfer["status"] != "pending":
                raise DomainError("该转让已处理，不能重复批准", 409)
            if actor == transfer["created_by"]:
                raise DomainError("发起人不能批准自己的转让", 403)
            source = self._account_row(conn, transfer["from_account_id"])
            target = self._account_row(conn, transfer["to_account_id"])
            amount = float(transfer["amount"])
            active = self._active_order(conn, source["region"])
            info = self._available_row(conn, source, active)
            other_held = info["held_outgoing"] - info["reserved_outgoing"]
            # 本笔的待审占用已在 held 的 reserved 部分里，校验时把它视作继续占用。
            if amount > info["dispatched_limit"] - float(source["used"]) - other_held + 1e-9:
                raise DomainError("审批时额度已被其他记录占用，不能批准", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if info["dispatched_limit"] - float(source["used"]) - other_held - amount + 1e-9 < minimum:
                    raise DomainError("审批时下游最小留存约束不再满足", 409)
            conn.execute(
                "UPDATE transfers SET status='approved',approved_by=?,approved_at=? WHERE id=?",
                (actor, utcnow(), transfer_id))
            if active is not None:
                # 已批准未执行：先冻结；待审占用同步让位。
                conn.execute(
                    "UPDATE dispatch_reservations SET state='superseded' WHERE transfer_id=? AND state='active'",
                    (transfer_id,))
                conn.execute(
                    """INSERT INTO dispatch_freezes(order_id,transfer_id,account_id,amount,state,created_at)
                       VALUES(?,?,?,?,'frozen',?)
                       ON CONFLICT(order_id,transfer_id) DO NOTHING""",
                    (active["id"], transfer_id, source["id"], amount, utcnow()),
                )
            self._audit(conn, actor, "transfer.approved", "transfer", transfer_id,
                        {"amount": amount, "frozen": active is not None})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            result = dict(row)
        return result

    def execute_transfer(self, transfer_id: int, actor: str, role: str = "editor") -> dict[str, Any]:
        if role not in {"editor", "dispatcher", "meter"}:
            raise DomainError("无权执行转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not transfer:
                raise DomainError("转让记录不存在", 404)
            if transfer["status"] != "approved":
                raise DomainError("只有已批准转让可以执行", 409)
            active = self._active_order(
                conn, conn.execute("SELECT region FROM accounts WHERE id=?",
                                   (transfer["from_account_id"],)).fetchone()["region"])
            if active is not None:
                freeze = conn.execute(
                    "SELECT 1 FROM dispatch_freezes WHERE transfer_id=? AND order_id=? AND state='frozen'",
                    (transfer_id, active["id"])).fetchone()
                if freeze:
                    raise DomainError("转让已被调度令冻结，不能执行", 409)
            amount = float(transfer["amount"])
            conn.execute("UPDATE accounts SET quota=quota-? WHERE id=?", (amount, transfer["from_account_id"]))
            conn.execute("UPDATE accounts SET quota=quota+? WHERE id=?", (amount, transfer["to_account_id"]))
            now = utcnow()
            conn.execute(
                "UPDATE transfers SET status='executed',executed_at=?,basis_order_id=COALESCE(basis_order_id,?) WHERE id=?",
                (now, active["id"] if active else transfer["basis_order_id"], transfer_id))
            self._audit(conn, actor, "transfer.executed", "transfer", transfer_id,
                        {"amount": amount, "basis_order_id": transfer["basis_order_id"]})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            result = dict(row)
        return result

    def reject_transfer(self, transfer_id: int, actor: str, role: str = "reviewer") -> dict[str, Any]:
        if role != "reviewer":
            raise DomainError("只有审核人可以退回转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not row or row["status"] != "pending":
                raise DomainError("转让不存在或已经处理", 409)
            if actor == row["created_by"]:
                raise DomainError("发起人不能自行退回", 403)
            conn.execute(
                "UPDATE dispatch_reservations SET state='released' WHERE transfer_id=? AND state='active'",
                (transfer_id,))
            conn.execute("UPDATE transfers SET status='rejected',approved_by=?,approved_at=? WHERE id=?", (actor, utcnow(), transfer_id))
            self._audit(conn, actor, "transfer.rejected", "transfer", transfer_id, {})
        return {"id": transfer_id, "status": "rejected"}

    # -------------------------------------------------------------------- usage
    def record_usage(self, actor: str, payload: dict[str, Any], role: str = "meter") -> dict[str, Any]:
        if role not in {"meter", "editor"}:
            raise DomainError("只有计量员可以登记取水", 403)
        try:
            account_id = int(payload.get("account_id"))
            amount = float(payload.get("amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("账户和取水量必须是数值") from exc
        meter_event_id = str(payload.get("meter_event_id", "")).strip()
        occurred = parse_date(str(payload.get("occurred_at", "")), "计量日期")
        if amount <= 0 or not meter_event_id:
            raise DomainError("取水量必须大于 0，计量事件编号不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            account = self._account_row(conn, account_id)
            if not (account["valid_from"] <= occurred.isoformat() <= account["valid_to"]):
                raise DomainError("取水日期不在许可有效期内", 409)
            active = self._active_order(conn, account["region"])
            info = self._available_row(conn, account, active)
            if amount > info["available"] + 1e-9:
                raise DomainError("取水超过当前可用额度（含调度限额与占用）", 409)
            # 季节上限以当前生效口径（调度后限额）为准。
            cap = info["dispatched_limit"]
            season = conn.execute("SELECT max_fraction FROM season_rules WHERE region=? AND month=?", (account["region"], occurred.month)).fetchone()
            month_total = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM usage_records WHERE account_id=? AND substr(occurred_at,1,7)=?",
                (account_id, occurred.strftime("%Y-%m")),
            ).fetchone()["total"]
            if season:
                month_cap = cap * float(season["max_fraction"])
                if float(month_total) + amount > month_cap + 1e-9:
                    raise DomainError("本次取水超过该月份的季节配额", 409)
            _, basis_id = self._allocation(conn, account, active)
            try:
                cur = conn.execute(
                    """INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at,basis_order_id)
                       VALUES(?,?,?,?,?,?,?)""",
                    (account_id, meter_event_id, amount, occurred.isoformat(), actor, utcnow(), basis_id),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("计量事件已登记，不能重复计水", 409) from exc
            conn.execute("UPDATE accounts SET used=used+? WHERE id=?", (amount, account_id))
            self._audit(conn, actor, "usage.recorded", "account", account_id,
                        {"amount": amount, "occurred_at": occurred.isoformat(),
                         "meter_event_id": meter_event_id, "basis_order_id": basis_id})
            row = conn.execute("SELECT * FROM usage_records WHERE id=?", (cur.lastrowid,)).fetchone()
            result = dict(row)
        return result

    # ------------------------------------------------------------------ reviews
    def list_reviews(self, status: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM dispatch_reviews WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM dispatch_reviews ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    def resolve_review(self, review_id: int, actor: str, role: str = "supervisor",
                       resolution: str = "confirmed") -> dict[str, Any]:
        if role not in {"supervisor", "reviewer"}:
            raise DomainError("只有调度主管可以复核差额", 403)
        if resolution not in {"confirmed", "dismissed"}:
            raise DomainError("复核结论必须是 confirmed 或 dismissed")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM dispatch_reviews WHERE id=?", (review_id,)).fetchone()
            if not row:
                raise DomainError("待复核记录不存在", 404)
            if row["status"] != "pending":
                raise DomainError("该差额已经复核", 409)
            now = utcnow()
            conn.execute(
                "UPDATE dispatch_reviews SET status=?,resolved_by=?,resolved_at=? WHERE id=?",
                (resolution, actor, now, review_id))
            self._audit(conn, actor, "review.resolved", "dispatch_review", review_id,
                        {"resolution": resolution, "amount": row["amount"]})
            out = dict(conn.execute("SELECT * FROM dispatch_reviews WHERE id=?", (review_id,)).fetchone())
        return out

    # -------------------------------------------------------------- simulation
    def simulate_drought(self, total_supply: float, reduction: float = 0.0, role: str = "viewer") -> dict[str, Any]:
        try:
            total_supply, reduction = float(total_supply), float(reduction)
        except (TypeError, ValueError) as exc:
            raise DomainError("供水量和削减比例必须是数值") from exc
        if total_supply < 0 or not 0 <= reduction < 1:
            raise DomainError("供水量不能为负，削减比例应在 0 到 1 之间")
        with self.connect() as conn:
            raw = conn.execute("SELECT * FROM accounts ORDER BY priority,name").fetchall()
            actives = {r["region"]: self._active_order(conn, r["region"])
                       for r in raw}
            rows = []
            for r in raw:
                item = dict(r)
                cap, _ = self._allocation(conn, r, actives[r["region"]])
                item["effective_quota"] = min(float(r["quota"]), cap)
                rows.append(item)
        supply = total_supply * (1 - reduction)
        allocation: dict[int, float] = {}
        deficit: dict[int, float] = {}
        remaining = supply
        for priority in range(1, 6):
            group = [r for r in rows if int(r["priority"]) == priority]
            if not group:
                continue
            # During shortage, more critical rights receive their remaining
            # allocation first; only then does water flow to lower priorities.
            requested = sum(max(0.0, float(r["effective_quota"]) - float(r["used"])) for r in group)
            take = min(remaining, requested)
            if requested <= 0:
                continue
            for row in group:
                quota_left = max(0.0, float(row["effective_quota"]) - float(row["used"]))
                share = take * quota_left / requested
                allocation[int(row["id"])] = share
                deficit[int(row["id"])] = quota_left - share
            remaining -= take
            if remaining <= 1e-9:
                for lower in rows:
                    if int(lower["priority"]) > priority:
                        left = max(0.0, float(lower["effective_quota"]) - float(lower["used"]))
                        allocation[int(lower["id"])] = 0.0
                        deficit[int(lower["id"])] = left
                break
        return {"total_supply": total_supply, "reduction": reduction, "effective_supply": supply,
                "unallocated": remaining, "allocations": [
                    {"account_id": int(r["id"]), "name": r["name"], "priority": r["priority"],
                     "allocation": allocation.get(int(r["id"]), 0.0), "deficit": deficit.get(int(r["id"]), 0.0)}
                    for r in rows
                ]}

    # -------------------------------------------------------------------- lists
    def list_accounts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
            result = []
            for row in rows:
                active = self._active_order(conn, row["region"])
                item = dict(row)
                item.update(self._available_row(conn, row, active))
                result.append(item)
        return result

    def list_transfers(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM transfers ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    def audit(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]


def seed_demo(db: Database) -> dict[str, int]:
    if db.list_accounts():
        return {str(a["name"]): int(a["id"]) for a in db.list_accounts()}
    upstream = db.create_account("alice", {"name": "北区水库", "region": "upstream", "holder": "北区水务公司", "priority": 1, "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": 1000}, "editor")
    downstream = db.create_account("alice", {"name": "河口灌区", "region": "downstream", "holder": "河口合作社", "priority": 2, "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": 500}, "editor")
    db.set_season_rule("alice", "upstream", 7, 0.35, "夏季上限", "editor")
    db.set_impact_rule("alice", "upstream", "downstream", 0.4, "保障河口最小生态流量", "editor")
    db.record_usage("meter-01", {"account_id": upstream["id"], "amount": 100, "meter_event_id": "UP-2026-0001", "occurred_at": "2026-03-01"}, "meter")
    return {"北区水库": int(upstream["id"]), "河口灌区": int(downstream["id"])}


class Handler(BaseHTTPRequestHandler):
    db: Database
    server_version = "WaterRights/1.0"

    def _send(self, payload: Any, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self) -> None:
        data = (ROOT / "static" / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise DomainError("请求体不是合法 JSON") from exc

    def _auth(self) -> tuple[str, str]:
        return self.headers.get("X-User", "anonymous"), self.headers.get("X-Role", "viewer")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            q = parse_qs(parsed.query)
            if parsed.path in {"/", "/index.html"}:
                return self._html()
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            if parsed.path == "/api/accounts":
                return self._send({"accounts": self.db.list_accounts()})
            if parsed.path == "/api/transfers":
                return self._send({"transfers": self.db.list_transfers()})
            if parsed.path == "/api/audit":
                return self._send({"audit": self.db.audit()})
            if parsed.path == "/api/dispatch/orders":
                return self._send({"orders": self.db.list_orders()})
            if parsed.path == "/api/dispatch/ledger":
                return self._send(self.db.ledger(q.get("region", [""])[0] or None))
            if parsed.path == "/api/dispatch/reviews":
                return self._send({"reviews": self.db.list_reviews(q.get("status", [""])[0] or None)})
            if parsed.path.startswith("/api/accounts/") and parsed.path.endswith("/available"):
                account_id = int(parsed.path.split("/")[3])
                return self._send(self.db.available(account_id))
            if parsed.path == "/api/drought/simulate":
                return self._send(self.db.simulate_drought(float(q.get("supply", ["0"])[0]), float(q.get("reduction", ["0"])[0])))
            raise DomainError("接口不存在", 404)
        except (ValueError, DomainError) as exc:
            self._send({"error": str(exc), **getattr(exc, "extra", {})}, getattr(exc, "status", 400))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            actor, role = self._auth()
            body = self._body()
            parts = [p for p in parsed.path.split("/") if p]
            if parts == ["api", "accounts"]:
                return self._send(self.db.create_account(actor, body, role), 201)
            if parts == ["api", "rules", "season"]:
                return self._send(self.db.set_season_rule(actor, str(body.get("region", "")), int(body.get("month", 0)), body.get("max_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "rules", "impact"]:
                return self._send(self.db.set_impact_rule(actor, str(body.get("source_region", "")), str(body.get("target_region", "")), body.get("min_source_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "transfers"]:
                return self._send(self.db.create_transfer(actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "approve":
                return self._send(self.db.approve_transfer(int(parts[2]), actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "reject":
                return self._send(self.db.reject_transfer(int(parts[2]), actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "execute":
                return self._send(self.db.execute_transfer(int(parts[2]), actor, role))
            if parts == ["api", "usage"]:
                return self._send(self.db.record_usage(actor, body, role), 201)
            if parts == ["api", "dispatch", "orders"]:
                return self._send(self.db.publish_order(actor, body, role), 201)
            if parts == ["api", "dispatch", "drafts"]:
                return self._send(self.db.create_draft(actor, body, role), 201)
            if len(parts) == 5 and parts[:3] == ["api", "dispatch", "orders"] and parts[4] == "revoke":
                return self._send(self.db.revoke_order(int(parts[3]), actor, role,
                                                       body.get("base_version")), 200)
            if len(parts) == 6 and parts[:3] == ["api", "dispatch", "reviews"] and parts[5] == "resolve":
                return self._send(self.db.resolve_review(int(parts[3]), actor, role,
                                                         str(body.get("resolution", "confirmed"))), 200)
            raise DomainError("接口不存在", 404)
        except (ValueError, TypeError, DomainError) as exc:
            self._send({"error": str(exc), **getattr(exc, "extra", {})}, getattr(exc, "status", 400))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[water] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="跨区域水资源使用权分配、转让与干旱调度账服务")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8007")))
    parser.add_argument("--db", default=os.getenv("WATER_DB", str(DEFAULT_DB)))
    parser.add_argument("--init", action="store_true", help="创建数据库并写入示例账户")
    args = parser.parse_args()
    db = Database(args.db)
    if args.init:
        seed_demo(db)
        print(f"initialized database at {args.db}")
        return
    Handler.db = db
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"water-rights listening on http://127.0.0.1:{args.port} (db={args.db})")
    server.serve_forever()


if __name__ == "__main__":
    main()
