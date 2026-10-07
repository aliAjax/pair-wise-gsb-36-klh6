"""Cross-region water-right allocation and transfer service (standard library only)."""
from __future__ import annotations

import argparse
import calendar
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


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_date(value: str, field: str = "日期") -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DomainError(f"{field}必须是 YYYY-MM-DD") from exc


def parse_datetime(value: str, field: str = "生效时刻") -> str:
    """Normalize a date or datetime to a UTC ISO string so lexical compare works."""
    try:
        dt = datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise DomainError(f"{field}必须是 ISO 日期或时间") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class Database:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = str(path)
        self._init_schema()

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
                    executed_by TEXT,
                    executed_at TEXT,
                    dispatch_version INTEGER
                );
                CREATE TABLE IF NOT EXISTS usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    meter_event_id TEXT NOT NULL,
                    amount REAL NOT NULL CHECK(amount > 0),
                    occurred_at TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    dispatch_version INTEGER,
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
                    region TEXT NOT NULL,
                    limit_amount REAL NOT NULL CHECK(limit_amount >= 0),
                    effective_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    version INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    published_by TEXT,
                    published_at TEXT,
                    revoked_by TEXT,
                    revoked_at TEXT,
                    checkpoint TEXT,
                    checkpoint_applied INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS freeze_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dispatch_order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    transfer_id INTEGER NOT NULL REFERENCES transfers(id),
                    amount REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(dispatch_order_id, transfer_id)
                );
                CREATE TABLE IF NOT EXISTS review_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dispatch_order_id INTEGER NOT NULL REFERENCES dispatch_orders(id),
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending_review',
                    created_at TEXT NOT NULL
                );
                """
            )
            # 旧库缺少调度编号等列时按结构补齐，数据编号在发布时按发布前口径回填。
            self._ensure_column(conn, "transfers", "dispatch_version", "dispatch_version INTEGER")
            self._ensure_column(conn, "transfers", "executed_by", "executed_by TEXT")
            self._ensure_column(conn, "transfers", "executed_at", "executed_at TEXT")
            self._ensure_column(conn, "usage_records", "dispatch_version", "dispatch_version INTEGER")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

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
            try:
                cur = conn.execute(
                    "INSERT INTO accounts(name,region,holder,priority,valid_from,valid_to,quota,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (name, region, holder, priority, valid_from.isoformat(), valid_to.isoformat(), quota, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("账户名称已存在", 409) from exc
            self._audit(conn, actor, "account.created", "account", cur.lastrowid, {"name": name, "quota": quota})
            return dict(conn.execute("SELECT * FROM accounts WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_season_rule(self, actor: str, region: str, month: int, max_fraction: float,
                        note: str = "", role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以设置季节规则", 403)
        if not 1 <= int(month) <= 12 or not 0 < float(max_fraction) <= 1:
            raise DomainError("月份或季节比例不合法")
        with self.connect() as conn:
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

    def _reserved_outgoing(self, conn: sqlite3.Connection, account_id: int) -> float:
        # 待审、已批准未执行和被调度冻结的转让都占用转出方额度。
        row = conn.execute(
            "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status IN ('pending','approved','frozen')",
            (account_id,),
        ).fetchone()
        return float(row["total"])

    def _active_dispatch(self, conn: sqlite3.Connection, region: str) -> sqlite3.Row | None:
        return conn.execute(
            """SELECT * FROM dispatch_orders
               WHERE region=? AND status='published' AND effective_at<=?
               ORDER BY version DESC, id DESC LIMIT 1""",
            (region, utcnow()),
        ).fetchone()

    def _quota_cap(self, conn: sqlite3.Connection, account: sqlite3.Row) -> tuple[float, sqlite3.Row | None]:
        """Effective quota ceiling for an account once the dispatch order applies."""
        order = self._active_dispatch(conn, account["region"])
        cap = float(account["quota"])
        if order is not None:
            cap = min(cap, float(order["limit_amount"]))
        return cap, order

    def available(self, account_id: int, as_of: str | None = None) -> dict[str, Any]:
        if as_of:
            parse_date(as_of, "查询日期")
        with self.connect() as conn:
            account = self._account_row(conn, account_id)
            reserved = self._reserved_outgoing(conn, account_id)
            cap, order = self._quota_cap(conn, account)
            value = max(0.0, cap - float(account["used"]) - reserved)
        return {"account_id": account_id, "available": value, "reserved_outgoing": reserved,
                "quota": account["quota"], "used": account["used"],
                "dispatch_limit": float(order["limit_amount"]) if order else None,
                "dispatch_version": int(order["version"]) if order else None}

    def create_transfer(self, actor: str, payload: dict[str, Any], role: str = "editor") -> dict[str, Any]:
        if role != "editor":
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
            if not (source["valid_from"] <= effective.isoformat() <= source["valid_to"]):
                raise DomainError("转出账户在生效日无效", 409)
            if not (target["valid_from"] <= effective.isoformat() <= target["valid_to"]):
                raise DomainError("转入账户在生效日无效", 409)
            reserved = self._reserved_outgoing(conn, source_id)
            cap, order = self._quota_cap(conn, source)
            available = cap - float(source["used"]) - reserved
            if amount > available + 1e-9:
                raise DomainError("可用额度不足，待审批转让会预占额度", 409)
            # More critical users (smaller priority number) cannot transfer their
            # protected allocation to a less critical user.
            if int(source["priority"]) > int(target["priority"]):
                raise DomainError("不能把较低优先级水量转给更高优先级账户", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                remaining = available - amount
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if remaining + 1e-9 < minimum:
                    raise DomainError("转让会违反下游第三方最小留存约束", 409)
            dispatch_version = int(order["version"]) if order else None
            cur = conn.execute(
                "INSERT INTO transfers(from_account_id,to_account_id,amount,effective_date,created_by,created_at,dispatch_version) VALUES(?,?,?,?,?,?,?)",
                (source_id, target_id, amount, effective.isoformat(), actor, utcnow(), dispatch_version),
            )
            self._audit(conn, actor, "transfer.created", "transfer", cur.lastrowid,
                        {"source": source_id, "target": target_id, "amount": amount, "effective_date": effective.isoformat()})
            return dict(conn.execute("SELECT * FROM transfers WHERE id=?", (cur.lastrowid,)).fetchone())

    def approve_transfer(self, transfer_id: int, actor: str, role: str = "reviewer") -> dict[str, Any]:
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
            # Compute against reservations other than this transfer.
            other_reserved = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status IN ('pending','approved','frozen') AND id<>?",
                (source["id"], transfer_id),
            ).fetchone()["total"]
            cap, _order = self._quota_cap(conn, source)
            available = cap - float(source["used"]) - float(other_reserved)
            if amount > available + 1e-9:
                raise DomainError("审批时额度已被其他记录占用，不能批准", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if available - amount + 1e-9 < minimum:
                    raise DomainError("审批时下游最小留存约束不再满足", 409)
            # Approval only marks the transfer approved; the quota movement happens
            # at execution so a dispatch order can still freeze approved water.
            conn.execute("UPDATE transfers SET status='approved',approved_by=?,approved_at=? WHERE id=?", (actor, utcnow(), transfer_id))
            self._audit(conn, actor, "transfer.approved", "transfer", transfer_id, {"amount": amount})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        return dict(row)

    def execute_transfer(self, transfer_id: int, actor: str, role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有水权编辑人员可以执行转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not transfer:
                raise DomainError("转让记录不存在", 404)
            if transfer["status"] == "frozen":
                raise DomainError("转让已被调度令冻结，不能执行", 409)
            if transfer["status"] != "approved":
                raise DomainError("只有已批准未执行的转让可以执行", 409)
            source = self._account_row(conn, transfer["from_account_id"])
            target = self._account_row(conn, transfer["to_account_id"])
            amount = float(transfer["amount"])
            # Re-check against the current dispatch basis instead of the approval-time one.
            other_reserved = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status IN ('pending','approved','frozen') AND id<>?",
                (source["id"], transfer_id),
            ).fetchone()["total"]
            cap, order = self._quota_cap(conn, source)
            available = cap - float(source["used"]) - float(other_reserved)
            if amount > available + 1e-9:
                raise DomainError("执行时额度不足或受调度令限制，不能执行", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if available - amount + 1e-9 < minimum:
                    raise DomainError("执行时下游最小留存约束不再满足", 409)
            # The approved amount moves between quota balances only at execution.
            conn.execute("UPDATE accounts SET quota=quota-? WHERE id=?", (amount, source["id"]))
            conn.execute("UPDATE accounts SET quota=quota+? WHERE id=?", (amount, target["id"]))
            dispatch_version = int(order["version"]) if order else transfer["dispatch_version"]
            conn.execute(
                "UPDATE transfers SET status='executed',executed_by=?,executed_at=?,dispatch_version=? WHERE id=?",
                (actor, utcnow(), dispatch_version, transfer_id),
            )
            self._audit(conn, actor, "transfer.executed", "transfer", transfer_id, {"amount": amount})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        return dict(row)

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
            conn.execute("UPDATE transfers SET status='rejected',approved_by=?,approved_at=? WHERE id=?", (actor, utcnow(), transfer_id))
            self._audit(conn, actor, "transfer.rejected", "transfer", transfer_id, {})
        return {"id": transfer_id, "status": "rejected"}

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
            reserved = self._reserved_outgoing(conn, account_id)
            cap, order = self._quota_cap(conn, account)
            available = cap - float(account["used"]) - reserved
            if amount > available + 1e-9:
                raise DomainError("取水超过可用额度", 409)
            season = conn.execute("SELECT max_fraction FROM season_rules WHERE region=? AND month=?", (account["region"], occurred.month)).fetchone()
            month_total = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM usage_records WHERE account_id=? AND substr(occurred_at,1,7)=?",
                (account_id, occurred.strftime("%Y-%m")),
            ).fetchone()["total"]
            if season:
                cap = float(account["quota"]) * float(season["max_fraction"])
                if float(month_total) + amount > cap + 1e-9:
                    raise DomainError("本次取水超过该月份的季节配额", 409)
            try:
                cur = conn.execute(
                    "INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at,dispatch_version) VALUES(?,?,?,?,?,?,?)",
                    (account_id, meter_event_id, amount, occurred.isoformat(), actor, utcnow(),
                     int(order["version"]) if order else None),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("计量事件已登记，不能重复计水", 409) from exc
            conn.execute("UPDATE accounts SET used=used+? WHERE id=?", (amount, account_id))
            self._audit(conn, actor, "usage.recorded", "account", account_id,
                        {"amount": amount, "occurred_at": occurred.isoformat(), "meter_event_id": meter_event_id})
            row = conn.execute("SELECT * FROM usage_records WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(row)

    def simulate_drought(self, total_supply: float, reduction: float = 0.0, role: str = "viewer") -> dict[str, Any]:
        try:
            total_supply, reduction = float(total_supply), float(reduction)
        except (TypeError, ValueError) as exc:
            raise DomainError("供水量和削减比例必须是数值") from exc
        if total_supply < 0 or not 0 <= reduction < 1:
            raise DomainError("供水量不能为负，削减比例应在 0 到 1 之间")
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY priority,name").fetchall()
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
            requested = sum(max(0.0, float(r["quota"]) - float(r["used"])) for r in group)
            take = min(remaining, requested)
            if requested <= 0:
                continue
            for row in group:
                quota_left = max(0.0, float(row["quota"]) - float(row["used"]))
                share = take * quota_left / requested
                allocation[int(row["id"])] = share
                deficit[int(row["id"])] = quota_left - share
            remaining -= take
            if remaining <= 1e-9:
                for lower in rows:
                    if int(lower["priority"]) > priority:
                        left = max(0.0, float(lower["quota"]) - float(lower["used"]))
                        allocation[int(lower["id"])] = 0.0
                        deficit[int(lower["id"])] = left
                break
        return {"total_supply": total_supply, "reduction": reduction, "effective_supply": supply,
                "unallocated": remaining, "allocations": [
                    {"account_id": int(r["id"]), "name": r["name"], "priority": r["priority"],
                     "allocation": allocation.get(int(r["id"]), 0.0), "deficit": deficit.get(int(r["id"]), 0.0)}
                    for r in rows
                ]}

    # ---- 调度令：发布、撤销、检查点恢复，共用同一份调度账 ----

    def _dispatch_row(self, conn: sqlite3.Connection, order_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (order_id,)).fetchone()
        if not row:
            raise DomainError("调度令不存在", 404)
        return row

    def create_dispatch(self, actor: str, payload: dict[str, Any], role: str = "dispatcher") -> dict[str, Any]:
        if role not in {"dispatcher", "supervisor"}:
            raise DomainError("只有调度员可以创建调度令", 403)
        region = str(payload.get("region", "")).strip()
        if not region:
            raise DomainError("调度令区域不能为空")
        try:
            limit = float(payload.get("limit_amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("调度限额必须是数值") from exc
        if limit < 0:
            raise DomainError("调度限额不能为负")
        effective_at = parse_datetime(str(payload.get("effective_at", "")), "生效时刻")
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO dispatch_orders(region,limit_amount,effective_at,created_by,created_at) VALUES(?,?,?,?,?)",
                (region, limit, effective_at, actor, utcnow()),
            )
            self._audit(conn, actor, "dispatch.created", "dispatch_order", cur.lastrowid,
                        {"region": region, "limit_amount": limit, "effective_at": effective_at})
            return dict(conn.execute("SELECT * FROM dispatch_orders WHERE id=?", (cur.lastrowid,)).fetchone())

    @staticmethod
    def _require_expected_version(order: sqlite3.Row, expected_version: Any) -> None:
        if expected_version is None:
            raise DomainError("必须携带预期版本号，以便发现并发提交冲突")
        if int(order["version"]) != int(expected_version):
            raise DomainError("调度令版本冲突：他人已先行提交，本次草稿保留", 409)

    def _apply_checkpoint(self, conn: sqlite3.Connection, order: sqlite3.Row) -> None:
        """Replay the stored checkpoint. Only idempotent UPDATEs run here, so
        replaying after a failed write never adds freeze records or audit rows."""
        plan = json.loads(order["checkpoint"])
        account_ids = plan.get("account_ids", [])
        freeze_ids = plan.get("freeze_transfer_ids", [])
        if freeze_ids:
            marks = ",".join("?" * len(freeze_ids))
            conn.execute(f"UPDATE transfers SET status='frozen' WHERE status='approved' AND id IN ({marks})", freeze_ids)
        if account_ids:
            marks = ",".join("?" * len(account_ids))
            # 待审转让按本次发布版本重新占用。
            conn.execute(
                f"UPDATE transfers SET dispatch_version=? WHERE status='pending' AND from_account_id IN ({marks})",
                [plan["version"], *account_ids],
            )
            # 旧数据缺少编号时按发布前口径回填。
            conn.execute(
                f"UPDATE transfers SET dispatch_version=? WHERE dispatch_version IS NULL AND from_account_id IN ({marks})",
                [plan["prev_version"], *account_ids],
            )
            conn.execute(
                f"UPDATE usage_records SET dispatch_version=? WHERE dispatch_version IS NULL AND account_id IN ({marks})",
                [plan["prev_version"], *account_ids],
            )
        conn.execute("UPDATE dispatch_orders SET checkpoint_applied=1 WHERE id=?", (order["id"],))

    def publish_dispatch(self, order_id: int, actor: str, role: str = "dispatcher",
                         expected_version: Any = None, updates: dict[str, Any] | None = None,
                         fail_after_checkpoint: bool = False) -> dict[str, Any]:
        if role not in {"dispatcher", "supervisor"}:
            raise DomainError("只有调度员可以发布调度令", 403)
        updates = updates or {}
        # 第一阶段：版本校验后把发布内容、冻结记录、待复核差额和检查点一次写清。
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = self._dispatch_row(conn, order_id)
            if order["status"] == "revoked":
                raise DomainError("调度令已撤销，不能再次发布", 409)
            self._require_expected_version(order, expected_version)
            region = str(updates.get("region") or order["region"]).strip()
            try:
                limit = float(updates.get("limit_amount", order["limit_amount"]))
            except (TypeError, ValueError) as exc:
                raise DomainError("调度限额必须是数值") from exc
            if limit < 0:
                raise DomainError("调度限额不能为负")
            effective_at = (parse_datetime(str(updates["effective_at"]), "生效时刻")
                            if updates.get("effective_at") else order["effective_at"])
            new_version = int(order["version"]) + 1
            accounts = conn.execute("SELECT * FROM accounts WHERE region=?", (region,)).fetchall()
            account_ids = [int(a["id"]) for a in accounts]
            if account_ids:
                marks = ",".join("?" * len(account_ids))
                freeze_rows = conn.execute(
                    f"SELECT id, amount FROM transfers WHERE status='approved' AND from_account_id IN ({marks})",
                    account_ids,
                ).fetchall()
            else:
                freeze_rows = []
            prev_version = int(conn.execute(
                "SELECT COALESCE(MAX(version),0) v FROM dispatch_orders WHERE region=? AND status='published'",
                (region,),
            ).fetchone()["v"])
            # 已执行或已取水的部分保留当时依据，只把超出新限额的差额转待复核。
            reviews = []
            for a in accounts:
                excess = float(a["used"]) - min(float(a["quota"]), limit)
                if excess > 1e-9:
                    reviews.append((int(a["id"]), round(excess, 9)))
            plan = {"version": new_version, "region": region, "limit_amount": limit,
                    "effective_at": effective_at, "account_ids": account_ids,
                    "freeze_transfer_ids": [int(r["id"]) for r in freeze_rows],
                    "prev_version": prev_version}
            conn.execute(
                """UPDATE dispatch_orders SET status='published',version=?,region=?,limit_amount=?,effective_at=?,
                   published_by=?,published_at=?,checkpoint=?,checkpoint_applied=0 WHERE id=?""",
                (new_version, region, limit, effective_at, actor, utcnow(),
                 json.dumps(plan, ensure_ascii=False), order_id),
            )
            for row in freeze_rows:
                conn.execute(
                    "INSERT OR IGNORE INTO freeze_records(dispatch_order_id,transfer_id,amount,created_at) VALUES(?,?,?,?)",
                    (order_id, int(row["id"]), float(row["amount"]), utcnow()),
                )
            for account_id, excess in reviews:
                conn.execute(
                    "INSERT INTO review_items(dispatch_order_id,entity_type,entity_id,amount,created_at) VALUES(?,?,?,?,?)",
                    (order_id, "account", account_id, excess, utcnow()),
                )
            self._audit(conn, actor, "dispatch.published", "dispatch_order", order_id,
                        {"version": new_version, "region": region, "limit_amount": limit,
                         "effective_at": effective_at, "frozen_transfers": plan["freeze_transfer_ids"],
                         "review_items": len(reviews)})
        if fail_after_checkpoint:
            # 模拟应用阶段写库失败：检查点已提交，等待恢复重放。
            raise DomainError("调度令应用阶段写库失败，已保留检查点，请调用恢复接口", 500)
        # 第二阶段：应用检查点（状态冻结、待审重占、旧数据回填）。
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._apply_checkpoint(conn, self._dispatch_row(conn, order_id))
        return self.get_dispatch(order_id)

    def revoke_dispatch(self, order_id: int, actor: str, role: str = "supervisor",
                        expected_version: Any = None) -> dict[str, Any]:
        if role != "supervisor":
            raise DomainError("只有调度主管可以撤销调度令", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = self._dispatch_row(conn, order_id)
            if order["status"] != "published":
                raise DomainError("调度令未发布或已撤销", 409)
            self._require_expected_version(order, expected_version)
            if order["checkpoint"] and not order["checkpoint_applied"]:
                self._apply_checkpoint(conn, order)  # 先补齐未应用的检查点，保持账目一致
            new_version = int(order["version"]) + 1
            conn.execute(
                "UPDATE dispatch_orders SET status='revoked',version=?,revoked_by=?,revoked_at=? WHERE id=?",
                (new_version, actor, utcnow(), order_id),
            )
            frozen = conn.execute(
                "SELECT transfer_id FROM freeze_records WHERE dispatch_order_id=?", (order_id,),
            ).fetchall()
            if frozen:
                marks = ",".join("?" * len(frozen))
                conn.execute(
                    f"UPDATE transfers SET status='approved' WHERE status='frozen' AND id IN ({marks})",
                    [int(r["transfer_id"]) for r in frozen],
                )
            self._audit(conn, actor, "dispatch.revoked", "dispatch_order", order_id,
                        {"version": new_version,
                         "unfrozen_transfers": [int(r["transfer_id"]) for r in frozen]})
        return self.get_dispatch(order_id)

    def recover_dispatch(self, order_id: int, actor: str, role: str = "dispatcher") -> dict[str, Any]:
        if role not in {"dispatcher", "supervisor"}:
            raise DomainError("只有调度员可以恢复调度令", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = self._dispatch_row(conn, order_id)
            if not order["checkpoint"] or order["checkpoint_applied"]:
                return {"id": order_id, "recovered": False, "detail": "没有待恢复的检查点"}
            self._apply_checkpoint(conn, order)
            version = int(json.loads(order["checkpoint"])["version"])
        return {"id": order_id, "recovered": True, "version": version}

    def get_dispatch(self, order_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = self._dispatch_row(conn, order_id)
        item = dict(row)
        item["checkpoint"] = json.loads(row["checkpoint"]) if row["checkpoint"] else None
        return item

    def list_dispatch(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM dispatch_orders ORDER BY id DESC").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["checkpoint"] = json.loads(row["checkpoint"]) if row["checkpoint"] else None
            result.append(item)
        return result

    def list_freeze_records(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM freeze_records ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    def list_review_items(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM review_items ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    def list_accounts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
            result = []
            for row in rows:
                item = dict(row)
                cap, order = self._quota_cap(conn, row)
                item["available"] = max(0.0, cap - float(row["used"]) - self._reserved_outgoing(conn, int(row["id"])))
                item["dispatch_limit"] = float(order["limit_amount"]) if order else None
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
            if parsed.path in {"/", "/index.html"}:
                return self._html()
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            if parsed.path == "/api/accounts":
                return self._send({"accounts": self.db.list_accounts()})
            if parsed.path == "/api/transfers":
                return self._send({"transfers": self.db.list_transfers()})
            if parsed.path == "/api/dispatch":
                return self._send({"orders": self.db.list_dispatch()})
            if parsed.path == "/api/review":
                return self._send({"review": self.db.list_review_items()})
            if parsed.path == "/api/freezes":
                return self._send({"freezes": self.db.list_freeze_records()})
            if parsed.path == "/api/audit":
                return self._send({"audit": self.db.audit()})
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 3 and parts[:2] == ["api", "dispatch"]:
                return self._send(self.db.get_dispatch(int(parts[2])))
            if parsed.path.startswith("/api/accounts/") and parsed.path.endswith("/available"):
                account_id = int(parsed.path.split("/")[3])
                return self._send(self.db.available(account_id))
            if parsed.path == "/api/drought/simulate":
                q = parse_qs(parsed.query)
                return self._send(self.db.simulate_drought(float(q.get("supply", ["0"])[0]), float(q.get("reduction", ["0"])[0])))
            raise DomainError("接口不存在", 404)
        except (ValueError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

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
            if parts == ["api", "dispatch"]:
                return self._send(self.db.create_dispatch(actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "dispatch"] and parts[3] == "publish":
                return self._send(self.db.publish_dispatch(
                    int(parts[2]), actor, role, body.get("expected_version"), body,
                    bool(body.get("simulate_failure"))))
            if len(parts) == 4 and parts[:2] == ["api", "dispatch"] and parts[3] == "revoke":
                return self._send(self.db.revoke_dispatch(int(parts[2]), actor, role, body.get("expected_version")))
            if len(parts) == 4 and parts[:2] == ["api", "dispatch"] and parts[3] == "recover":
                return self._send(self.db.recover_dispatch(int(parts[2]), actor, role))
            if parts == ["api", "usage"]:
                return self._send(self.db.record_usage(actor, body, role), 201)
            raise DomainError("接口不存在", 404)
        except (ValueError, TypeError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[water] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="跨区域水资源使用权分配与转让服务")
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
