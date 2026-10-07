# 跨区域水资源使用权分配、转让与干旱调度账

一个仅使用 Python 标准库实现的水权账户、计量、转让审批、干旱情景与**干旱调度账**服务。SQLite 保存账户额度、取水记录、季节规则、上下游影响规则、调度令版本和完整审计日志。

干旱调度时，账户、待审转让和取水不再按旧依据放行：调度令、账户、转让、取水和审计接成**同一份调度账**。

## 运行

```bash
python app.py --init
python app.py --port 8007
```

打开 <http://127.0.0.1:8007>。`--init` 会创建北区水库和河口灌区两个示例账户，并添加一条 7 月季节上限和一条最小留存规则。数据库默认是 `water_rights.db`，可用 `--db` 或 `WATER_DB` 修改。

## 角色

请求头 `X-User` 和 `X-Role` 模拟身份：

- `editor`：建立账户、规则、发起转让、执行转让。
- `reviewer`：审批转让、参与差额复核。
- `meter`：登记取水。
- `dispatcher`（调度员）：起草/发布调度令。
- `supervisor`（调度主管）：撤销调度令、复核差额。
- `viewer`：只读。

## 调度账语义

- **发布留痕**：发布调度令时记录区域、区域限额和生效时刻，自动生成版本号与令号（`F-0000xx`）。
- **账户立即重算**：发布后受影响账户的调度后限额 = 区域限额 × 本账户许可额度 / 区域许可总额；可用量按调度后限额重算。
- **待审转让按发布版本重新占用**：每个版本有独立占用行（`dispatch_reservations`），可用量按当前发布版本口径计算；退回转让时释放对应占用。
- **已批准未执行先冻结**：批准转让不再立即划转额度，而是占用/冻结（`dispatch_freezes`），冻结期间禁止执行；执行（`POST /api/transfers/{id}/execute`）时才划转许可额度。
- **已执行或已取水保留当时依据**：历史记录的 `basis_order_id` 不被改写，仍锚定发布前基线（`PRE-<REGION>-0000`）；若旧账累计超出新限额，超出的**差额**写入待复核（`dispatch_reviews`），由主管确认或驳回。
- **撤销**：仅调度主管可撤销；撤销后该版本占用与冻结释放，账户回到许可额度口径。
- **并发**：发布/撤销用 `BEGIN IMMEDIATE` 加 `base_version` 乐观版本控制。两个调度员同时提交时，首份写入生效，另一位保留草稿（`D-0000xx`）并收到 409 冲突及当前版本；草稿随后可带新的 `base_version` 重新发布。
- **检查点恢复**：调度令与检查点先提交，账本效果随后幂等重放。若写库在两者之间失败，下次打开服务时自动从检查点恢复；冻结靠 `(order_id, transfer_id)` 唯一约束、审计靠 `idempotency_key` 去重，重放不新增冻结记录或审计。
- **旧数据回填**：旧库缺少依据编号时，启动按“发布前口径”为每个区域回填基线令（限额=该区域许可总额），并把历史转让、取水的依据绑定到基线；回填本身幂等。

## API

- `POST /api/accounts`：建立账户（额度、优先级、有效期）。
- `POST /api/rules/season` / `POST /api/rules/impact`：季节比例上限、上下游最小留存。
- `POST /api/transfers`：发起转让；待审批金额按当前调度版本预占。
- `POST /api/transfers/{id}/approve|reject`：审核；发起人不能审批自己的记录。
- `POST /api/transfers/{id}/execute`：执行已批准转让（划转许可额度；冻结期间拒绝）。
- `POST /api/usage`：登记实际取水，同一账户同一事件编号只入账一次；季节上限按当前生效限额计算。
- `GET /api/accounts/{id}/available`：当前口径可用额度，含 `dispatched_limit`、`reserved_outgoing`、`frozen_outgoing`、`basis_order_id`。
- `POST /api/dispatch/orders`：发布调度令，body：`region`、`cap`、可选 `effective_at`、`base_version`、`draft_id`。
- `POST /api/dispatch/drafts`：起草调度令（先留草稿不发布）。
- `POST /api/dispatch/orders/{id}/revoke`：主管撤销（body 可选 `base_version`）。
- `GET /api/dispatch/orders`：全部调度令（含草稿/基线）。
- `GET /api/dispatch/ledger?region=`：一份调度账——调度令、账户重算结果、待复核差额。
- `GET /api/dispatch/reviews?status=pending`：差额待复核列表。
- `POST /api/dispatch/reviews/{id}/resolve`：主管复核，body：`{"resolution":"confirmed|dismissed"}`。
- `GET /api/drought/simulate?supply=1000&reduction=0.3`：干旱分配，需求口径使用账户当前生效限额。
- `GET /api/audit`：完整操作审计（幂等重放不产生重复条目）。

## 测试

```bash
python -m unittest discover -s tests -v
```

覆盖原有转让审批/计量/规则流程，以及：发布重算与版本占用、已批准冻结与解冻执行、已执行/已取水保留依据与差额复核、并发发布首份生效与冲突草稿、越权撤销拒绝、检查点恢复幂等（不新增冻结/审计）、旧库发布前口径回填。
