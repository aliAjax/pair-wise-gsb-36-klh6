# 跨区域水资源使用权分配与转让

一个仅使用 Python 标准库实现的水权账户、计量、转让审批、干旱调度令和干旱情景服务。SQLite 保存账户额度、取水记录、季节规则、上下游影响规则、调度令和完整审计日志。

## 运行

```bash
python app.py --init
python app.py --port 8007
```

打开 <http://127.0.0.1:8007>。`--init` 会创建北区水库和河口灌区两个示例账户，并添加一条 7 月季节上限和一条最小留存规则。数据库默认是 `water_rights.db`，可用 `--db` 或 `WATER_DB` 修改。

## API

请求头 `X-User` 和 `X-Role` 用来模拟身份。角色包括 `editor`、`reviewer`、`meter`、`viewer`、`dispatcher`（调度员）、`supervisor`（调度主管）。

- `POST /api/accounts`：建立账户（额度、优先级、有效期）。
- `POST /api/rules/season`：设置某地区某月份的用水比例上限。
- `POST /api/rules/impact`：设置上下游转让的最小留存比例。
- `POST /api/transfers`：发起转让；待审批金额立即预占，避免同一额度被重复转卖。
- `POST /api/transfers/{id}/approve|reject`：审核；发起人不能审批自己的记录。批准只锁定额度，不移动配额。
- `POST /api/transfers/{id}/execute`：执行已批准未执行的转让，配额在此刻移动；执行前按当前调度依据重新校验，被冻结的转让不能执行。
- `POST /api/usage`：按计量事件登记实际取水，同一账户同一事件编号只会入账一次。
- `GET /api/accounts/{id}/available`：查看扣减实际用量和占用后的可用额度；已生效的调度令会把可用上限压到 `min(许可额度, 调度限额)`。
- `POST /api/dispatch`：创建调度令草稿（区域、限额、生效时刻）。
- `POST /api/dispatch/{id}/publish`：发布或更新调度令，需携带 `expected_version`。发布后受影响账户可用量立即重算，待审转让按发布版本重新占用，已批准未执行的转让先冻结，已执行或已取水的保留当时依据、超出限额的差额转待复核。
- `POST /api/dispatch/{id}/revoke`：撤销调度令（仅调度主管），解除本令造成的冻结。
- `POST /api/dispatch/{id}/recover`：写库失败后从调度令检查点恢复；重放只做幂等的状态更新，不新增冻结记录或审计。
- `GET /api/dispatch`、`GET /api/review`、`GET /api/freezes`：查看调度令、待复核差额和冻结记录。
- `GET /api/drought/simulate?supply=1000&reduction=0.3`：按高优先级先行分配，同级账户按剩余额度比例分配。
- `GET /api/audit`：完整操作审计。

余额计算、审批和调度令发布使用 `BEGIN IMMEDIATE`，把余额判断与写入放在同一事务中；因此并发提交不会绕过额度检查。两个调度员同时提交发布或撤销时，以 `expected_version` 做乐观并发控制：首份写入生效，另一方的草稿保留并收到版本冲突。最小留存比例按转出账户的当前许可额度计算。转让和取水记录带有 `dispatch_version` 作为当时依据；旧数据缺少编号时，在下一次发布时按发布前口径回填。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖转让审批执行与实际计量、重复计量事件、季节/最小留存规则、预占导致余额不足、发起人自审冲突，以及调度令的发布重算、冻结、待复核差额、版本冲突、检查点恢复、旧数据回填和撤销权限。
