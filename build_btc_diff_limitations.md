# build_btc_diff limitations

本文说明 [build_btc_diff.rs](/home/aimee/order_book_server/binaries/src/bin/build_btc_diff.rs) 当前仅基于 `replica_cmds` 和 `node_fills_by_block` 生成 BTC diff parquet 时的能力边界、和参考文件的主要差异来源，以及当前程序对这些差异的处理策略。

比较口径：

- 参考文件：`/home/aimee/BTC_diff_HFT_951980001_952000000.parquet`
- 当前程序输出样本：`btc_diff_951980001_span10000_matchfix9.parquet`
- 重叠区间：`951980001..951990000`

## 输入与基本假设

程序只使用两类输入：

- `replica_cmds`
  - 提供 `order / cancel / cancelByCloid / modify / batchModify` 等 action 和对应 response
  - 能提供 `oid / cloid / side / px / order_sz / tif / reduce_only / trigger` 等信息
- `node_fills_by_block`
  - 提供成交事件
  - 当前用同 block 内 `tid` 把 `crossed=true` taker fill 和 `crossed=false` maker fill 配对

这意味着程序没有以下信息：

- 区间起点之前的完整 live order 快照
- 所有取消原因的精细枚举
- 所有改单成功后的完整新旧状态链
- 所有触发单内部状态迁移
- 参考文件生成链路里可能存在的额外上游内部字段

## 当前程序的核心策略

- 非 trigger `order` 产生 `new/add`
- 非 trigger `modify/batchModify` 产生旧单 `remove/cancel`，以及在 response 可恢复时产生新单 `new/add`
- `update/fill` 只由 `node_fills_by_block` 生成
- `remove/fill` 只在 live state 已知且剩余数量被 fill 扣到 0 时生成
- trigger 单不进入 live state，不落 parquet
- trigger 的 `modify / batchModify / cancel` 当前直接跳过
- `modify / batchModify` 命中 `Cannot modify canceled or filled order` 时直接跳过
- 非 trigger `cancel success` 若当前没有 live state，仍保留 non-null fallback `remove/cancel`
- 非 trigger `modify type=default` 若没有 `statuses`，仍保留 non-null fallback `remove/cancel`

## new/add

### 当前可做的事

- 普通 `order` 成功且 response 能给出 `resting.oid` 时，可稳定生成 `new/add`
- `Gtc` 限价单如果 response 只有 `filled`，也会用 `order_sz - filled.totalSz` 计算是否仍有剩余入簿
- `raw_sz` 当前只在 `new/add` 上有意义
  - 含义是原始提交数量
  - `sz` 是实际入簿数量

### 局限

- 如果 `order / modify / batchModify` 返回 `type=default` 或没有 `statuses`，程序通常无法恢复新单 `oid`
- 对于 reference 中存在、但当前 `replica_cmds` 里没有足够可见痕迹的订单，程序无法凭空补出 `new/add`
- trigger 单当前完全不进 parquet，因此触发链路相关的 `new/add` 不会生成
- 有少量 reduce-only 场景，参考中的 `sz/raw_sz` 需要结合持仓才能还原，当前仅凭本地数据无法精确识别

### 当前和参考的差异

当前结果：

- `new/add`: `missing=72`, `extra=1`
- 同键错量：`5`

主要原因：

- `modify / batchModify` 的 `type=default / no-status` 路径拿不到新 `oid`
- 少量 reference 中的订单在当前可见输入里没有完整链路
- `1` 条 extra 来自当前对 `filled-only GTC` 的保守入簿判断
- `5` 条同键错量是 reduce-only 超过仓位大小的特殊情况，当前数据无法识别，属于已知接受项

## update/fill

### 当前可做的事

- 只要 `node_fills_by_block` 在同 block 内能找到同 `tid` 的 taker 和 maker 配对，程序就能生成 `update/fill`
- 当前只把 maker side 的 fill 写入 parquet
- `update/fill` 的 `sz / orig_sz / raw_sz` 当前都留空

### 局限

- `update/fill` 完全依赖 `node_fills_by_block`
- 如果 fills 缺失、错块、或同 `tid` 配对信息异常，程序无法从 `replica_cmds` 补一条可信的 fill
- `update/cancel` 当前没有实现，参考里存在的缩量链不会直接输出

### 当前和参考的差异

当前结果：

- `update/fill`: `missing=0`, `extra=0`
- `update/cancel`: `missing=73`, `extra=0`

说明：

- `update/fill` 已和参考对齐
- `update/cancel` 当前全缺失，这是实现选择，不是偶发 bug

## remove/fill

### 当前可做的事

- 当某个 live order 已知存在，并且累计 fill 把 `remaining_sz` 扣到 `0`，程序会生成 `remove/fill`

### 局限

- 没有 warmup 时，区间开始前就已存在的 live order 不在内存 state 中
- 如果前序 `new/add` 丢失，后续 fill 无法触发 `remove/fill`
- 参考里的 `update/cancel` 缩量链当前不输出，且内部 state 也未完全吸收，因此部分订单最终不会被扣到 `0`

### 当前和参考的差异

当前结果：

- `remove/fill`: `missing=135`, `extra=0`

主要原因：

- 绝大多数是 no-warmup 导致的前序 live order 不存在
- 少量是前序 `new/add` 缺失的下游结果
- 少量是 reference 依赖 `update/cancel` 先缩量再最终 fill 清零

## remove/cancel

### 当前可做的事

- 如果 cancel / modify 命中的 old order 当前在 live state 中，程序会输出带 `side / px / orig_sz` 的 rich `remove/cancel`
- 如果无法解析出 live state，但 action 明确引用了 non-trigger `oid`，程序仍可输出 non-null fallback `remove/cancel`

### 局限

- 当前没有完整的取消原因恢复能力
  - `reduceOnlyCanceled`
  - `selfTradeCanceled`
  - 以及更多细粒度 reason
- `cancelByCloid` 如果映射链不完整，可能只能降级成 `oid=NULL`
- 对于 non-trigger `cancel success`，上游返回成功不等于参考文件一定记录这条 cancel
- 对于 non-trigger `modify type=default`，只能保守地把旧单当成 fallback cancel，无法确认参考是否应记录

### 当前和参考的差异

当前结果：

- `remove/cancel`: `missing=732`, `extra=63`
- 新文件里的 `oid=NULL remove/cancel`: `536`

`missing=732` 的构成：

- `538` 条：参考是非空 `oid canceled`，当前降级成了 `oid=NULL canceled`
- `192` 条：`reduceOnlyCanceled`
- `2` 条：`selfTradeCanceled`

`extra=63` 的构成：

- `63/63` 都是 non-trigger `cancel`
- `63/63` 的原始响应都是 `status=ok, type=cancel, statuses[0]=success`
- 这些行当前都表现为 non-null fallback cancel，且参考里没有对应 `user+oid` cancel

这 63 条是当前程序和参考文件最主要的剩余策略差异之一：

- 当前程序：保留 non-trigger `cancel success` 的 non-null fallback
- 参考文件：并不总是记录这类 cancel

## 触发单和参考文件的差异

当前程序对 trigger 单采取的是“完全不入 live state，不落 parquet”的保守策略：

- trigger `order` 不写 `new/add`
- trigger `modify / batchModify` 不写 `remove/cancel` 或 `new/add`
- trigger `cancel` 直接跳过

这样做的原因是：

- 仅凭 `replica_cmds + node_fills_by_block`，触发单状态链最不完整
- `type=default`
- trigger 改单 success 但 reference 不记 old cancel
- trigger 改单命中 dead order

这些路径很容易制造假的 `remove/cancel`

代价是：

- 参考里和 trigger 相关的部分记录会缺失

## 当前程序与参考文件的整体关系

相同点：

- `update/fill` 已对齐
- 大部分普通 `new/add` 与普通 `remove/cancel` 已能从 visible inputs 重建
- 非 trigger 的 `modify -> old cancel + new add` 主链已经基本成立

不同点：

- 当前程序不输出 `update/cancel`
- 当前程序不维护 trigger 单 diff
- 当前程序保留 non-trigger `cancel success` 的 fallback cancel，而参考并不总保留
- 当前程序无法恢复所有 `type=default / no-status` 场景下的新 `oid`
- 当前程序在 no-warmup 模式下天然会丢掉一部分 `remove/fill`

因此，当前程序更适合被理解为：

- 一个仅基于公开 `cmds + fills_by_block` 可见信息重建的 diff 生成器
- 它可以非常接近参考文件，但不能保证在所有 cancel reason、trigger 链路、pre-range live orders、以及 `type=default` 降级返回上完全等价
