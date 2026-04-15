# build_btc_diff limitations

本文说明 [build_btc_diff.rs](/home/aimee/order_book_server/binaries/src/bin/build_btc_diff.rs) 当前仅基于 `replica_cmds` 和 `node_fills_by_block` 生成 BTC diff parquet 时的能力边界、和参考文件的主要差异来源，以及当前程序对这些差异的处理策略。

比较口径：

- 参考文件：`/home/aimee/BTC_diff_HFT_951980001_952000000.parquet`
- 当前程序输出样本：`/tmp/btc_diff_951980001_span20000_triggerfix_waitfill.parquet`
- 重叠区间：`951980001..952000000`

本轮代码变更后做了快速回归验证（`2026-04-12`, `span=2000`）：

- 编译命令：`RUSTFLAGS="-C target-cpu=native" cargo build --release -p binaries --bin build_btc_diff`
- 执行命令：

```bash
./target/release/build_btc_diff \
  --output-parquet "$out_dir" \
  --unknown-oid-log-sqlite "$out_sqlite" \
  --start-height "$START" \
  --height-span "$SPAN" \
  --warmup "$WARMUP" \
  --warmup-state-output-dir "$state_dir" \
  --warmup-state-file "$state_file" \
  --replica-prefetch-workers 4 \
  --replica-prefetch 16
```
- 输出目录：`/tmp/btc_diff_out`（文件名由程序按实际起止高度生成）
- 状态快照目录：`--warmup-state-output-dir` 可选；warmup 期间不落快照，warmup 结束块（`output_start-1`，例如 `938000000`）会立即输出一次 `warmup_state_<block>.msgpack.zst`，之后按 1m block 边界继续输出（zstd level 3）
- 状态恢复文件：`--warmup-state-file` 可选，启动时先恢复 live state/trigger 索引，缩短或消除 warmup 回放窗口
- `remove/fill` 行数：`0`
- `remove/cancel` 中 `oid IS NULL` 行数：`0`

当前 BTC diff schema 已收敛为：

- 已移除：`diff_type`、`status`、`trigger_condition`、`trigger_px`、`is_position_tpsl`、`tp_trigger_px`、`sl_trigger_px`
- 新增：`fill_sz`，仅 `update/fill` 行有效
- `lifetime` 采用 6 位整数编码：`<= 999999` 时写入毫秒，超过后写入负分钟

## 最近主要改动

下面是这轮 git 变更的主线，便于快速理解当前代码和旧版本的差异：

- `replica_cmds` 读路径新增并行 LZ4 分块解压，并保持输出顺序不变。
- 读取链路从“串行解压 + 并行 JSON 解析”扩展成“并行解压 + 并行 JSON 解析 + 有序回放”。
- `replica_prefetch_workers` 默认值是 `4`，`replica_prefetch` 未显式传入时默认 `16`，并保留 `--replica-prefetch-workers` 上限为 16。
- `build_btc_diff` 的进度日志改为每 `10000 block` 打印一次，warmup/processing 两段都统一使用同一间隔。
- `--warmup` 从固定必填改为可选参数，支持不传表示禁用、单独传 `--warmup` 使用默认值，也支持 `20k / 2m` 这类后缀。
- 运行时新增更细的 perf 统计，包括 `read_line_lz4`、`json_parse`、`s3_get`、`process_block`、`prune` 等分段。
- 新增独立 LZ4 并发解压 bench，用于验证顺序、完整性和加速比。

结合当前压测结果，`w4/p16` 仍是固定 `workers=4` 时更稳的甜点，`w4/p20` 没有继续带来收益。

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
- `remove/fill` 已禁用（即使订单在 fill 后归零，也只从 live state 删除，不落 parquet）
- trigger 单不进入 live state，不落 parquet
- trigger 的 `modify / batchModify / cancel` 当前直接跳过
- `modify / batchModify` 命中 `Cannot modify canceled or filled order` 时直接跳过
- 未知 `oid` 的 `cancel` fallback 已禁用（不再落 `remove/cancel`）
- 未知旧单的 `modify` cancel fallback 已禁用（不再落 `remove/cancel`）
- live state 引入 24 小时 TTL：订单入 state 时记录创建 `block/time`，仅在 10000 block 边界执行一次过期清理，避免内存无限增长
- live state 过期索引改为“按 `(created_time_ms, oid)` 排序的活跃索引”，不再累计历史入簿事件队列，过期清理与取消/成交删除都会同步回收索引
- 未知 `oid` 的 `modify/cancel` 诊断日志按 10000 block 窗口写入 SQLite
- 进度日志除 `live_orders` 外，额外打印 `known_oids / order_expiry_index / known_oid_expiry_queue / cloid_map_entries`，用于观察内存相关结构体积
- parquet row-group 按 `10000 block` 边界切分（按区块窗口落组，而非按固定行数）
- parquet 文件按 `1000000 block` 边界轮转：跨 1m 边界会先关闭当前文件再打开新文件
- 若配置 `--warmup-state-output-dir`，warmup 期间不会输出快照；warmup 结束块（`output_start-1`）会强制输出一份，之后在每个 `1000000 block` 边界额外输出（msgpack + zstd3）
- 支持 `--warmup-state-file <path>` 在启动时恢复 `live orders / trigger refs / known oids`，用于替代超长 warmup（例如 1.2m）
- `--output-parquet` 现在传入输出目录，产物命名改为 `btc_diff_<actual_start>_<actual_end>.parquet`，重名自动 `_dupN`
- `replica_cmds` 在 `20250720..20260331` 区间使用硬编码日索引直达 S3 key，不再递归扫描目录
- `--height-span` 现在支持 `20k / 2m` 这类后缀，便于快速指定长区间压测范围
- 支持 `--warmup [blocks]`：不传则禁用 warmup，只写 `--warmup` 时使用默认 `1200000`，也支持 `20k / 2m` 这类后缀；warmup 期间不写 parquet、不写 unknown-oid SQLite，仅用于恢复 live state
- 控制台会打印 warmup 开始/结束

## replica_cmds 硬编码索引

- 索引文件：`binaries/src/bin/build_btc_diff_replica_day_index.rs`
- 覆盖范围：`20250720..20260331`（共 `255` 天）
- 探索结果：原桶下该区间存在 `31` 个“同日多 snapshot”重叠日期，索引固定选择该日期的最新 snapshot
- 结构：
  - `REPLICA_SNAPSHOT_DIRS`：snapshot 目录表（`32` 个）
  - `REPLICA_DAY_INDEX`：按天顺序记录 `snapshot_idx + start_chunk`
- 解析方式：
  - 先把目标高度转成 chunk（`<height-1>` 对齐 `10000`）
  - 对 `REPLICA_DAY_INDEX.start_chunk` 做二分，定位所属日期
  - 直接拼接 `replica_cmds/<snapshot>/<yyyymmdd>/<chunk>.lz4`
- 范围外（早于 `20250720` 或晚于 `20260331`）仍回退到原有动态查找逻辑

## unknown oid 调试日志（SQLite）

- 参数：`--unknown-oid-log-sqlite`（默认 `/tmp/build_btc_diff_unknown_oid.sqlite`）
- 对齐规则：窗口按 `...00001.. ...10000`（每 `10000` 块一窗口）
- 输出策略：
  - 能定位到具体引用（如 `oid` 或已映射 `cloid->oid`）时，写入 `unknown_oid_samples`
  - 不能通过 `user+cloid` 映射到 `oid` 时，只计数，不写样本明细
- 单窗口样本上限：`200`，超出计入 `sample_dropped`
- stale prune 仅在窗口边界触发；每次触发写入 `stale_prune_runs` 和 `stale_prune_samples`
- 并发写同一个 sqlite 时，如果 `window_start_block` 主键冲突会跳过该窗口并打印 warn，不抛异常

## parquet 切分与命名

- row-group:
  - 以 `10000 block` 窗口触发 flush
  - 可通过 `parquet_metadata(...)` 看到 `block_number` 的 row-group 统计范围对应窗口边界
- 文件轮转:
  - 以 `1000000 block` 窗口轮转
  - 例如跨越 `952000001` 时，会结束旧文件并命名为 `btc_diff_<start>_<end>.parquet`，再新开下一文件
- 命名冲突规避:
  - 同区间重复运行时，自动输出为 `btc_diff_<start>_<end>_dup1.parquet`, `_dup2.parquet` 等

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

- `new/add`: `missing=129`, `extra=2`
- 同键错量：`20`

归因建议：

- `missing` 主要还是上游返回不足，集中在 `type=default / no-status` 路径，程序拿不到可恢复的新 `oid`
- 少量 reference 中的订单在当前可见输入里没有完整链路
- `extra` 里很小的一部分，可能来自 `reduce_only` 自动缩量或残量计算口径与参考不完全一致
- `20` 条同键错量仍主要是 reduce-only 超过仓位大小的特殊情况，当前数据无法识别，属于已知接受项

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

- `update/fill`: 按 `(block, oid)` 键集 `missing=0`, `extra=0`
- `update/cancel`: `missing=137`, `extra=0`

说明：

- `update/fill` 的键集已和参考对齐
- 但仍有极少数同 block、同 oid 的多次 fill，在行级 multiplicity 上还没完全对齐
- `update/cancel` 当前全缺失，这是实现选择，不是偶发 bug

## remove/fill

### 当前可做的事

- 当某个 live order 已知存在，并且累计 fill 把 `remaining_sz` 扣到 `0`，程序只会从 live state 移除，不写 parquet

### 局限

- `remove/fill` 当前策略是“完全不输出”，因此和任何包含该类型的参考文件都会有系统性差异
- 没有 warmup 时，区间开始前订单仍不可见，但这只影响 live state 内部一致性，不再影响 `remove/fill` 输出（因为该输出已禁用）

### 当前和参考的差异

当前版本下，`remove/fill` 期望始终为 `0` 行；如参考文件存在 `remove/fill`，会全部体现为 `missing`。

## remove/cancel

### 当前可做的事

- 如果 cancel / modify 命中的 old order 当前在 live state 中，程序会输出带 `side / px / orig_sz` 的 rich `remove/cancel`
- 如果无法解析出 live state，当前版本不再输出未知 `oid` 的 fallback `remove/cancel`

### 局限

- 当前没有完整的取消原因恢复能力
  - `reduceOnlyCanceled`
  - `selfTradeCanceled`
  - 以及更多细粒度 reason
- `cancelByCloid` 如果映射链不完整，当前策略是直接不落 `remove/cancel`
- 对于 non-trigger `cancel success`，上游返回成功不等于参考文件一定记录这条 cancel
- 对于 non-trigger `modify type=default`，当前不会再写未知旧单的 fallback cancel，因此可能相对参考增加 `missing`
- 当前只保留约 24 小时左右的 live order 历史，24 小时之前的订单即使发生 cancel 也可能无法记录

### 当前和参考的差异

当前版本特征：

- 未知 oid 的 `cancel` fallback 禁用后，`oid=NULL` 的 `remove/cancel` 应接近 `0`（短区间实测为 `0`）
- 相比参考文件，`remove/cancel` 的缺失会增多，但“伪造 fallback 行”会显著减少
- 参考文件里以下 cancel reason 在当前实现里可能缺失，或者被合并成普通 `canceled`：
  - `reduceOnlyCanceled`
  - `scheduledCancel`
  - `selfTradeCanceled`
  - `marginCanceled`
  - `liquidatedCanceled`
  - `vaultWithdrawalCanceled`

以下分解来自历史样本（开启未知 cancel fallback 的版本），用于解释差异来源：

`missing=1013` 的构成：

- `604` 条：参考是非空 `oid canceled`，当前降级成了 `oid=NULL canceled`
- `397` 条：`reduceOnlyCanceled`
- `10` 条：`selfTradeCanceled`

`extra=80`（历史样本）的构成：

- 全部是 trigger `cancel` 的非空 `oid` 样本
- 这类行当前都有 `status=canceled`，但参考里没有对应 `user+oid+block` 的 `remove/cancel`
- 触发单里 `open->canceled` 的残差已压到 `0`（`has_open=0`）

这组 extra 仍是当前程序和参考文件的主要策略差异之一：

- 当前程序：未知 oid fallback 已禁用，extra 主体收敛到触发链路与历史可见性差异
- 参考文件：并不总是记录这类 cancel（尤其是区间前状态不可见时）

补充（触发单修复后）：

- `waitingForTrigger / waitingForFill` 已纳入 trigger pending 识别
- 可避免的 `open->canceled` trigger 误落库已修复
- 剩余 `extra=80` 更偏向“区间前历史状态不可见”导致的触发单 cancel 差异

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
- 当前程序已禁用未知 oid cancel fallback（相对参考会增加 `remove/cancel missing`，但减少伪造行）
- 当前程序无法恢复所有 `type=default / no-status` 场景下的新 `oid`
- 当前程序已禁用 `remove/fill` 输出（和含该类型的参考会系统性不一致）

## 仍未完全还原的逻辑

以下几点在 `20000` block 对比里仍然可见，说明它们不是单纯噪声，而是当前实现尚未完全还原的行为差异。

### 1. filled-only GTC 不一定真的入簿

当前程序的规则是：

- `Gtc`
- response 只有 `filled`
- 且 `order_sz > filled.totalSz`

就认为仍有剩余数量入簿，因此生成 `new/add`。

但在 `20000` block 对比里仍有 `2` 条 extra `new/add`，说明这个条件还偏宽。至少存在以下情况：

- response 看起来像部分成交
- 但参考文件并没有把剩余部分记成入簿订单

这意味着仅凭 `filled.totalSz` 仍不足以判断“是否真的产生了挂单”。

### 2. 同 block cancelByCloid + batchModify 的顺序/绑定语义未完全还原

已观察到同 block 内出现：

- 先 `cancelByCloid(success)`
- 再 `batchModify` 返回新的 `resting oid`

参考文件会把这条链还原成：

- old oid `remove/cancel`
- new oid `new/add`
- 有时 new oid 在同块又立刻 `remove/cancel`

当前程序在这类场景里仍可能出现：

- old oid 已正确 cancel
- 在旧版本里同块可能多打一条 old oid fallback cancel（当前版本已禁用未知 cancel fallback）
- new oid 的同块 cancel 漏掉

因此这部分 block 内时序和 `cloid -> oid` 重绑定逻辑还未完全还原。

### 3. 同 block、同 oid 的多次 fill 还存在极少数 multiplicity 差异

`update/fill` 的键集已对齐，但 `20000` block 内仍能看到极少数：

- 参考里同 block、同 oid 有两条 `update/fill`
- 当前程序只落一条

这说明当前按 `node_fills_by_block + tid` 的 fill 还原，已足够覆盖键集合，但在少量多笔成交合并/拆分上仍和参考不完全一致。

> [!WARNING]
> 这类差异不要误判成 `node_fills_by_block` 本身少了一笔。当前已核对到的 3 个 `(block, oid)` 样本里，原始 fills 输入和当前 parquet 的 maker-side 记录数一致，参考文件只是保留了更细的 fill split 语义。

### 4. trigger 触发后转为普通单的后续生命周期未还原

当前程序采取 trigger 完全不入 parquet 的保守策略。

代价是，当参考文件把某些 trigger 单触发后，继续按普通单记录其：

- `new/add`
- `remove/cancel`
- 甚至后续 fill/cancel 生命周期

当前程序会整体缺失这一段。

这不是简单的 `oid=NULL` 或 fallback 问题，而是“trigger 触发后何时变成普通 live order”的状态迁移没有建模。

因此，当前程序更适合被理解为：

- 一个仅基于公开 `cmds + fills_by_block` 可见信息重建的 diff 生成器
- 它可以非常接近参考文件，但不能保证在所有 cancel reason、trigger 链路、pre-range live orders、以及 `type=default` 降级返回上完全等价

补充说明：

- `new/add` 的少量 extra，优先怀疑 `reduce_only` 自动缩量或残量计算口径差异
- `new/add` 的 missing，优先怀疑上游返回不足，尤其是 `type=default / no-status`

## 完整 Snapshot 可避免的差异

这里的“完整 snapshot”指区间起点时刻的完整 live state（至少包括非 trigger/trigger 的 `user + oid + cloid + remaining_sz`）。

可明显降低或消除的项：

- `remove/cancel` 中由“区间前 live state 不可见”导致的误差
  - 起点前已存在订单的 `cloid->oid` 可直接命中，减少未知 old order 的漏删/错删
- 触发单 `remove/cancel extra` 中由“区间前已存在 trigger 订单”造成的残差
  - 当前剩余 `extra=80` 基本都属于此类可见性问题

即使有完整 snapshot 仍不能完全消除的项：

- `remove/fill` 全缺失（当前实现已显式禁用该输出）
- `update/cancel` 全缺失（当前实现未输出该类型）
- `type=default` / no-status 返回导致的新 `oid` 不可恢复
- `filled-only GTC` 的入簿判断偏差仍可能存在，但当前更应优先关注 `reduce_only` 自动缩量 / 残量口径差异对 `new/add extra` 的影响
- reduce-only 超仓等场景的 `sz/raw_sz` 偏差（缺少持仓快照与撮合细节）
- 少量 `update/fill` 行级 multiplicity 偏差（同 block、同 oid 多笔 fill 拆分差异）
