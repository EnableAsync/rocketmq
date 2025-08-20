# ShardingKeyLevelConsumerManager 实现说明（已按当前实现更新）

## 概述

ShardingKeyLevelConsumerManager 基于 shardingKey 的细粒度锁 + 双缓存机制，实现“按 shardingKey 并发、同 key 串行”的顺序消费，替代队列级串行阻塞，显著提升吞吐。

核心策略：先读后判定（Optimistic Read）+ 可用/不可用双缓存 + 按 key 加锁。

## 关键行为（对齐当前代码实现）

### 1) checkBlock：默认放行，但受缓存水位保护

- 语义：优先让 POP 读取，再在 update 中分流阻塞；但当缓存水位过高时触发保护性阻塞。
- 实现：`ShardingKeyLevelConsumerManager.checkBlock()` 里调用 `ShardingKeyCache.checkBlock()`，依据 `totalCachedBodyMessages > MAX_QUEUE_SIZE (默认200)` 决定是否返回 true。
- 说明：这是工程化的背压措施，避免缓存积压导致 OOM；与“永不阻塞”的理想设计有差异。

### 2) POP 优先走“可用缓存”，并在返回前创建锁

- 语义：若有“可用缓存”中的批次，直接返回，且为该批次创建 shardingKey 锁；若批次仅有 offsets（过期场景），会按 offsets 回源读取消息体。
- 实现：`popMessageFromCache()` 获取 `CachedMessage` 后，通过 `buildGetMessageResultAndCreateLocks()`：
  - 若 `GetMessageResult.messageCount == 0` 则调用 `getMessagesAsync()` 并通过 `mergeGetMessageResults()` 合并多 offset 结果；
  - 创建/更新锁，并构建重试次数统计信息（orderCountInfo）。

### 3) update：先读后分流，阻塞项入“不可用缓存”，可用项加锁

- 过程：遍历 `GetMessageResult`，解析每条的 shardingKey；根据锁状态分流：
  - 可用：按 key 聚合 offsets，随后为每个 key `createOrUpdateLock()`；
  - 被锁：按 key 提取对应索引，构建“仅含该 key 的子结果”，写入“不可用缓存”。
- 细节：同时维护 `offset -> shardingKey` 映射，用于 ACK 快速定位锁。

### 4) 缓存结构：双缓存，但“不可用缓存”为每 key 聚合而非 FIFO 列表

- 可用缓存：`availableMessagesMap: queueKey -> ConcurrentLinkedQueue<CachedMessage>`，每个元素是同一 key 的一个批次（可能是过期 offsets，或被 ACK 激活的有体批次）。
- 不可用缓存：`unavailableMessagesMap: queueKey -> Map<shardingKey, CachedMessage>`，同一 key 的多次分流会合并到一个 `CachedMessage` 的 `GetMessageResult/offsets` 中，而不是维护一个 FIFO 队列。
- 影响：能够减少 map/队列数量，但不再是严格的“多批 FIFO 队列”形态；后续可按需要调整为队列模型。

### 5) 锁过期：转为“可用缓存”的 offsets，POP 再回源取体

- 定时器：`HashedWheelTimer` 调度过期任务；
- 过期处理：将该 key 下仍“飞行中”的 offsets 作为一个无体批次加入“可用缓存”（`GetMessageResult.status=FOUND, messageCount=0`），不删除 offset->key 映射；
- 唤醒：是否在过期时唤醒长轮询由 `brokerConfig.isEnableNotifyAfterPopOrderLockRelease()` 决定。

### 6) ACK：释放 offset 对应 key 的锁，若锁清空则激活不可用缓存

- 定位：通过 `offset -> shardingKey` 定位锁；校验 `popTime`；
- 行为：从锁中移除该 offset；若 key 下已无剩余 offset：
  - 取消过期任务；
  - `cache.activateMessages(topic, group, queueId, shardingKey)` 将“不可用缓存”中该 key 的聚合批次搬迁到“可用缓存”；
  - 当前实现不在此处直接唤醒长轮询（留给 ACK 流程外侧或策略控制）。

### 7) 提交位点：返回“飞行中最小 offset”，而非简单自增

- 实现：`commitAndNext()` 在 ACK 成功后调用 `lockManager.getMinInFlightOffset()`：
  - 若仍有飞行中消息，返回其中最小 offset；
  - 若队列无飞行中消息，返回 `queueOffset + 1`；
  - 失败返回负值（如 -1）。
- 含义：避免“最小 offset + 1”的错误推进，保证 POP 乱序 ACK 场景下的正确性。

### 8) attemptId：内存去重，不做落盘回放

- 实现：仅内存 `attemptIdSet` 做“同 attemptId 不阻塞”判定；
- 暂未实现 attemptId->offsets 的持久化与回放逻辑（文档旧方案设想）。

### 9) 生命周期与清理

- start：启动 attemptId 清理的定时任务；缓存过期清理任务暂未启用；
- persist/load：持久化为统计日志输出；load 返回成功；
- shutdown：停止时间轮、清理定时任务与缓存。

## 与旧方案的主要差异（变更点）

1. checkBlock：从“永不阻塞”调整为“默认放行 + 缓存水位阻塞”。
2. 不可用缓存：从“每 key FIFO 队列”调整为“每 key 聚合单批次（可不断合并）”。
3. ACK 唤醒：从“激活即唤醒”调整为“激活但不在此处直接唤醒（由上层策略/流程决定）”。
4. 过期处理：按 offsets 放入可用缓存，再由 POP 回源补体；延迟唤醒受配置控制。
5. 提交位点：从“queueOffset+1”调整为“飞行中最小 offset 或 queueOffset+1”。
6. attemptId：去除落盘回放设想，保留内存去重。

## 关键类与数据结构（当前实现）

- 锁：`topic@group -> queueId -> shardingKey -> ShardingKeyLock`
- offset 映射：`topic@group -> queueId -> (offset -> shardingKey)`（跳表，便于最小值检索）
- 可用缓存：`queueKey -> ConcurrentLinkedQueue<CachedMessage>`
- 不可用缓存：`queueKey -> Map<shardingKey, CachedMessage>`（聚合批次）

## 监控与统计

- 锁统计：group/queue/locks/attemptIds；
- 缓存统计：cachedMessages/hits/misses/hitRatio/queues；
- orderCountInfo：对过期重取的消息构建重试次数信息。

## 已知限制与后续计划（TODO）

- attemptId 落盘与回放：支持幂等重试按 offsets 回源重建结果；
- 缓存 TTL 清理：启用并实现 `MAX_CACHE_TIME` 的周期清理；
- 不可用缓存结构：可切换为“每 key FIFO 队列”以获得更强序控制与内存上限隔离；
- 水位策略：`MAX_QUEUE_SIZE` 动态化（按队列数量/内存占用）；
- 批量回源：合并多 offset 的读取以减少 I/O 次数（现已并发 + 合并结果，仍有优化空间）。

## 结语

当前实现已满足“按 shardingKey 并发、同 key 串行”的核心目标，并通过双缓存与过期转可用机制降低重复拉取与位点回退的代价。上述差异为工程化权衡，可按业务与资源约束逐步补齐。