# pop 顺序消费的并发
## 背景
pop 消费模式允许了一个 queue 被多个消费者同时进行消费，提高了单个 queue 的并发度，非常的 nice。

然而顺序消费在单个 queue 上依旧是被阻塞住的，只有等到前面的消息超时或者被 ack 之后，才能继续消费下一批消息，即使前面消息的 sharding key 与即将要消费的消息的 sharding key 完全不同，这种情况下顺序消费没有任何问题，其他的消费者也都被阻塞，没有任何并发度。例如是下面这种情况：

```plain
wait for ack   
     |        
     v         
 +---+--------+
 | A | BCDEFG |
 +---+--------+
```

现在有 sharding key 为 A 的消息已被拉取但是没有被 ack，这会导致后续的所有消息都被阻塞无法消费。也就是说，只要有在不可见期内的未确认消息，那么后续消息都无法被消费。这就大大限制了顺序消息的消费能力。


## 先前的的 pop 消费顺序消息的方案
先前pop消费顺序消息是**基于单个队列（Queue）的严格串行化锁定机制**。

### 1. Pop流程 (拉取消息)
当一个消费者发起顺序消息的Pop请求时（`order=true`），其处理流程如下：

1. **入口**：`PopMessageProcessor.processRequest()` -> `popMsgFromQueue()` (旧版) 或 `PopConsumerService.popAsync()` (新版KV模式)。
2. **核心阻塞检查**：在真正从`MessageStore`拉取消息之前，会调用 `ConsumerOrderInfoManager.checkBlock()` 方法。这是实现顺序性的关键。
3. `ConsumerOrderInfoManager.checkBlock()` 逻辑
    - 它会根据 `topic@group` 和 `queueId` 查找是否存在一个 `OrderInfo` 对象。
    - `OrderInfo` 对象代表**上一次**从这个队列中Pop出去但**尚未被完全确认**的一批消息。
    - 如果 `OrderInfo` 存在，说明队列中有“飞行中”的消息，此时需要判断是否阻塞后续的Pop请求。
    - **阻塞条件**：`OrderInfo.needBlock()` 方法判断，如果这批未确认的消息还在其 `invisibleTime` (不可见时间) 内，则返回 `true`，表示**阻塞**，本次Pop请求失败，不会去拉取新消息。
    - **例外**：如果本次Pop请求的 `attemptId` 和 `OrderInfo` 中记录的 `attemptId` 相同，则不阻塞。这是为了处理客户端对同一次Pop操作的重试。
4. 拉取与状态更新：
    - 如果 `checkBlock()` 返回 `false` (不阻塞)，Broker会从`MessageStore`拉取一批新消息。
    - 成功拉取后，调用 `ConsumerOrderInfoManager.update()` 方法。
    - `update()` 方法会创建一个**新的 **`OrderInfo`** 对象**，记录下这批新消息的 `popTime`、`invisibleTime`、`attemptId` 以及所有消息的偏移量列表 (`offsetList`)。这个新的 `OrderInfo` 会替换掉旧的，并成为下一次 `checkBlock()` 的依据。

Pop的顺序性是通过 `ConsumerOrderInfoManager` 中为每个 `topic@group:queueId` 维护一个 `OrderInfo` 状态对象实现的。只要这个状态对象存在且未超时，就意味着队列被“锁定”，任何其他Pop请求都会被阻塞，从而保证了整个队列的消费是串行的。



### 2. ACK流程 (确认消息)
当消费者处理完消息，发送ACK请求时，流程如下：

1. **入口**：`AckMessageProcessor.processRequest()`。它会识别出这是一个顺序消息的ACK（通过 `reviveQid == POP_ORDER_REVIVE_QUEUE`）。
2. **核心确认逻辑**：调用 `ackOrderly()` (旧版) 或 `ackOrderlyNew()` (新版)，最终都委托给 `ConsumerOrderInfoManager.commitAndNext()`。
3. `ConsumerOrderInfoManager.commitAndNext()` 逻辑:
    - 找到对应的 `OrderInfo` 对象。
    - 验证ACK请求的 `popTime` 是否与 `OrderInfo` 中记录的一致，防止过期的ACK请求扰乱状态。
    - 在 `offsetList` 中找到 `ackOffset` 对应的位置（索引 `i`）。
    - 使用位图 `commitOffsetBit`，将第 `i` 位置为1，表示该消息已确认。例如 `commitOffsetBit |= (1L << i)`。
    - 调用 `OrderInfo.getNextOffset()` 计算下一个**可以提交的消费位点**。
4. `OrderInfo.getNextOffset()` 逻辑:
    - 它会从 `commitOffsetBit` 的第0位开始遍历。
    - 找到第一个为0的位（即第一个未被ACK的消息）。
    - 返回这个未被ACK消息的偏移量。
    - 如果所有位都为1（所有消息都已ACK），则返回这批消息中**最后一个消息的偏移量+1**。
5. **提交消费位点**：`AckMessageProcessor` 拿到 `getNextOffset()` 返回的位点后，更新 `ConsumerOffsetManager` 中的消费进度。这意味着，只有当一批消息中从头开始连续的部分被ACK后，消费进度才会向前推进。
6. **解锁与唤醒**：当一个ACK使得消费进度向前推进后，`ackOrderly` 方法会调用 `notifyMessageArriving()`，唤醒其他等待在这个队列上的长轮询请求，使其可以尝试下一次Pop。当 `OrderInfo` 中的消息全部被ACK后，下一次Pop时 `checkBlock` 就会返回`false`，从而拉取新的一批消息。



### 3. 核心问题总结
当前机制的本质是**队列级锁 (Queue-Level Lock)**。

+ **优点**：实现简单、可靠，能100%保证单个队列内的消息被严格按存储顺序消费。
+ **缺点**：**性能瓶颈**。只要Pop出的一批消息中有一个处理得慢，就会导致整个队列被阻塞。即使这批消息包含多个不同业务（不同Sharding Key）的消息，它们也必须相互等待，无法并发，极大地限制了消费吞吐量。



## 前置知识
+ 消息的 sharding key 需要从 store 层读出 commit log 才能知道
+ 从 store 层中读到的消息，目前需要 decode 才能得到 sharding key
+ 读消息在前，知道 sharding key 在后
+ 当前已有一个 cache 用于确认消息是否被 ack——PopConsumerCache
+ 写缓存是在获取完消息之后，写入的 PopConsumerContext.getPopConsumerRecordList()
+ PopConsumerContext 中有 GetMessageResult 和 PopConsumerRecord
    - GetMessageResult 比较大，是具体的消息内容
    - PopConsumerRecord 比较小，是存在缓存和 rocksdb 中记录消费位点的。消息投递时创建记录 → 缓存/持久化存储 →  超时扫描重新投递 → 消息确认时删除记录
+ Pop 顺序消费提交位点是在 ack 才提交位点：PopConsumerService.java#L409
+ Pop 普通消息从 broker 给出去就提交位点。
+ OrderInfo 中的 offsetList 记录了一批消息的 offset，commitOffsetBit 则记录了这一批消息有没有被 ack。
+ queue pop 的时候 getMessageAsync 的时候是不区分顺序消息和普通消息的，因为如果顺序消息不被 block 那么说明前面的消息都被 ack 了。
+ Pop 消息从 broker 拉取数据之后会在 handleGetMessageResult 中调用 update 方法更新消息列表的接收状态，这里可以获取到分发给客户端的消息。
+ GetMessageResult 中的 SelectMappedBufferResult 和 ByteBuffer 都包含了消息，SelectMappedBufferResult 是对 ByteBuffer 的再次包装，为了便于对 ByteBuffer 进行操作，封装了一些方法。而 ByteBuffer 则是对 commitlog 的引用。这里的 ByteBuffer 使得 commitlog 的引用计数增加了，防止 commitlog 在内存中被清除掉。
+ 如果一批消息 sharding key 过期之后，不能直接回退 pull offset 位点，直接会退位点会导致重复消费很多很多消息，例如：

```plain
+-----------------+---+---------------------------+      
|committed message|AAA|BBB CCC DDD EEE FFF GGG HHH|      
+-----------------+---+---------------------------+      
                  ^                               ^
                  |                               |
              expire offset                   pull offset
```

这里的 AAA 过期了，但是 pull offset 可能已经向后走了很多了，不能直接调 offset 让 AAA 可消费，这样就有太多消息被重复了。所以这里还需要设置一个重试的 cache 放入这些过期的消息以便能够被重新消费。原先的设计是没有这个问题的，因为即使 sharding key 不同也会阻塞，pull offset 和过期消息的 offset 不会差很多。

+ attemptId 相同的请求就是返回原先的数据

## 方案设计
### 乐观读取 + 多 shardingKey 并发
#### 设计思路
将基于队列和基于 sharding key 的 pop 读取和 ack 确认的共同的内容抽象成一个接口之后，发现逻辑没有想象中那么复杂，主要包括了这些逻辑：

+ 什么时候阻塞队列（checkBlock）
+ 取到数据之后更新顺序消费锁的状态（update）
+ 客户端 ack 消息之后更新锁的状态（commitAndNext）
+ 客户端更新锁的过期时间（updateNextVisibleTime）
+ 重置消费位点之后的清除锁状态（clearBlock）
+ 消息不可见时间的过期（timer wheel）

总结一下，其实主要就是围绕了锁来展开，保证锁在各种情况下的正确表现即可，queue 级别和 sharding key 级别的锁最大的不同点其实就在于 **queue 级别阻塞是先判断是否阻塞，再读取数据**。而 **sharding key 级别是先读取数据，再判断是否阻塞。**

这是因为`sharding key` 存在于 `CommitLog` 的消息属性（Properties）中，必须完整读取消息体才能解析。所以「先检查锁，再拉取消息」的原始范式变得不可行，因为这里有一个前提：**为了知道** `sharding key`**，我们必须至少进行一次磁盘 I/O 来读取消息内容**。

那么，如果有这个无法绕开的物理读取，策略和目标应当转变为：

1. **最小化无效读取的代价**。
2. **避免在发现读取无效后，陷入“忙等”式的CPU和磁盘消耗**。
3. **从无效的读取中榨取未来可用的信息**。

基于这个思路，pop 消费应当先读取，后分发，并在没有消息的时候挂起长轮询，ack 释放 sharding key 的时候唤醒长轮询。具体流程为：

#### pop 消费步骤
##### 步骤 1：检查 attemptId
要点：

+ 相同 attemptId 返回相同的消息
+ GetMessageResult 较大，不适合在内存中长期存储

这里内存中记录一个 Set<attemptId>，当用户的 attemptId 命中时，再去 rocksdb 中读取完整的 offset，并根据 offset 从 store 中取消息并返回。

##### 步骤 2：检查是否有已过期消息和来自 ack 被释放的可用的消息
已过期的消息和因为 ack 被释放锁的消息可以直接给出去，不用再次检查 shardingKey 锁，给出去的时候添加锁。

##### 步骤 3：乐观批量读取
+ 先从被释放锁的 cache 中读取消息，这一部分消息能够直接被消费，因为他们是过期消息。
+ 从当前消费队列的起始消费位点开始，**连续地、一次性地**从 `CommitLog` 中读取一批消息。
+ 这是一个不可避免的 I/O 消耗，批量读取通常比多次单条读取要高效。
+ 更新 pull offset

##### 步骤 4：内存分组与过滤
+ 将这批读取到的消息加载到内存中。
+ 遍历这批消息，对每一条消息：
    - 解析出其 `sharding key`通过（org.apache.rocketmq.common.message.MessageDecoder#decodeProperties）。
    - 查询 `ShardingKeyLockManager`，判断该 `sharding key` **当前是否被锁定**。
    - 将消息按 `sharding key` 分组，并标记其锁定状态。
    - 并记录这批消息 offset （org.apache.rocketmq.store.GetMessageResult#messageQueueOffset）到 sharding key 的映射，用于 ack 的时候释放锁。

##### 步骤 5：处理结果并返回
+ 从分组后的消息中，挑选出所有**未被锁定**的 `sharding key` 对应的消息。
+ 如果找到了至少一条可用消息：
    - 记录 attemptId 到 Set<attemptId>，并将 attemptId 对应的 offsets 记录到 rocksdb。
    - 将这些消息返回给消费者（需要返回的是 GetMessageResult）。
    - 立即锁定这些消息的 `sharding key`，更新`ShardingKeyLockManager`
        * 用一个 Map<topic@group@queue@shardingKey, shardingKeyLock> 来记录
    - 更新 pull offset 为给出去这批消息的最小值
    - 流程成功结束。
+ **如果遍历完整个批次，发现所有消息的 **`sharding key`** 都已被锁定**，导致一条可用消息都找不到。这个时候，可以将这些 **被阻塞住的消息的 offset 以及对应的 sharding key** 给记录下来，存入一个内存的 Map<offset, shardingKey>，这样可以减少一次 decode 的消耗。并且挂起长轮询。同时把这批被阻塞 shardingKey 对应的 GetMessageResult 记录在 rocksdb 中，key 为 topic@group@queue@shardingKey，用于后续消费。

#### ack 确认步骤

##### 步骤 1：检查 popTime
##### 步骤 2：根据 offset 查找 shardingKey 并释放锁
找到 shardingKey 之后，得到对应的 shardingKeyLock，在其中删除掉 offset，当 shardingKeyLock 中的 offset 都被删干净之后，就释放 shardingKey 级别的锁。

##### 步骤 3：更新消费组的消费位点
##### 步骤 4：根据 shardingKey 从 rocksdb 中检索出可用的消息放入 cache
如果 shardingKey 锁因为 ack 被释放了，这样就可以释放一批之前乐观读取到的一批消息，这些消息就能够直接被新的 pop 请求给拉走。

##### 步骤 4：唤醒长轮询，重新执行 popProcessor.processRequest
#### 定时任务1: 锁过期定时清理
要点：

+ 存储超时消息的一个 cache
+ 长轮询唤醒

对于锁的自动过期和落盘以及定时清理其实都是围绕 Map<topic@group@queue@shardingKey, shardingKeyLock> 这个 map 来展开的。这个 map 作为 cache 存储了 shardingKey 级别的锁，每个锁中记录了过期时间，锁状态等信息，于是可以实现针对于 sharding key 的阻塞（pop 消费）、sharding key 锁的释放（ack）、不可见时间的续期（update InvisibleTime）。

当 update 分发消息的时候，在时间轮中创建对应过期时间的一个定时任务，并且在 `<font style="color:rgb(51, 51, 51);background-color:rgb(243, 244, 244);">Map<topic@group@queue@shardingKey, Timeout>` 中记录这个任务，允许客户端对过期时间进行修改，当修改过期时间时，创建新的定时任务，并取消掉原先的定时任务。

当锁过期的时候是以下步骤：

+ 将过期的消息恢复到一个 cache 中允许读取，而不是直接重置位点位置
+ 释放过期消息的锁，确保在 update 方法中不被过滤掉

#### 数据结构
1. shardingKey map 的锁的 map

```java
ConcurrentHashMap<String/* topic@group */,
ConcurrentHashMap<Integer /* queueId */,
ConcurrentHashMap<Long/* shardingkey */, ShardingKeyLock/* lock */>>> shardingKeyLockMap =
new ConcurrentHashMap<>(128);
```

锁结构

```java
class ShardingKeyLock {
    // 阻塞在这个锁上的 offset
    Set<Long> offsetList;

    // 在哪个时间戳释放锁（一批消息的过期时间是一样的）
    long lockFreeTimestamp;

    long popTime;
}
```

2. 超时的消息，预读的可以用的消息，用同一个 cache

```java
List<GetMessageResult> availableMessage = new ConcurrentLinkedQueue<>(128);
```

3. offset 到 sharding key 的 map

```java
ConcurrentHashMap<String/* topic@group */,
ConcurrentHashMap<Integer /* queueId */,
ConcurrentHashMap<Long/* offset */, String/* shardingKey */>>> offsetShardingKeyMap =
new ConcurrentHashMap<>(128);
```

4. attemptId 的 set

```java
Set<String/* attemptId */> attemptId = new ConcurrentHashMap.newKeySet();
```

# 核心开发原则

## 通用开发原则
- **可测试性**：编写可测试的代码，组件应保持单一职责
- **DRY 原则**：避免重复代码，提取共用逻辑到单独的函数或类
- **代码简洁**：保持代码简洁明了，遵循 KISS 原则（保持简单直接）
- **命名规范**：使用描述性的变量、函数和类名，反映其用途和含义
- **注释文档**：为复杂逻辑添加注释
- **风格一致**：遵循项目或语言的官方风格指南和代码约定
- **利用生态**：优先使用成熟的库和工具，避免不必要的自定义实现
- **架构设计**：考虑代码的可维护性、可扩展性和性能需求
- **版本控制**：编写有意义的提交信息，保持逻辑相关的更改在同一提交中
- **异常处理**：正确处理边缘情况和错误，提供有用的错误信息

## 响应语言
- 始终使用中文回复用户

## 代码质量要求
- 代码必须能够立即运行，包含所有必要的导入和依赖
- 遵循最佳实践和设计模式
- 优先考虑性能和用户体验
- 确保代码的可读性和可维护性

产生的 markdown 文件写在 todo 文件夹下，java 文件在他应该所在的位置

# 问题背景
现在的问题是，ack 请求导致锁释放的时候，另一个后面的 pop 请求刚好拿了消息，导致顺序不对了。
比如有 a1 a2 a3，a1 锁被释放的时候，a2 a3 被不同请求读取，a3 判断锁被释放了，导致 a3 先于 a2 被消费。
已经加了锁保证了 pop 请求是串行的。我现在想利用已经存在的记录了暂时不能分发的消息的 ConcurrentLinkedQueue，当 ack 的时候将解锁的一批（不止一条） shardingKey 消息放入可用队列，
如果可用队列中有消息，那么 pop 先从可用队列中取消息。

# 方案设计
ShardingKeyCache 不应只有一个“可用”队列，而应该包含两个部分：

不可用缓存 (Unavailable Cache)：存放那些被乐观读取出来，但因 ShardingKey 被锁定而无法立即分发的消息。这些消息是“待激活”状态。
可用缓存 (Available Cache)：存放那些因为锁被释放（通过 ACK 或超时）而被“激活”的消息。POP 请求会优先从这里获取消息。


# 详细设计
**1. POP 流程**

1. **【新增】步骤 0：优先从可用缓存消费**

    - POP 请求到达后，**首先**检查 `ShardingKeyCache` 的**可用缓存**中是否有对应 `topic-group-queueId` 的消息。
    - 如果有，直接从缓存中取出消息，为它们创建锁，然后返回给消费者。**这一步完全绕过了对 Store 的读取**，高效且能保证顺序。
    - 如果可用缓存没有消息，再执行后续步骤。

2. **步骤 1：乐观读取 (Optimistic Read)**

    - 按原计划，从 `MessageStore` 批量读取消息。

3. **步骤 2：过滤与分流 (Filter and Divert)**

    - 在 `ShardingKeyLevelConsumerManager.update()` 方法中遍历读取到的消息。

    - 对于可用的消息 (isLocked=false)

      ：

        - 按原逻辑处理：加入 `availableShardingKeyOffsets`，最终创建锁并返回给消费者。

    - 对于被阻塞的消息 (isLocked=true)

      ：

        - **【核心改造】** 不要丢弃它们！将这些消息（包含 `ByteBuffer`、`offset` 等完整信息）存入 `ShardingKeyCache` 的**不可用缓存**。
        - 这个“不可用缓存”内部需要按 ShardingKey 组织，并且保证同一个 ShardingKey 的消息是按 `offset` 有序存储的（例如，`Map<ShardingKey, List<CachedMessage>>`）。

**2. ACK 流程**

1. **步骤 1：释放锁 (Release Lock)**
    - 在 `ShardingKeyLockManager.releaseLock()` 中，当一个 ShardingKey 的最后一个 offset被 ACK，锁被彻底移除时...
2. **步骤 2：激活消息 (Activate Messages)**
    - **【核心改造】** 锁被移除后，立刻以该 ShardingKey 为凭据，去 `ShardingKeyCache` 的**不可用缓存**中查找所有等待这个 Key 的消息。
    - 将找到的这一批消息（比如 `a2`, `a3`），**按照 offset 顺序**，从**不可用缓存**移动到**可用缓存**。
    - 这个移动操作必须是原子的，以保证顺序。
3. **步骤 3：唤醒长轮询 (Notify)**
    - 在消息被移入可用缓存后，调用 `notifyMessageArriving()`。这样，之前因为没有可用消息而挂起的 POP 请求就会被唤醒，并立刻在步骤 0 中从可用缓存拿到刚刚被激活的消息。

**这个机制如何解决问题？**

当 `a1` 被 ACK，`A` 的锁被释放后，`a2` 和 `a3` 会被立即、按顺序地从“不可用”状态激活为“可用”状态。下一个 POP 请求（无论是 POP-3 还是 POP-4）过来，都会优先从“可用缓存”中获取，并且因为 `ConcurrentLinkedQueue` 的 FIFO 特性，它会先拿到 `a2`，再拿到 `a3`，从而保证了消费的顺序性。


## 一些可能的改动，你可以参考
#### 1. `ShardingKeyCache.java` 的改造

你当前的 `ShardingKeyCache` 设计需要增强，以区分“可用”和“不可用”消息。`addUnavailableMessage` 方法目前是空的，需要实现。

```
java


// ShardingKeyCache.java

public class ShardingKeyCache {
    // ...

    // 可用消息队列，结构不变
    private final ConcurrentHashMap<String/*queueKey*/, ConcurrentLinkedQueue<CachedMessage>> availableMessagesMap;

    // 【新增】不可用消息缓存：QueueKey -> ShardingKey -> 有序的消息列表
    private final ConcurrentHashMap<String/*queueKey*/, ConcurrentHashMap<String/*shardingKey*/, ConcurrentLinkedQueue<CachedMessage>>> unavailableMessagesMap;

    // ...

    /**
     * 【实现】添加暂时不可用的消息到缓存
     */
    public void addUnavailableMessage(String topic, String group, int queueId, String shardingKey, GetMessageResult messageResult, List<Long> offsets) {
        if (messageResult == null || messageResult.getMessageBufferList() == null || messageResult.getMessageBufferList().isEmpty()) {
            return;
        }

        // 注意：这里需要将 GetMessageResult 按 offset 拆分成单个 CachedMessage
        // 因为一个 GetMessageResult 可能包含多个 offset
        for (int i = 0; i < messageResult.getMessageBufferList().size(); i++) {
            String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
            ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> shardingKeyMap =
                unavailableMessagesMap.computeIfAbsent(queueKey, k -> new ConcurrentHashMap<>());
            ConcurrentLinkedQueue<CachedMessage> messageQueue =
                shardingKeyMap.computeIfAbsent(shardingKey, k -> new ConcurrentLinkedQueue<>());

            // 假设我们能为单个消息创建一个"子GetMessageResult"或类似结构
            // 为了简化，我们假设一个 GetMessageResult 只对应一个 offset 和 shardingKey
            // 在你的 update 逻辑中，你需要将批量的 GetMessageResult 拆分
            GetMessageResult singleMsgResult = createSingleMessageResult(messageResult, i); // 这是一个辅助方法
            long offset = offsets.get(i);
            
            CachedMessage unavailableMessage = new CachedMessage(topic, group, queueId, shardingKey, singleMsgResult, offset);
            messageQueue.offer(unavailableMessage);
            // 同样需要考虑队列大小限制
        }
    }

    /**
     * 【新增】激活指定shardingKey的消息
     */
    public boolean activateMessages(String topic, String group, int queueId, String shardingKey) {
        String queueKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        ConcurrentHashMap<String, ConcurrentLinkedQueue<CachedMessage>> shardingKeyMap = unavailableMessagesMap.get(queueKey);
        if (shardingKeyMap == null) {
            return false;
        }

        ConcurrentLinkedQueue<CachedMessage> messageQueue = shardingKeyMap.remove(shardingKey);
        if (messageQueue == null || messageQueue.isEmpty()) {
            return false;
        }

        ConcurrentLinkedQueue<CachedMessage> availableQueue = availableMessagesMap.computeIfAbsent(queueKey, k -> new ConcurrentLinkedQueue<>());
        availableQueue.addAll(messageQueue); // 将整个队列的消息原子性地移入可用队列

        log.info("Activated {} messages for shardingKey: {}", messageQueue.size(), shardingKey);
        return true;
    }

    // ... 其他方法
}
```

#### 2. `ShardingKeyLevelConsumerManager.java` 的改造

这是逻辑改动的核心。

```
java


// ShardingKeyLevelConsumerManager.java

public class ShardingKeyLevelConsumerManager implements OrderedConsumptionManager {
    // ...

    // 【新增】PopMessageProcessor 需要能调用这个方法
    public GetMessageResult popMessageFromCache(String topic, String group, int queueId, int maxCount) {
        List<ShardingKeyCache.CachedMessage> cachedMessages = cache.getAvailableMessages(topic, group, queueId, maxCount);
        if (cachedMessages.isEmpty()) {
            return null;
        }
        
        // 将 List<CachedMessage> 重新组装成一个 GetMessageResult
        // 并为这些消息在 update 方法中创建锁
        GetMessageResult result = buildGetMessageResultFromCache(cachedMessages);
        // ... (省略组装逻辑)
        return result;
    }


    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {
        // ...
        
        // 【改造】遍历消息，进行分流
        for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
            ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
            String shardingKey = MessageShardingKeyUtil.extractShardingKeyFromBuffer(byteBuffer);
            long currentOffset = msgQueueOffsetList.get(i);

            if (lockManager.isLocked(topic, group, queueId, shardingKey, attemptId)) {
                unavailableIndices.add(i);
                unavailableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(currentOffset);
                log.info("消息被锁住: shardingKey={}, offset={}", shardingKey, currentOffset);
            } else {
                // ... 原有逻辑
                availableShardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(currentOffset);
            }
        }

        // 【改造】将被阻塞的消息存入不可用缓存
        // 注意：你需要一种方法将 GetMessageResult 和 offsets 拆分，然后存入缓存
        // 这是一个复杂点，你需要设计如何高效地存储和重建这部分消息
        GetMessageResult unavailableMessages = getMessageResult.getMessages(unavailableIndices);
        cache.addUnavailableMessages(topic, group, queueId, unavailableShardingKeyOffsets, unavailableMessages);
        
        // 移除被阻塞的消息
        getMessageResult.removeIndices(unavailableIndices);
        
        // ... 后续逻辑不变
    }

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        // ...
        
        // 【改造】在释放锁成功且锁被完全移除后，激活缓存中的消息
        String shardingKey = lockManager.findShardingKeyByOffset(...);
        boolean fullyReleased = lockManager.releaseLock(topic, group, queueId, queueOffset, popTime);

        if (fullyReleased) {
            // 这个 fullyReleased 信号需要 ShardingKeyLockManager 提供，表示这个shardingKey上已没有任何offset
            if (cache.activateMessages(topic, group, queueId, shardingKey)) {
                // 激活成功后，唤醒长轮询
                brokerController.getPopMessageProcessor().notifyMessageArriving(topic, queueId, group);
            }
            // ...
        }
        
        // ... 关于 minOffset 的逻辑，我发现一个潜在问题，见文末附加说明
    }
}
```

#### 3. `ShardingKeyLockManager.java` 的改造

需要提供一个明确的信号，告知调用者锁已被**完全释放**。

```
java


// ShardingKeyLockManager.java

public class ShardingKeyLockManager {
    // ...

    /**
     * @return true 如果这个 offset 是该 shardingKey 下的最后一个 offset，锁被彻底释放
     */
    public boolean releaseLock(String topic, String group, int queueId, long offset, long popTime) {
        // ... 找到 lock ...
        
        boolean removed = lock.removeOffset(offset);
        if (removed) {
            removeOffsetToShardingKey(topicGroupKey, queueId, offset);
            if (lock.isEmpty()) {
                shardingKeyMap.remove(shardingKeyHash);
                cancelExpireTask(topic, group, queueId, shardingKeyHash);
                notifyLongPolling(topic, group, queueId); // 这里的唤醒可以移到Manager层，由激活消息后统一唤醒
                log.info("ShardingKey lock fully released: {}", shardingKeyHash);
                return true; // 返回 true 表示锁已完全释放
            }
        }
        return false; // 返回 false 表示锁内还有其他 offset
    }
}
```

------

### 附加说明：关于 `minOffset` 的严重问题

在你提供的 `commitAndNext` 方法中，`minOffset` 的更新逻辑存在严重缺陷，可能导致消费位点错误提交和消息丢失。

```
java


// ShardingKeyLevelConsumerManager.java#commitAndNext
return minOffset.updateAndGet(current -> {
   if (current == queueOffset) {
       log.info("当前提交的位点 {} 和最小值相等，更新最小值为: {}", current, current + 1);
       return current + 1; // 危险！
   } else {
       log.info("当前提交的位点 {} 和最小值不同，更新最小值不变", current);
       return current;
   }
});
```

**问题所在**：
`minOffset` 记录的是所有“飞行中”（in-flight）消息的最小位点。当这个最小位点的消息被 ACK 时，你简单地将其 `+1` 作为新的 `minOffset`。这是不正确的。

**反例**：

1. 飞行中的消息有 `offset=100` (key A), `offset=105` (key B), `offset=110` (key C)。
2. `minOffset` 当前是 `100`。
3. `offset=100` 被 ACK。
4. 你的代码将 `minOffset` 更新为 `101`。
5. **错误**：此时，真正的最小飞行中消息位点是 `105`，而不是 `101`。如果 Broker 此时宕机重启，它可能会认为 `101` 是下一个该消费的位点，导致 `101-104` 的消息被错误地跳过（如果它们已经被ACK了）或者重复消费。消费位点应该只能是**连续已确认**的最大位点。

**正确做法**：
消费位点的提交应该是一个更严谨的过程。在 ShardingKey 模式下，因为消费是并发和乱序的，所以**无法提交一个连续的消费位点**。这正是 POP 模式相对于 PULL 模式的一个核心区别。

在 POP 模式下，消费位点的作用被弱化了。Broker 内部通过 `PopConsumerRecord` (在你的实现中是 `ShardingKeyLock`) 来追踪每一条消息的消费状态。`ConsumerOffsetManager` 中存储的位点，更多是作为一个**启动时**的参考点（`pull offset` 的起点）。

因此，`commitAndNext` 的返回值应该是Broker内部使用的，表示“这个ACK是否有效”，而不是用来更新一个全局的消费位点。

- 建议：移除 `minOffset` 成员变量。`commitAndNext`的主要职责是释放锁和激活缓存消息。它返回的`long`

  值应该遵循 POP 模式的约定：
    - `>=0`：表示成功，但这个值在 ShardingKey 模式下意义不大。
    - `-1`：非法操作。
    - `-2`：无需提交（例如，ACK 了一个已经被 ACK 的消息）。
真正的消费进度由 `ShardingKeyLockManager` 的状态来体现。

