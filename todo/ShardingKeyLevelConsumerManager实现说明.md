# ShardingKeyLevelConsumerManager 实现说明

## 概述

ShardingKeyLevelConsumerManager 是基于 sharding key 的并发顺序消费管理器，用于替代传统的队列级别阻塞机制，提升顺序消费的并发度。

## 核心设计思路

### 乐观读取 + 多 shardingKey 并发

传统的队列级别阻塞机制是**先判断是否阻塞，再读取数据**，而 sharding key 级别则是**先读取数据，再判断是否阻塞**。

这种设计的原因是：
- `sharding key` 存在于 `CommitLog` 的消息属性中，必须完整读取消息体才能解析
- 为了知道 `sharding key`，我们必须至少进行一次磁盘 I/O 来读取消息内容
- 采用乐观读取策略，最小化无效读取的代价，避免陷入"忙等"式的CPU和磁盘消耗

### 核心优势

1. **并发性提升**：不同 sharding key 的消息可以并发消费，不会相互阻塞
2. **资源利用率**：避免了因单个慢消息导致整个队列被阻塞的问题
3. **向后兼容**：完全兼容现有的 OrderedConsumptionManager 接口

## 架构设计

### 核心组件

1. **MessageShardingKeyUtil**：消息 sharding key 提取和处理工具类
2. **ShardingKeyLock**：单个 sharding key 的锁结构
3. **ShardingKeyLockManager**：锁的创建、检查、释放和过期处理管理器
4. **ShardingKeyCache**：可用消息的缓存管理器

### 核心数据结构

```java
// 1. shardingKey 锁的三级 Map
ConcurrentHashMap<String/* topic@group */,
    ConcurrentHashMap<Integer/* queueId */,
        ConcurrentHashMap<Long/* shardingKeyHash */, ShardingKeyLock>>> shardingKeyLockMap;

// 2. offset 到 sharding key 的映射
ConcurrentHashMap<String/* topic@group */,
    ConcurrentHashMap<Integer/* queueId */,
        ConcurrentHashMap<Long/* offset */, Long/* shardingKeyHash */>>> offsetToShardingKeyMap;

// 3. attemptId 集合，用于检查重复请求
Set<String> attemptIdSet;

// 4. 可用消息缓存
ConcurrentHashMap<String/* topic@group@queueId */, 
    ConcurrentLinkedQueue<AvailableMessage>> availableMessagesMap;
```

## 实现细节

### 核心方法实现

#### 1. checkBlock() 方法

```java
@Override
public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
    // 对于 sharding key 级别，总是返回 false，让消息先读取出来
    // 真正的阻塞逻辑在 update 方法中通过分析 sharding key 来实现
    return false;
}
```

**设计决策**：
- 采用乐观读取策略，总是允许消息先读取
- 阻塞判断延迟到 update 方法中进行

#### 2. update() 方法

```java
@Override
public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
                  long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
                  StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {
    // 1. 从GetMessageResult中提取sharding key信息
    MessageShardingKeyUtil.MessageShardingInfo shardingInfo = 
        MessageShardingKeyUtil.extractShardingKeyInfo(getMessageResult);
    
    // 2. 按sharding key分组消息并创建锁
    for (Map.Entry<String, List<MessageShardingKeyUtil.MessageInfo>> entry : 
         shardingInfo.getShardingKeyGroups().entrySet()) {
        String shardingKey = entry.getKey();
        List<MessageShardingKeyUtil.MessageInfo> messages = entry.getValue();
        
        // 创建或更新锁
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey,
            popTime, invisibleTime, attemptId, offsets);
        
        // 构建顺序信息
        buildOrderInfo(orderInfoBuilder, topic, group, queueId, shardingKey, messages);
    }
}
```

**设计决策**：
- 从 GetMessageResult 中解析消息的 sharding key
- 按 sharding key 分组消息，为每个 sharding key 创建独立的锁
- 支持多个 sharding key 并发处理

#### 3. commitAndNext() 方法

```java
@Override
public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
    // 根据 offset 释放对应的 sharding key 锁
    boolean released = lockManager.releaseLock(topic, group, queueId, queueOffset, popTime);
    
    if (released) {
        return queueOffset + 1;  // 返回下一个偏移量
    } else {
        return -2;  // 释放失败，无需提交
    }
}
```

**设计决策**：
- 通过 offset 查找对应的 sharding key
- 释放特定 sharding key 的锁，而不是整个队列
- 支持细粒度的锁释放

### 锁管理机制

#### 锁的创建与更新

1. **三级 Map 结构**：`topic@group -> queueId -> shardingKeyHash -> ShardingKeyLock`
2. **时间轮定时器**：使用 HashedWheelTimer 处理锁过期
3. **Offset 映射**：维护 offset 到 sharding key 的映射，支持 ACK 时快速查找

#### 锁的过期处理

```java
// 过期处理流程
private void handleExpiredLock(String topic, String group, int queueId, long shardingKeyHash) {
    // 1. 移除过期锁
    ShardingKeyLock lock = shardingKeyMap.remove(shardingKeyHash);
    
    // 2. 将过期的 sharding key 添加到可用缓存
    Set<Long> expiredSet = expiredShardingKeyCache.computeIfAbsent(
        cacheKey, k -> ConcurrentHashMap.newKeySet());
    expiredSet.add(shardingKeyHash);
    
    // 3. 唤醒长轮询
    notifyLongPolling(topic, group, queueId);
}
```

**设计决策**：
- 过期消息不直接回退位点，而是放入可用缓存
- 避免因位点回退导致的大量重复消费
- 支持过期消息的重新投递

### 缓存管理

#### 可用消息缓存

```java
public class AvailableMessage {
    private final String topic;
    private final String group;
    private final int queueId;
    private final String shardingKey;
    private final GetMessageResult messageResult;
    private final long createTime;
}
```

**功能特性**：
- 支持按队列和 sharding key 进行消息检索
- 自动过期清理机制
- 缓存大小限制，防止内存溢出

#### 缓存统计

```java
public class CacheStatistics {
    private final long totalCachedMessages;
    private final long totalHits;
    private final long totalMisses;
    private final int queueCount;
    
    public double getHitRatio() {
        long total = totalHits + totalMisses;
        return total > 0 ? (double) totalHits / total : 0.0;
    }
}
```

## 生命周期管理

### 启动流程

1. **定时清理任务**：启动 ScheduledExecutorService
2. **attemptId 清理**：每5分钟清理过期的 attemptId
3. **缓存清理**：每10分钟清理过期的缓存消息

### 关闭流程

1. **定时任务关闭**：优雅关闭 ScheduledExecutorService
2. **锁管理器关闭**：关闭时间轮定时器，取消所有定时任务
3. **缓存清理**：清空所有缓存数据

## 关键设计决策

### 1. 乐观读取策略

**决策**：采用先读取、后判断的乐观策略
**原因**：sharding key 需要从消息体中解析，必须进行磁盘 I/O
**优势**：减少无效的磁盘访问，提高整体性能

### 2. 三级 Map 数据结构

**决策**：使用 `topic@group -> queueId -> shardingKeyHash -> ShardingKeyLock` 结构
**原因**：支持细粒度的锁管理，不同 sharding key 独立处理
**优势**：提供良好的并发性能和内存局部性

### 3. 时间轮定时器

**决策**：使用 Netty 的 HashedWheelTimer 处理锁过期
**原因**：高性能的定时任务调度，适合大量超时任务
**优势**：低延迟、高吞吐量的定时任务处理

### 4. 缓存机制

**决策**：引入多级缓存管理
**原因**：避免过期消息的位点回退，减少重复消费
**优势**：提高消息投递效率，降低系统负载

## 性能优化

### 1. 内存管理

- **对象池化**：复用 ShardingKeyLock 对象
- **批量操作**：支持批量消息处理
- **内存限制**：设置缓存大小限制，防止内存溢出

### 2. 锁优化

- **细粒度锁**：基于 sharding key 的细粒度锁
- **无锁数据结构**：使用 ConcurrentHashMap 等无锁数据结构
- **锁范围最小化**：减少锁的持有时间

### 3. I/O 优化

- **批量读取**：一次性读取多条消息
- **异步处理**：支持异步消息处理
- **缓存预热**：预加载热点数据

## 监控与调试

### 统计信息

- **锁统计**：活跃锁数量、过期锁数量
- **缓存统计**：缓存命中率、缓存大小
- **性能指标**：处理延迟、吞吐量

### 调试接口

```java
// 获取锁管理器统计信息
public String getStatistics() {
    return lockManager.getStatistics();
}

// 获取缓存统计信息
public ShardingKeyCache.CacheStatistics getCacheStatistics() {
    return cache.getStatistics();
}
```

## 兼容性

### 接口兼容性

- **完全兼容**：实现 OrderedConsumptionManager 接口
- **无缝切换**：通过配置切换队列级别或 sharding key 级别
- **向后兼容**：保持与现有代码的完全兼容

### 配置兼容性

```java
// 在 FIFOConsumptionManager 中的配置切换
if (brokerController.getBrokerConfig().getOrderedConsumptionLevel() == OrderedConsumptionLevel.SHARDING_KEY) {
    this.orderedConsumptionManager = new ShardingKeyLevelConsumerManager(brokerController);
} else {
    this.orderedConsumptionManager = new QueueLevelConsumerManager(brokerController);
}
```

## 总结

ShardingKeyLevelConsumerManager 通过引入基于 sharding key 的细粒度锁机制，在保证顺序消费语义的前提下，显著提升了消费并发度。其核心创新在于：

1. **乐观读取**：先读取消息再判断阻塞，适应 sharding key 解析的需求
2. **细粒度锁**：基于 sharding key 的独立锁，不同业务并发处理
3. **高性能设计**：时间轮定时器、无锁数据结构、多级缓存
4. **完全兼容**：无缝替换现有队列级别实现

该实现为 RocketMQ 的顺序消费提供了更高的并发性能，同时保持了良好的可维护性和扩展性。