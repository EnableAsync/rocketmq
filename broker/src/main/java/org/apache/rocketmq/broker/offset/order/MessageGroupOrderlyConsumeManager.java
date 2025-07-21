/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.offset.order;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * 消息组级别的顺序消费控制器
 * 基于消息组（sharding key）实现队列内的并发消费，显著提升并发度
 * <p>
 * 核心特点：
 * 1. 同一队列内不同消息组可以并行消费
 * 2. 只有相同消息组内的消息需要保持严格顺序
 * 3. 提升并发度，适用于消息组 sharding key 分散的场景
 * 4. 支持锁的自动过期、续期和WAL持久化
 * 5. 允许消息重复但不允许丢失
 * <p>
 * 数据结构：
 * topic@group -> queueId -> offset -> messageGroup (用于快速查找)
 */
public class MessageGroupOrderlyConsumeManager implements OrderlyConsumeManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public static final String CONTROLLER_TYPE = "MESSAGE_GROUP_LEVEL";

    // Topic和Group的分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 默认消息组，用于没有sharding key的消息
    private static final String DEFAULT_MESSAGE_GROUP = "";

    // 锁管理器
    private final ShardingKeyLockManager lockManager;

    // topic@group -> queueId -> offset -> messageGroup (用于快速查找)
    private final
    ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Integer /* queueId */,
            ConcurrentHashMap<Long/* offset */, String/* messageGroup */>>> offsetShardingKeyMap =
        new ConcurrentHashMap<>(128);

    // 锁管理器，用于管理消费者的锁状态
    private ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    // Broker控制器引用
    private BrokerController brokerController;

    /**
     * 构造函数
     *
     * @param brokerController             Broker控制器
     * @param consumerOrderInfoLockManager 锁管理器
     */
    public MessageGroupOrderlyConsumeManager(BrokerController brokerController,
        ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = consumerOrderInfoLockManager;

        // 初始化锁管理器，WAL路径基于broker存储路径
        String walPath = "/tmp/sharding_key_locks";
        this.lockManager = new ShardingKeyLockManager(walPath);
    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        // message group 不阻塞 queue，先取数据，但是取数据之后不提交位点，而是看这批消息的 sharding key
        System.out.println("MessageGroupOrderlyConsumeManager#checkBlock: " + attemptId + ", " + topic + ", " + group + ", " + queueId + ", " + invisibleTime);
        return false;
    }

    /**
     * 在 handleGetMessageResult 中被调用，与 QueueLevel 不同，在这里还过滤给消费者的消息
     *
     * @param attemptId          区分不同的 pop 请求
     * @param isRetry            是否为重试主题
     * @param topic              主题名称
     * @param group              消费者组名称
     * @param queueId            队列ID
     * @param popTime            弹出消息的时间
     * @param invisibleTime      消息不可见时间
     * @param msgQueueOffsetList 消息的队列偏移量列表
     * @param orderInfoBuilder   用于构建顺序信息的字符串构建器
     * @param getMessageResult   传入的所有的 GetMessageResult
     */
    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {
        System.out.println("MessageGroupOrderlyConsumeManager#update: " + getMessageResult);
        System.out.println("MessageGroupOrderlyConsumeManager#update: " + getMessageResult.getMessageQueueOffset());
        System.out.println("MessageGroupOrderlyConsumeManager#update: " + getMessageResult.getMessageMapedList());
        // 在这里获取到了所有的拉取到的消息，在这里实现 sharding key 分组和过滤
        if (msgQueueOffsetList == null || msgQueueOffsetList.isEmpty()) {
            return;
        }

        String key = buildKey(topic, group);

        // 获取或创建 offset 映射
        ConcurrentHashMap<Integer, ConcurrentHashMap<Long, String>> queueOffsetMap =
            this.offsetShardingKeyMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
        ConcurrentHashMap<Long, String> offsetMap =
            queueOffsetMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));

        // 按sharding key分组消息
        Map<String, List<Integer>> shardingKeyGroups = new HashMap<>();
        Map<String, List<Long>> shardingKeyOffsets = new HashMap<>();

        for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
            ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
            String shardingKey = extractShardingKey(byteBuffer);
            long offset = getMessageResult.getMessageQueueOffset().get(i);

            // 记录offset到sharding key的映射
            offsetMap.put(offset, shardingKey);

            // 按sharding key分组
            shardingKeyGroups.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(i);
            shardingKeyOffsets.computeIfAbsent(shardingKey, k -> new ArrayList<>()).add(offset);
        }

        // 尝试为每个sharding key获取锁
        List<Integer> removeIndex = new ArrayList<>();

        for (Map.Entry<String, List<Integer>> entry : shardingKeyGroups.entrySet()) {
            String shardingKey = entry.getKey();
            List<Integer> messageIndexes = entry.getValue();
            List<Long> offsets = shardingKeyOffsets.get(shardingKey);

            // 检查锁是否被占用
            if (lockManager.isLockOccupied(topic, group, queueId, shardingKey)) {
                // 锁被占用，这批消息不能发送给客户端
                removeIndex.addAll(messageIndexes);
                log.debug("Sharding key locked, messages filtered: topic={}, group={}, queueId={}, shardingKey={}, count={}",
                    topic, group, queueId, shardingKey, messageIndexes.size());
            } else {
                // 尝试获取锁
                ShardingKeyLock lock = lockManager.tryAcquireLock(topic, group, queueId, shardingKey, offsets);
                if (lock == null) {
                    // 获取锁失败，过滤这批消息
                    removeIndex.addAll(messageIndexes);
                    log.debug("Failed to acquire lock, messages filtered: topic={}, group={}, queueId={}, shardingKey={}, count={}",
                        topic, group, queueId, shardingKey, messageIndexes.size());
                } else {
                    log.debug("Lock acquired successfully: topic={}, group={}, queueId={}, shardingKey={}, count={}",
                        topic, group, queueId, shardingKey, messageIndexes.size());
                }
            }
        }

        // 过滤掉不发送给客户端的消息
        getMessageResult.removeMessageByIndexList(removeIndex);
        System.out.println("MessageGroupOrderlyConsumeManager#update: " + getMessageResult.getMessageQueueOffset());
    }

    String extractShardingKey(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            log.debug("extract shardingKey from null byteBuffer");
            return DEFAULT_MESSAGE_GROUP;
        }
        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);
        if (properties == null) {
            log.debug("extract shardingKey from null properties");
            return DEFAULT_MESSAGE_GROUP;
        }
        return properties.getOrDefault(MessageConst.PROPERTY_SHARDING_KEY, DEFAULT_MESSAGE_GROUP);
    }

    /**
     * 在客户端 ack 的时候被调用
     *
     * @param topic       主题名称
     * @param group       消费者组名称
     * @param queueId     队列ID
     * @param queueOffset 消息的队列偏移量
     * @param popTime     弹出时间，用于验证
     * @return -1 时说明客户端提交的偏移量错误，>-1 时客户端消费成功
     */
    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer, ConcurrentHashMap<Long, String>> queueOffsetMap =
            offsetShardingKeyMap.get(key);

        if (queueOffsetMap == null) {
            log.warn("No offset mapping found: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return queueOffset + 1; // 没有映射信息，返回下一个偏移量
        }

        ConcurrentHashMap<Long, String> offsetMap = queueOffsetMap.get(queueId);
        if (offsetMap == null) {
            log.warn("No offset mapping found for queue: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return queueOffset + 1; // 没有映射信息，返回下一个偏移量
        }

        // 获取该offset对应的sharding key
        String shardingKey = offsetMap.remove(queueOffset);
        if (shardingKey == null) {
            log.warn("No sharding key found for offset: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return queueOffset + 1; // 没有找到对应的sharding key，可能是重复ack
        }

        // 释放锁
        boolean released = lockManager.releaseLock(topic, group, queueId, shardingKey, queueOffset);
        if (!released) {
            log.warn("Failed to release lock: topic={}, group={}, queueId={}, shardingKey={}, queueOffset={}",
                topic, group, queueId, shardingKey, queueOffset);
            // 即使释放锁失败，也返回下一个偏移量，避免阻塞消费进度
            // 锁会在过期时自动清理
        } else {
            log.debug("Lock released successfully: topic={}, group={}, queueId={}, shardingKey={}, queueOffset={}",
                topic, group, queueId, shardingKey, queueOffset);
        }

        return queueOffset + 1; // 返回下一个偏移量
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer, ConcurrentHashMap<Long, String>> queueOffsetMap =
            offsetShardingKeyMap.get(key);

        if (queueOffsetMap == null) {
            log.warn("No offset mapping found for updateNextVisibleTime: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return;
        }

        ConcurrentHashMap<Long, String> offsetMap = queueOffsetMap.get(queueId);
        if (offsetMap == null) {
            log.warn("No offset mapping found for queue in updateNextVisibleTime: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return;
        }

        // 获取该offset对应的sharding key
        String shardingKey = offsetMap.get(queueOffset);
        if (shardingKey == null) {
            log.warn("No sharding key found for offset in updateNextVisibleTime: topic={}, group={}, queueId={}, queueOffset={}",
                topic, group, queueId, queueOffset);
            return;
        }

        // 更新锁的过期时间
        boolean updated = lockManager.updateLockExpireTime(topic, group, queueId, shardingKey, nextVisibleTime);
        if (updated) {
            log.debug("Lock expire time updated successfully in updateNextVisibleTime: topic={}, group={}, queueId={}, shardingKey={}, nextVisibleTime={}",
                topic, group, queueId, shardingKey, nextVisibleTime);
        } else {
            log.warn("Failed to update lock expire time in updateNextVisibleTime: topic={}, group={}, queueId={}, shardingKey={}, nextVisibleTime={}",
                topic, group, queueId, shardingKey, nextVisibleTime);
        }
    }

    @Override
    public void clearBlock(String topic, String group, int queueId) {
        // 清除队列的所有锁和映射信息
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer, ConcurrentHashMap<Long, String>> queueOffsetMap =
            offsetShardingKeyMap.get(key);

        if (queueOffsetMap != null) {
            ConcurrentHashMap<Long, String> offsetMap = queueOffsetMap.remove(queueId);
            if (offsetMap != null) {
                // 释放所有相关的锁
                for (Map.Entry<Long, String> entry : offsetMap.entrySet()) {
                    String shardingKey = entry.getValue();
                    long offset = entry.getKey();
                    try {
                        lockManager.releaseLock(topic, group, queueId, shardingKey, offset);
                    } catch (Exception e) {
                        log.warn("Failed to release lock during clearBlock: topic={}, group={}, queueId={}, shardingKey={}",
                            topic, group, queueId, shardingKey, e);
                    }
                }
                log.info("Cleared blocks for queue: topic={}, group={}, queueId={}, lockCount={}",
                    topic, group, queueId, offsetMap.size());
            }
        }
    }

    @Override
    public String getControllerType() {
        return CONTROLLER_TYPE;
    }

    @Override
    public void start() {
        try {
            lockManager.start();
            if (consumerOrderInfoLockManager != null) {
                consumerOrderInfoLockManager.start();
            }
            log.info("MessageGroupOrderlyConsumeManager started");
        } catch (Exception e) {
            log.error("Failed to start MessageGroupOrderlyConsumeManager", e);
            throw new RuntimeException("Failed to start MessageGroupOrderlyConsumeManager", e);
        }
    }

    @Override
    public void shutdown() {
        try {
            lockManager.shutdown();
            if (consumerOrderInfoLockManager != null) {
                consumerOrderInfoLockManager.shutdown();
            }
            log.info("MessageGroupOrderlyConsumeManager shutdown");
        } catch (Exception e) {
            log.error("Failed to shutdown MessageGroupOrderlyConsumeManager", e);
        }
    }

    /**
     * 构建Topic和Group的组合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 获取锁统计信息
     */
    public Map<String, Object> getLockStatistics() {
        return lockManager.getLockStatistics();
    }

    /**
     * 获取锁管理器实例（用于测试和监控）
     */
    public ShardingKeyLockManager getLockManager() {
        return lockManager;
    }
}