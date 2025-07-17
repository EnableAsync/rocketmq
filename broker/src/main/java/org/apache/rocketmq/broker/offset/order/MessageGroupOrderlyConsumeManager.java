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
 * <p>
 * 数据结构：
 * topic@group -> queueId -> messageGroup -> OrderInfo
 */
public class MessageGroupOrderlyConsumeManager implements OrderlyConsumeManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public static final String CONTROLLER_TYPE = "MESSAGE_GROUP_LEVEL";

    // Topic和Group的分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 默认消息组，用于没有sharding key的消息
    private static final String DEFAULT_MESSAGE_GROUP = "";

    // topic@group -> queueId -> messageGroup -> message count
    private final
    ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Integer/* queueId */,
            ConcurrentHashMap<String/* messageGroup */, Integer>>> shardingKeyCountMap =
        new ConcurrentHashMap<>(128);

    // topic@group -> queueId -> offset -> messageGroup
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
    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        // message group 不阻塞 queue，先取数据，但是取数据之后不提交位点，而是看这批消息的 sharding key
        return false;
    }

    /**
     * 在 handleGetMessageResult 中被调用，可以在这里过滤给消费者的消息
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
        // 在这里获取到了所有的拉取到的消息，在这里实现 sharding key 分组和过滤
        if (msgQueueOffsetList == null || msgQueueOffsetList.isEmpty()) {
            return;
        }

        String key = buildKey(topic, group);

        // 获取或创建队列映射
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>> queueShardingKeyMap =
            shardingKeyCountMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
        ConcurrentHashMap<Integer, ConcurrentHashMap<Long, String>> queueOffsetMap =
            offsetShardingKeyMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));

        // 使用 computeIfAbsent 确保线程安全的初始化
        ConcurrentHashMap<String, Integer> blockedShardingKeyMap =
            queueShardingKeyMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));
        ConcurrentHashMap<Long, String> offsetMap =
            queueOffsetMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));

        // 检查是否为新创建的空映射（第一次访问该队列）
        boolean isFirstAccess = blockedShardingKeyMap.isEmpty() && offsetMap.isEmpty();

        if (isFirstAccess) {
            // 使用 synchronized 确保初始化过程的原子性
            synchronized (blockedShardingKeyMap) {
                // 双重检查，防止重复初始化
                if (blockedShardingKeyMap.isEmpty() && offsetMap.isEmpty()) {
                    // 遍历所有消息，记录 sharding key 信息但不过滤任何消息
                    for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
                        ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
                        String shardingKey = extractShardingKey(byteBuffer);

                        // 在 shardingKeyCountMap中 记录 sharding key
                        blockedShardingKeyMap.putIfAbsent(shardingKey, 1);

                        // 在 offsetShardingKeyMap中 记录 offset对应的sharding key
                        offsetMap.putIfAbsent(getMessageResult.getMessageQueueOffset().get(i), shardingKey);
                    }

                    // 不过滤任何消息，直接返回所有结果
                    return;
                }
            }
        }

        // 如果不是首次访问或者在 synchronized 块中发现已被其他线程初始化，执行过滤逻辑
        List<Integer> removeIndex = new ArrayList<>();
        for (int i = 0; i < getMessageResult.getMessageBufferList().size(); i++) {
            ByteBuffer byteBuffer = getMessageResult.getMessageBufferList().get(i);
            String shardingKey = extractShardingKey(byteBuffer);
            Integer previousValue = blockedShardingKeyMap.putIfAbsent(shardingKey, 1);
            if (previousValue != null) {
                // key 已经存在，说明这个 shardingKey 正在被处理中（被其他消息占用）
                // 这条消息不发送给客户端消费
                removeIndex.add(i);
                // 缓存 offset -> shardingKey 的映射，用于后续解锁
                offsetMap.putIfAbsent(getMessageResult.getMessageQueueOffset().get(i), shardingKey);
            }
        }
        // 过滤掉不发送给客户端的消息
        getMessageResult.removeMessageByIndexList(removeIndex);
    }

    private String extractShardingKey(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            log.info("extract shardingKey from null byteBuffer");
            return DEFAULT_MESSAGE_GROUP;
        }
        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);
        if (properties == null) {
            log.info("extract shardingKey from null properties");
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
//        String key = buildKey(topic, group);
//        ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>> queueMap = shardingKeyCountMap.get(key);
//        ConcurrentHashMap<Long, String> offsetMap = offsetShardingKeyMap.get(key);
//
//        if (queueMap == null || offsetMap == null) {
//            return queueOffset + 1; // 没有顺序信息，返回下一个偏移量
//        }
//
//        ConcurrentHashMap<String, Integer> blockedShardingKeyMap = queueMap.get(queueId);
//        if (blockedShardingKeyMap == null) {
//            log.warn("blockedShardingKeyMap is null, {}, {}", key, queueOffset);
//            return queueOffset + 1; // 没有顺序信息，返回下一个偏移量
//        }

        return -1;
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {

    }

    @Override
    public void clearBlock(String topic, String group, int queueId) {

    }

    @Override
    public String getControllerType() {
        return CONTROLLER_TYPE;
    }

    @Override
    public void start() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.start();
        }
        log.info("MessageGroupOrderlyConsumeController started");
    }

    @Override
    public void shutdown() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.shutdown();
        }
        log.info("MessageGroupOrderlyConsumeController shutdown");
    }

    /**
     * 构建Topic和Group的组合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 消息组分析结果
     */
    private static class MessageGroupAnalysis {
        private Map<String, List<Long>> messageGroupOffsets = new HashMap<>();

        public Map<String, List<Long>> getMessageGroupOffsets() {
            return messageGroupOffsets;
        }

        public void setMessageGroupOffsets(Map<String, List<Long>> messageGroupOffsets) {
            this.messageGroupOffsets = messageGroupOffsets;
        }
    }
}