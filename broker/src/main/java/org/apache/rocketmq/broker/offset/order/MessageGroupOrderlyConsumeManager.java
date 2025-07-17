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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;

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

    /**
     * 核心数据结构：四层嵌套Map
     * 第一层：topic@group -> 第二层Map
     * 第二层：queueId -> 第三层Map
     * 第三层：messageGroup -> OrderInfo
     * <p>
     * 这种结构支持队列内不同消息组的并发消费
     */
    private final ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Integer/* queueId */,
            ConcurrentHashMap<String/* messageGroup */, Integer>>> table =
        new ConcurrentHashMap<>(128);

    private final ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Long/* offset */, String/* messageGroup */>> offsetShardingKeyMap = new ConcurrentHashMap<>(128);

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
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>> queueMap =
            table.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
        // 无论多少线程同时访问，同一个 queueId 只会创建一个 map 实例
        ConcurrentHashMap<String, Integer> blockedShardingKeyMap =
            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>(16));

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
                ConcurrentHashMap<Long, String> offsetMap = offsetShardingKeyMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>(16));
                offsetMap.put(getMessageResult.getMessageQueueOffset().get(i), shardingKey);
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

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
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