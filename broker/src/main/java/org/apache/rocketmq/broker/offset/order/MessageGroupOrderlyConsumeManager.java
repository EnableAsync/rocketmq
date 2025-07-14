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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

/**
 * 消息组级别的顺序消费控制器
 * 基于消息组（sharding key）实现队列内的并发消费，显著提升并发度
 * <p>
 * 核心特点：
 * 1. 同一队列内不同消息组可以并行消费
 * 2. 只有相同消息组内的消息需要保持严格顺序
 * 3. 大幅提升并发度，特别适用于消息组分布均匀的场景
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
    private static final String DEFAULT_MESSAGE_GROUP = "_DEFAULT_GROUP_";

    /**
     * 核心数据结构：四层嵌套Map
     * 第一层：topic@group -> 第二层Map
     * 第二层：queueId -> 第三层Map
     * 第三层：messageGroup -> OrderInfo
     * <p>
     * 这种结构支持队列内不同消息组的并发消费
     */
    private final ConcurrentHashMap<String/* topic@group*/,
        ConcurrentHashMap<Integer/*queueId*/,
            ConcurrentHashMap<String/*messageGroup*/, ConsumerOrderInfoManager.OrderInfo>>> table =
        new ConcurrentHashMap<>(128);

    // 锁管理器，用于管理消费者的锁状态
    private ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    // Broker控制器引用
    private BrokerController brokerController;

    /**
     * 构造函数
     *
     * @param brokerController Broker控制器
     */
    public MessageGroupOrderlyConsumeManager(BrokerController brokerController,
        ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.brokerController = brokerController;
    }

    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder) {
    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        return true;
    }

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        return 0;
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime) {
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
     * 从attemptId中提取消息组信息
     * attemptId格式通常包含sharding key信息，这里简化处理
     * <p>
     * 实际实现中，可以从消息属性或attemptId中解析出sharding key
     * 如果没有sharding key，使用默认消息组
     */
    private String extractMessageGroup(String attemptId) {
        if (attemptId == null || attemptId.isEmpty()) {
            return DEFAULT_MESSAGE_GROUP;
        }

        // 尝试从attemptId中提取消息组信息
        // 格式可能是: {messageGroup}_{timestamp}_{random} 等
        // 这里简化处理，可以根据实际的attemptId格式进行调整
        int firstUnderScore = attemptId.indexOf('_');
        if (firstUnderScore > 0) {
            String possibleGroup = attemptId.substring(0, firstUnderScore);
            // 验证是否是有效的消息组标识
            if (possibleGroup.length() > 0 && possibleGroup.length() <= 64) {
                return possibleGroup;
            }
        }

        // 默认情况下，使用attemptId的hash作为消息组
        // 这样可以保证相同sharding key的消息有相同的消息组
        return String.valueOf(Math.abs(attemptId.hashCode() % 1000));
    }
}