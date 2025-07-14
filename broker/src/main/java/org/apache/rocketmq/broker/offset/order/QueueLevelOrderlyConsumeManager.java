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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

/**
 * 队列级别的顺序消费控制器
 * 这是基于现有逻辑的实现，在队列级别进行阻塞控制
 * <p>
 * 特点：
 * 1. 同一队列在有未确认消息时会被整体阻塞
 * 2. 简单可靠，确保严格的队列内顺序
 * 3. 并发度受限于队列数量
 */
public class QueueLevelOrderlyConsumeManager implements OrderlyConsumeManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public static final String CONTROLLER_TYPE = "QUEUE_LEVEL";

    // Topic和Group的分隔符，用于构建唯一键
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    /**
     * 核心数据结构：存储所有的顺序信息
     * 外层Map的Key: "topic@group" 格式的字符串
     * 内层Map的Key: queueId（队列ID）
     * Value: OrderInfo对象，包含该队列的消费顺序信息
     */
    private final ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo>> table;

    // 锁管理器，用于管理消费者的锁状态
    private final ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    /**
     * 构造函数
     *
     * @param table 并发安全
     */
    public QueueLevelOrderlyConsumeManager(ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo>> table,
        ConsumerOrderInfoLockManager consumerOrderInfoLockManager) {
        this.table = table;
        this.consumerOrderInfoLockManager = consumerOrderInfoLockManager;
    }

    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId,
        long popTime, long invisibleTime, List<Long> msgQueueOffsetList,
        StringBuilder orderInfoBuilder) {

        // 构建存储键
        String key = buildKey(topic, group);

        // 获取或创建该topic@group对应的队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        // 获取或创建该队列的顺序信息
        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo != null) {
            // 如果已存在，创建新的OrderInfo并合并消费计数信息
            ConsumerOrderInfoManager.OrderInfo newOrderInfo = new ConsumerOrderInfoManager.OrderInfo(
                attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.getAttemptId(), orderInfo.getOffsetList(), orderInfo.getOffsetConsumedCount());
            orderInfo = newOrderInfo;
        } else {
            // 创建新的OrderInfo
            orderInfo = new ConsumerOrderInfoManager.OrderInfo(
                attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
        }

        // 更新队列的顺序信息
        qs.put(queueId, orderInfo);

        // 构建消费次数信息，用于返回给客户端
        Map<Long, Integer> offsetConsumedCount = orderInfo.getOffsetConsumedCount();
        int minConsumedTimes = Integer.MAX_VALUE;
        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            // 为每个偏移量构建消费次数信息
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }
            if (offsetConsumedCount.size() != orderInfo.getOffsetList().size()) {
                // 如果大小不等，说明有新消息，最小消费次数为0
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // 为了兼容性，构建队列级别的消费次数信息
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);

        // 更新锁释放时间戳
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);

        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            return false; // 没有顺序信息，不需要阻塞
        }

        // 调用OrderInfo的needBlock方法判断是否需要阻塞
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);

        // 获取队列映射
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            return queueOffset + 1; // 没有顺序信息，返回下一个偏移量
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> offsetList = orderInfo.getOffsetList();
        if (offsetList == null || offsetList.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 验证popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}",
                key, queueOffset, orderInfo, popTime);
            return -2; // popTime不匹配，返回错误
        }

        // 找到对应的偏移量在列表中的位置
        long firstOffset = offsetList.get(0);
        int size = offsetList.size();
        int i = 0;

        for (; i < size; i++) {
            long currentOffset = (i == 0) ? firstOffset : firstOffset + offsetList.get(i);
            if (currentOffset == queueOffset) {
                break;
            }
        }

        // 没有找到对应的偏移量
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 设置对应位的ACK标记（使用位运算）
        orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << i));

        // 计算下一个需要消费的偏移量
        long nextOffset = orderInfo.getNextOffset();

        // 更新锁释放时间戳
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);

        return nextOffset;
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset,
        long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }

        ConsumerOrderInfoManager.OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }

        // 验证popTime
        if (popTime != orderInfo.getPopTime()) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}",
                key, queueOffset, orderInfo, popTime);
            return;
        }

        // 更新指定偏移量的下次可见时间
        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    @Override
    public void clearBlock(String topic, String group, int queueId) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = table.get(key);
        if (qs != null) {
            qs.remove(queueId);
        }

        // 清除锁管理器中的相关信息
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.clearLock(topic, group, queueId);
        }
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
        log.info("QueueLevelOrderlyConsumeController started");
    }

    @Override
    public void shutdown() {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.shutdown();
        }
        log.info("QueueLevelOrderlyConsumeController shutdown");
    }

    /**
     * 构建Topic和Group的组合键
     */
    private static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 更新锁释放时间戳
     */
    private void updateLockFreeTimestamp(String topic, String group, int queueId,
        ConsumerOrderInfoManager.OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }
}