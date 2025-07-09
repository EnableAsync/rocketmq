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
package org.apache.rocketmq.broker.offset;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

/**
 * 消费者顺序信息管理器
 *
 * 主要功能：
 * 1. 管理Pop消费模式下的顺序消息信息
 * 2. 跟踪每个消费组在每个队列中的消息消费状态
 * 3. 控制顺序消息的消费顺序，确保按offset顺序确认
 * 4. 处理消息的重复消费统计
 * 5. 管理消息的可见性控制
 *
 * 核心概念：
 * - OrderInfo: 记录一批Pop出来的消息的顺序信息
 * - commitOffsetBit: 使用位图记录哪些消息已被确认
 * - offsetList: 压缩存储的偏移量列表（第一个是绝对偏移量，后续是相对偏移量）
 * - 阻塞机制: 当前面的消息未确认时，阻塞后续消息的Pop操作
 *
 * 调用链路：
 * PopMessageProcessor -> ConsumerOrderInfoManager
 * AckMessageProcessor -> ConsumerOrderInfoManager
 */
public class ConsumerOrderInfoManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // 主题和消费组之间的分隔符
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 清理间隔：24小时未消费的记录会被清理
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;

    /**
     * 核心数据结构：存储所有消费者的顺序信息
     * 第一层Key: topic@group 格式的字符串
     * 第二层Key: queueId (队列ID)
     * Value: OrderInfo (该队列的顺序消息信息)
     */
    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    // 顺序信息锁管理器，用于协调锁的释放时间
    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    // Broker控制器引用
    private transient BrokerController brokerController;

    public ConsumerOrderInfoManager() {
    }

    /**
     * 构造函数
     * 意图：初始化管理器并关联锁管理器
     */
    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = new ConsumerOrderInfoLockManager(brokerController);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    /**
     * 构建存储Key的方法
     * 意图：统一Key格式，便于查找和管理
     * 格式：topic@group
     */
    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 解码Key的方法
     * 意图：从Key中提取topic和group信息
     */
    protected static String[] decodeKey(String key) {
        return key.split(TOPIC_GROUP_SEPARATOR);
    }

    /**
     * 更新锁释放时间戳
     * 意图：通知锁管理器更新该队列的锁释放时间
     * <p>
     * 调用链路：各个更新方法 -> updateLockFreeTimestamp()
     */
    private void updateLockFreeTimestamp(String topic, String group, int queueId, OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }

    /**
     * 更新消息列表接收信息（Pop消息时调用）
     * <p>
     * 意图：
     * 1. 记录Pop出的消息列表和相关元信息
     * 2. 合并历史消费次数统计
     * 3. 构建订单信息用于客户端
     * 4. 更新锁释放时间
     * <p>
     * 调用链路：PopMessageProcessor.popMessage() -> update()
     *
     * @param attemptId          尝试ID，用于标识同一次Pop操作
     * @param isRetry            是否为重试主题
     * @param topic              主题名
     * @param group              消费组名
     * @param queueId            队列ID
     * @param popTime            Pop时间
     * @param invisibleTime      不可见时间
     * @param msgQueueOffsetList 消息队列偏移量列表
     * @param orderInfoBuilder   订单信息构建器，用于返回给客户端
     */
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime,
        long invisibleTime,
        List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder) {

        String key = buildKey(topic, group);

        // 获取或创建队列映射表
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo != null) {
            // 如果存在旧的OrderInfo，创建新的并合并消费统计
            OrderInfo newOrderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList,
                System.currentTimeMillis(), 0);
            // 合并之前的消费次数统计
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.attemptId, orderInfo.offsetList, orderInfo.offsetConsumedCount);
            orderInfo = newOrderInfo;
        } else {
            // 创建新的OrderInfo
            orderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList,
                System.currentTimeMillis(), 0);
        }

        qs.put(queueId, orderInfo);

        // 构建顺序信息返回给客户端
        Map<Long, Integer> offsetConsumedCount = orderInfo.offsetConsumedCount;
        int minConsumedTimes = Integer.MAX_VALUE;

        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                // 为每个偏移量构建消费次数信息
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }

            if (offsetConsumedCount.size() != orderInfo.offsetList.size()) {
                // 如果大小不等，说明有新消息（消费次数为0的消息不存储在offsetConsumedCount中）
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // 为了兼容旧版本SDK，构建基于队列ID的消费次数信息
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);

        // 更新锁释放时间
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    /**
     * 检查是否需要阻塞Pop操作
     * <p>
     * 意图：
     * 1. 确保顺序消息按顺序消费
     * 2. 如果有未确认的消息且尚未超时，则阻塞新的Pop操作
     * 3. 相同attemptId的请求不会被阻塞（重复请求）
     * <p>
     * 调用链路：PopMessageProcessor.popMessage() -> checkBlock()
     *
     * @param attemptId     尝试ID
     * @param topic         主题名
     * @param group         消费组名
     * @param queueId       队列ID
     * @param invisibleTime 不可见时间
     * @return true表示需要阻塞，false表示可以继续Pop
     */
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);

        // 获取或创建队列映射表
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            return false; // 没有OrderInfo，不需要阻塞
        }

        // 检查是否需要阻塞
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    /**
     * 清除阻塞状态
     * 意图：当队列的所有消息都已确认时，清除该队列的OrderInfo
     * <p>
     * 调用链路：某些清理逻辑 -> clearBlock()
     */
    public void clearBlock(String topic, String group, int queueId) {
        table.computeIfPresent(buildKey(topic, group), (key, val) -> {
            val.remove(queueId);
            return val;
        });
    }

    /**
     * 提交消息确认并返回下一个消费偏移量
     * <p>
     * 意图：
     * 1. 标记指定偏移量的消息已被确认
     * 2. 计算下一个可以提交的连续偏移量
     * 3. 更新位图标记已确认的消息
     * 4. 更新锁释放时间
     * <p>
     * 核心逻辑：
     * - 使用位图记录已确认的消息
     * - 只有连续确认的消息才能推进消费偏移量
     * - 返回值说明：-1非法，-2无需提交，>=0可提交的偏移量
     * <p>
     * 调用链路：AckMessageProcessor.ackOrderly() -> commitAndNext()
     *
     * @param topic       主题名
     * @param group       消费组名
     * @param queueId     队列ID
     * @param queueOffset 要确认的队列偏移量
     * @param popTime     Pop时间（用于验证）
     * @return 下一个消费偏移量
     */
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            // 没有OrderInfo，直接返回下一个偏移量
            return queueOffset + 1;
        }

        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> o = orderInfo.offsetList;
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 验证popTime是否匹配（防止过期请求）
        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}",
                key, queueOffset, orderInfo, popTime);
            return -2;
        }

        // 在偏移量列表中查找要确认的偏移量
        Long first = o.get(0);
        int i = 0, size = o.size();
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first; // 第一个是绝对偏移量
            } else {
                temp = first + o.get(i); // 后续是相对偏移量，需要加上第一个
            }
            if (queueOffset == temp) {
                break; // 找到了要确认的偏移量
            }
        }

        // 没有找到对应的偏移量
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        // 设置对应位为1，表示已确认
        orderInfo.setCommitOffsetBit(orderInfo.commitOffsetBit | (1L << i));

        // 计算下一个可提交的偏移量
        long nextOffset = orderInfo.getNextOffset();

        // 更新锁释放时间
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);

        return nextOffset;
    }

    /**
     * 更新消息的下次可见时间
     * <p>
     * 意图：
     * 1. 支持动态调整消息的可见性
     * 2. 用于消息的延迟重试机制
     * 3. 更新特定偏移量消息的下次可见时间
     * <p>
     * 调用链路：某些重试逻辑 -> updateNextVisibleTime()
     *
     * @param topic           主题名
     * @param group           消费组名
     * @param queueId         队列ID
     * @param queueOffset     队列偏移量
     * @param popTime         Pop时间（用于验证）
     * @param nextVisibleTime 下次可见时间
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }

        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }

        // 验证popTime
        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}",
                key, queueOffset, orderInfo, popTime);
            return;
        }

        // 更新指定偏移量的下次可见时间
        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);

        // 更新锁释放时间
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    /**
     * 自动清理过期数据
     * <p>
     * 意图：
     * 1. 清理不存在的主题和消费组对应的数据
     * 2. 清理超过队列数量的无效队列数据
     * 3. 清理长时间未消费的过期数据
     * 4. 保持内存使用的合理性
     * <p>
     * 调用链路：encode() -> autoClean() (序列化时清理)
     */
    protected void autoClean() {
        if (brokerController == null) {
            return;
        }

        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = entry.getValue();

            // 解析topic和group
            String[] arrays = decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];

            // 检查主题是否存在
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig == null) {
                iterator.remove();
                log.info("Topic not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            // 检查消费组是否存在
            if (!this.brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            // 检查队列映射表是否为空
            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            // 清理无效的队列数据
            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = qs.entrySet().iterator();
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();

                // 检查队列ID是否超出范围
                if (qsEntry.getKey() >= topicConfig.getReadQueueNums()) {
                    qsIterator.remove();
                    log.info("Queue not exist, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }

                // 检查是否长时间未消费
                if (System.currentTimeMillis() - qsEntry.getValue().getLastConsumeTimestamp() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                }
            }
        }
    }

    // ConfigManager接口实现方法

    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * 获取配置文件路径
     * 意图：确定OrderInfo数据的持久化文件位置
     */
    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    /**
     * 反序列化配置
     * 意图：从持久化文件中恢复OrderInfo数据，并恢复锁管理器状态
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
                // 恢复锁管理器状态
                if (this.consumerOrderInfoLockManager != null) {
                    this.consumerOrderInfoLockManager.recover(this.table);
                }
            }
        }
    }

    /**
     * 序列化配置
     * 意图：将OrderInfo数据序列化为JSON格式进行持久化，序列化前先清理过期数据
     */
    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean(); // 序列化前清理过期数据
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 关闭管理器
     * 意图：优雅关闭锁管理器
     */
    public void shutdown() {
        if (this.consumerOrderInfoLockManager != null) {
            this.consumerOrderInfoLockManager.shutdown();
        }
    }

    @VisibleForTesting
    protected ConsumerOrderInfoLockManager getConsumerOrderInfoLockManager() {
        return consumerOrderInfoLockManager;
    }

    /**
     * 顺序信息内部类
     * <p>
     * 意图：
     * 1. 记录一批Pop消息的详细信息
     * 2. 使用位图高效记录消息确认状态
     * 3. 压缩存储偏移量列表节省内存
     * 4. 支持消息重复消费统计
     * 5. 管理消息的可见性控制
     */
    public static class OrderInfo {
        // Pop时间，用于验证请求的有效性
        private long popTime;

        /**
         * Pop时的不可见时间
         * 使用JSONField注解优化序列化大小
         */
        @JSONField(name = "i")
        private Long invisibleTime;

        /**
         * 偏移量列表（压缩存储）
         * offsetList[0] 是消息的队列偏移量（绝对值）
         * offsetList[i] (i > 0) 是当前消息与offsetList[0]的距离（相对值）
         * 这样设计可以节省存储空间
         */
        @JSONField(name = "o")
        private List<Long> offsetList;

        /**
         * 消息的下次可见时间戳
         * key: 消息队列偏移量
         * value: 下次可见时间戳
         */
        @JSONField(name = "ot")
        private Map<Long, Long> offsetNextVisibleTime;

        /**
         * 消息的消费次数统计
         * key: 消息队列偏移量
         * value: 消费次数
         */
        @JSONField(name = "oc")
        private Map<Long, Integer> offsetConsumedCount;

        /**
         * 最后消费时间戳
         */
        @JSONField(name = "l")
        private long lastConsumeTimestamp;

        /**
         * 提交偏移量位图
         * 使用位图记录哪些消息已被确认
         * 位图的第i位为1表示offsetList[i]对应的消息已确认
         */
        @JSONField(name = "cm")
        private long commitOffsetBit;

        /**
         * 尝试ID，用于标识同一次Pop操作
         */
        @JSONField(name = "a")
        private String attemptId;

        public OrderInfo() {
        }

        /**
         * 构造函数
         * 意图：创建新的OrderInfo实例并初始化所有必要字段
         */
        public OrderInfo(String attemptId, long popTime, long invisibleTime, List<Long> queueOffsetList,
            long lastConsumeTimestamp, long commitOffsetBit) {
            this.popTime = popTime;
            this.invisibleTime = invisibleTime;
            this.offsetList = buildOffsetList(queueOffsetList); // 压缩存储偏移量
            this.lastConsumeTimestamp = lastConsumeTimestamp;
            this.commitOffsetBit = commitOffsetBit;
            this.attemptId = attemptId;
        }

        // Getter和Setter方法
        public List<Long> getOffsetList() {
            return offsetList;
        }

        public void setOffsetList(List<Long> offsetList) {
            this.offsetList = offsetList;
        }

        public long getLastConsumeTimestamp() {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public long getCommitOffsetBit() {
            return commitOffsetBit;
        }

        public void setCommitOffsetBit(long commitOffsetBit) {
            this.commitOffsetBit = commitOffsetBit;
        }

        public long getPopTime() {
            return popTime;
        }

        public void setPopTime(long popTime) {
            this.popTime = popTime;
        }

        public Long getInvisibleTime() {
            return invisibleTime;
        }

        public void setInvisibleTime(Long invisibleTime) {
            this.invisibleTime = invisibleTime;
        }

        public Map<Long, Long> getOffsetNextVisibleTime() {
            return offsetNextVisibleTime;
        }

        public void setOffsetNextVisibleTime(Map<Long, Long> offsetNextVisibleTime) {
            this.offsetNextVisibleTime = offsetNextVisibleTime;
        }

        public Map<Long, Integer> getOffsetConsumedCount() {
            return offsetConsumedCount;
        }

        public void setOffsetConsumedCount(Map<Long, Integer> offsetConsumedCount) {
            this.offsetConsumedCount = offsetConsumedCount;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }

        /**
         * 构建压缩的偏移量列表
         * <p>
         * 意图：
         * 1. 节省内存空间
         * 2. 第一个元素存储绝对偏移量
         * 3. 后续元素存储相对于第一个元素的距离
         * <p>
         * 例如：[100, 101, 102, 105] -> [100, 1, 2, 5]
         */
        public static List<Long> buildOffsetList(List<Long> queueOffsetList) {
            List<Long> simple = new ArrayList<>();
            if (queueOffsetList.size() == 1) {
                // 只有一个元素，直接添加
                simple.addAll(queueOffsetList);
                return simple;
            }

            Long first = queueOffsetList.get(0);
            simple.add(first); // 第一个是绝对偏移量

            // 后续元素存储相对偏移量
            for (int i = 1; i < queueOffsetList.size(); i++) {
                simple.add(queueOffsetList.get(i) - first);
            }
            return simple;
        }

        /**
         * 检查是否需要阻塞Pop操作
         * <p>
         * 意图：
         * 1. 确保顺序消息按顺序消费
         * 2. 如果有未确认的消息且尚未超时，则需要阻塞
         * 3. 相同attemptId的请求不会被阻塞（重复请求处理）
         *
         * @param attemptId            当前请求的尝试ID
         * @param currentInvisibleTime 当前请求的不可见时间
         * @return true表示需要阻塞，false表示不需要阻塞
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean needBlock(String attemptId, long currentInvisibleTime) {
            if (offsetList == null || offsetList.isEmpty()) {
                return false;
            }

            // 相同attemptId的请求不阻塞（重复请求）
            if (this.attemptId != null && this.attemptId.equals(attemptId)) {
                return false;
            }

            int num = offsetList.size();
            int i = 0;

            // 确保有不可见时间设置
            if (this.invisibleTime == null || this.invisibleTime <= 0) {
                this.invisibleTime = currentInvisibleTime;
            }

            long currentTime = System.currentTimeMillis();

            // 检查每个未确认的消息是否还在不可见期内
            for (; i < num; i++) {
                if (isNotAck(i)) { // 如果消息未确认
                    long nextVisibleTime = popTime + invisibleTime;

                    // 检查是否有自定义的下次可见时间
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }

                    // 如果当前时间小于下次可见时间，需要阻塞
                    if (currentTime < nextVisibleTime) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * 获取锁释放时间戳
         * <p>
         * 意图：
         * 1. 计算该队列的锁什么时候可以释放
         * 2. 用于锁管理器协调锁的释放
         * 3. 返回最早的未确认消息的可见时间
         *
         * @return 锁释放时间戳，null表示无需等待
         */
        @JSONField(serialize = false, deserialize = false)
        public Long getLockFreeTimestamp() {
            if (offsetList == null || offsetList.isEmpty()) {
                return null;
            }

            int num = offsetList.size();
            int i = 0;
            long currentTime = System.currentTimeMillis();

            // 查找第一个未确认的消息
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    if (invisibleTime == null || invisibleTime <= 0) {
                        return null;
                    }

                    long nextVisibleTime = popTime + invisibleTime;

                    // 检查自定义可见时间
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }

                    // 如果还在不可见期内，返回可见时间
                    if (currentTime < nextVisibleTime) {
                        return nextVisibleTime;
                    }
                }
            }

            // 所有消息都已可见，返回当前时间
            return currentTime;
        }

        /**
         * 更新指定偏移量的下次可见时间
         * 意图：支持动态调整消息的可见性，用于重试机制
         */
        @JSONField(serialize = false, deserialize = false)
        public void updateOffsetNextVisibleTime(long queueOffset, long nextVisibleTime) {
            if (this.offsetNextVisibleTime == null) {
                this.offsetNextVisibleTime = new HashMap<>();
            }
            this.offsetNextVisibleTime.put(queueOffset, nextVisibleTime);
        }

        /**
         * 获取下一个可以提交的偏移量
         * <p>
         * 意图：
         * 1. 计算连续确认的消息范围
         * 2. 返回下一个可以安全提交的偏移量
         * 3. 确保消费进度的正确性
         *
         * @return 下一个偏移量，-2表示无需提交
         */
        @JSONField(serialize = false, deserialize = false)
        public long getNextOffset() {
            if (offsetList == null || offsetList.isEmpty()) {
                return -2;
            }

            int num = offsetList.size();
            int i = 0;

            // 找到第一个未确认的消息
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    break;
                }
            }

            if (i == num) {
                // 所有消息都已确认，返回最后一个消息的下一个偏移量
                return getQueueOffset(num - 1) + 1;
            }

            // 返回第一个未确认消息的偏移量
            return getQueueOffset(i);
        }

        /**
         * 将偏移量列表中的索引转换为实际的队列偏移量
         * <p>
         * 意图：
         * 1. 解压缩存储的偏移量
         * 2. 将相对偏移量转换为绝对偏移量
         *
         * @param offsetIndex 偏移量列表中的索引
         * @return 实际的队列偏移量
         */
        @JSONField(serialize = false, deserialize = false)
        public long getQueueOffset(int offsetIndex) {
            return getQueueOffset(this.offsetList, offsetIndex);
        }

        /**
         * 静态方法：将偏移量列表中的索引转换为实际的队列偏移量
         * 意图：提供通用的偏移量转换功能
         */
        protected static long getQueueOffset(List<Long> offsetList, int offsetIndex) {
            if (offsetIndex == 0) {
                return offsetList.get(0); // 第一个是绝对偏移量
            }
            return offsetList.get(0) + offsetList.get(offsetIndex); // 绝对偏移量 + 相对偏移量
        }

        /**
         * 检查指定索引的消息是否未确认
         * 意图：使用位图快速检查消息的确认状态
         *
         * @param offsetIndex 偏移量列表中的索引
         * @return true表示未确认，false表示已确认
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean isNotAck(int offsetIndex) {
            return (commitOffsetBit & (1L << offsetIndex)) == 0;
        }

        /**
         * 合并偏移量消费次数统计
         * <p>
         * 意图：
         * 1. 统计消息的重复消费次数
         * 2. 合并历史统计数据
         * 3. 只记录消费次数大于0的消息以节省内存
         *
         * @param preAttemptId            之前的尝试ID
         * @param preOffsetList           之前的偏移量列表
         * @param prevOffsetConsumedCount 之前的消费次数统计
         */
        @JSONField(serialize = false, deserialize = false)
        public void mergeOffsetConsumedCount(String preAttemptId, List<Long> preOffsetList,
            Map<Long, Integer> prevOffsetConsumedCount) {
            Map<Long, Integer> offsetConsumedCount = new HashMap<>();
            if (prevOffsetConsumedCount == null) {
                prevOffsetConsumedCount = new HashMap<>();
            }

            // 如果attemptId相同，直接使用之前的统计
            if (preAttemptId != null && preAttemptId.equals(this.attemptId)) {
                this.offsetConsumedCount = prevOffsetConsumedCount;
                return;
            }

            // 构建之前的偏移量集合
            Set<Long> preQueueOffsetSet = new HashSet<>();
            for (int i = 0; i < preOffsetList.size(); i++) {
                preQueueOffsetSet.add(getQueueOffset(preOffsetList, i));
            }

            // 统计当前偏移量列表中每个消息的消费次数
            for (int i = 0; i < offsetList.size(); i++) {
                long queueOffset = this.getQueueOffset(i);
                if (preQueueOffsetSet.contains(queueOffset)) {
                    // 如果之前也有这个偏移量，消费次数+1
                    int count = 1;
                    Integer preCount = prevOffsetConsumedCount.get(queueOffset);
                    if (preCount != null) {
                        count = preCount + 1;
                    }
                    offsetConsumedCount.put(queueOffset, count);
                }
            }
            this.offsetConsumedCount = offsetConsumedCount;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("popTime", popTime)
                .add("invisibleTime", invisibleTime)
                .add("offsetList", offsetList)
                .add("offsetNextVisibleTime", offsetNextVisibleTime)
                .add("offsetConsumedCount", offsetConsumedCount)
                .add("lastConsumeTimestamp", lastConsumeTimestamp)
                .add("commitOffsetBit", commitOffsetBit)
                .add("attemptId", attemptId)
                .toString();
        }
    }
}