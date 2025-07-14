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

/**
 * 消费者顺序信息管理器
 * 主要用于管理RocketMQ中POP消费模式下的顺序消息消费状态
 * <p>
 * 核心功能：
 * 1. 管理每个队列中已弹出但未确认的消息的状态信息
 * 2. 控制顺序消息的阻塞逻辑，确保消息按顺序消费
 * 3. 跟踪消息的可见时间和重试次数
 * 4. 提供消息确认和下一个消费偏移量的计算
 */
public class ConsumerOrderInfoManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    // Topic和Group的分隔符，用于构建唯一键
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    // 自动清理的时间间隔：24小时
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;

    /**
     * 核心数据结构：存储所有的顺序信息
     * 外层Map的Key: "topic@group" 格式的字符串
     * 内层Map的Key: queueId（队列ID）
     * Value: OrderInfo对象，包含该队列的消费顺序信息
     */
    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    // 锁管理器，用于管理消费者的锁状态（transient表示不会被序列化）
    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;

    // Broker控制器引用
    private transient BrokerController brokerController;

    // 顺序消费控制器，支持不同的并发策略
    private final transient OrderlyConsumeController orderlyConsumeController;

    /**
     * 默认构造函数
     */
    public ConsumerOrderInfoManager() {
        // 默认使用队列级别的控制器
        this.orderlyConsumeController = new QueueLevelOrderlyConsumeController(this.table, this.consumerOrderInfoLockManager);

        // 启动控制器
        this.orderlyConsumeController.start();
    }

    /**
     * 带参数的构造函数
     *
     * @param brokerController Broker控制器，用于获取配置和管理信息
     */
    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = new ConsumerOrderInfoLockManager(brokerController);

        // 根据配置选择不同的顺序消费策略
        String controllerType = brokerController.getBrokerConfig().getOrderlyConsumeControllerType();
        if ("MESSAGE_GROUP_LEVEL".equals(controllerType)) {
            // 使用消息组级别的高并发控制器
            this.orderlyConsumeController = new MessageGroupOrderlyConsumeController(brokerController, this.consumerOrderInfoLockManager);
        } else {
            // 默认使用队列级别的控制器
            this.orderlyConsumeController = new QueueLevelOrderlyConsumeController(this.table, this.consumerOrderInfoLockManager);
        }

        // 启动控制器
        this.orderlyConsumeController.start();
    }

    // Getter和Setter方法
    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    /**
     * 构建Topic和Group的组合键
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @return 格式为"topic@group"的字符串
     */
    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * 解析组合键，分离出Topic和Group
     *
     * @param key 格式为"topic@group"的字符串
     * @return 包含topic和group的字符串数组
     */
    protected static String[] decodeKey(String key) {
        return key.split(TOPIC_GROUP_SEPARATOR);
    }

    /**
     * 更新消息列表的接收状态
     * 这是核心方法之一，当消费者POP消息时调用
     */
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime,
        long invisibleTime,
        List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder) {

        if (orderlyConsumeController != null) {
            orderlyConsumeController.update(attemptId, isRetry, topic, group, queueId, popTime, invisibleTime, msgQueueOffsetList, orderInfoBuilder);
        }
    }

    /**
     * 检查是否需要阻塞当前的POP请求
     * 用于确保顺序消息的顺序消费
     */
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        return orderlyConsumeController != null &&
            orderlyConsumeController.checkBlock(attemptId, topic, group, queueId, invisibleTime);
    }

    /**
     * 清除指定队列的阻塞状态
     * 通常在消费者重新平衡或队列重新分配时调用
     */
    public void clearBlock(String topic, String group, int queueId) {
        if (orderlyConsumeController != null) {
            orderlyConsumeController.clearBlock(topic, group, queueId);
        }
    }

    /**
     * 提交消息并计算下一个消费偏移量
     * 这是核心方法之一，当消费者ACK消息时调用
     */
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        if (orderlyConsumeController != null) {
            return orderlyConsumeController.commitAndNext(topic, group, queueId, queueOffset, popTime);
        }
        return queueOffset + 1; // 默认返回下一个偏移量
    }

    /**
     * 更新消息的下次可见时间
     * 用于消息的延时重新消费
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {
        if (orderlyConsumeController != null) {
            orderlyConsumeController.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);
        }
    }

    /**
     * 自动清理过期的顺序信息
     * 清理策略：
     * 1. 主题不存在
     * 2. 消费者组不存在
     * 3. 队列映射为空
     * 4. 队列ID超出范围
     * 5. 长时间未消费（超过24小时）
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

            // 检查消费者组是否存在
            if (!this.brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            // 检查队列映射是否为空
            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            // 清理无效的队列信息
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
     */
    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    /**
     * 从JSON字符串反序列化配置
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
                // 恢复锁管理器的状态
                if (this.consumerOrderInfoLockManager != null) {
                    this.consumerOrderInfoLockManager.recover(this.table);
                }
            }
        }
    }

    /**
     * 序列化为JSON字符串
     * 在序列化前会执行自动清理
     */
    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean(); // 序列化前清理过期数据
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 关闭管理器，释放资源
     */
    public void shutdown() {
        if (this.orderlyConsumeController != null) {
            this.orderlyConsumeController.shutdown();
        }
        if (this.consumerOrderInfoLockManager != null) {
            this.consumerOrderInfoLockManager.shutdown();
        }
    }

    @VisibleForTesting
    protected ConsumerOrderInfoLockManager getConsumerOrderInfoLockManager() {
        return consumerOrderInfoLockManager;
    }

    /**
     * 顺序信息类
     * 存储单个队列的消费顺序状态
     */
    public static class OrderInfo {
        // 弹出消息的时间戳
        private long popTime;

        /**
         * 消息不可见时间（毫秒）
         * 在此时间内，消息不会被其他消费者看到
         */
        @JSONField(name = "i")
        private Long invisibleTime;

        /**
         * 偏移量列表（压缩存储）
         * offsetList[0] 是第一个消息的队列偏移量
         * offsetList[i] (i > 0) 是第i个消息与第一个消息的偏移量差值
         * 这种存储方式可以节省空间，特别是当消息偏移量连续时
         */
        @JSONField(name = "o")
        private List<Long> offsetList;

        /**
         * 消息的下次可见时间映射
         * key: 消息的队列偏移量
         * value: 该消息的下次可见时间戳
         */
        @JSONField(name = "ot")
        private Map<Long, Long> offsetNextVisibleTime;

        /**
         * 消息的消费次数映射
         * key: 消息的队列偏移量
         * value: 该消息被消费的次数
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
         * 使用位运算来标记哪些消息已经被ACK
         * 第i位为1表示第i个消息已被确认
         */
        @JSONField(name = "cm")
        private long commitOffsetBit;

        /**
         * 尝试ID，用于区分不同的消费尝试
         */
        @JSONField(name = "a")
        private String attemptId;

        public OrderInfo() {
        }

        /**
         * 构造函数
         *
         * @param attemptId            尝试ID
         * @param popTime              弹出时间
         * @param invisibleTime        不可见时间
         * @param queueOffsetList      队列偏移量列表
         * @param lastConsumeTimestamp 最后消费时间戳
         * @param commitOffsetBit      提交偏移量位图
         */
        public OrderInfo(String attemptId, long popTime, long invisibleTime, List<Long> queueOffsetList,
            long lastConsumeTimestamp,
            long commitOffsetBit) {
            this.popTime = popTime;
            this.invisibleTime = invisibleTime;
            this.offsetList = buildOffsetList(queueOffsetList); // 转换为压缩格式
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
         * 将队列偏移量列表转换为压缩格式
         * 第一个元素保持不变，后续元素存储与第一个元素的差值
         * <p>
         * 例如：[100, 101, 102, 105] -> [100, 1, 2, 5]
         */
        public static List<Long> buildOffsetList(List<Long> queueOffsetList) {
            List<Long> simple = new ArrayList<>();
            if (queueOffsetList.size() == 1) {
                simple.addAll(queueOffsetList);
                return simple;
            }
            Long first = queueOffsetList.get(0);
            simple.add(first);
            for (int i = 1; i < queueOffsetList.size(); i++) {
                simple.add(queueOffsetList.get(i) - first); // 存储差值
            }
            return simple;
        }

        /**
         * 判断是否需要阻塞新的消费请求
         * 用于确保顺序消费的核心逻辑
         *
         * @param attemptId            当前尝试ID
         * @param currentInvisibleTime 当前不可见时间
         * @return true表示需要阻塞
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean needBlock(String attemptId, long currentInvisibleTime) {
            if (offsetList == null || offsetList.isEmpty()) {
                return false;
            }

            // 如果是同一个attemptId，不需要阻塞（同一批消息的重复请求）
            if (this.attemptId != null && this.attemptId.equals(attemptId)) {
                return false;
            }

            int num = offsetList.size();

            // 设置不可见时间
            if (this.invisibleTime == null || this.invisibleTime <= 0) {
                this.invisibleTime = currentInvisibleTime;
            }

            long currentTime = System.currentTimeMillis();

            // 检查是否有未ACK的消息仍在不可见期内
            for (int i = 0; i < num; i++) {
                if (isNotAck(i)) { // 如果消息未被ACK
                    // 计算消息的下次可见时间
                    long nextVisibleTime = popTime + invisibleTime;
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
         * 返回最早的未ACK消息的可见时间
         */
        @JSONField(serialize = false, deserialize = false)
        public Long getLockFreeTimestamp() {
            if (offsetList == null || offsetList.isEmpty()) {
                return null;
            }

            int num = offsetList.size();
            long currentTime = System.currentTimeMillis();

            // 找到第一个未ACK的消息
            for (int i = 0; i < num; i++) {
                if (isNotAck(i)) {
                    if (invisibleTime == null || invisibleTime <= 0) {
                        return null;
                    }

                    // 计算该消息的下次可见时间
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }

                    // 如果还未到可见时间，返回该时间；否则返回当前时间
                    if (currentTime < nextVisibleTime) {
                        return nextVisibleTime;
                    }
                }
            }
            return currentTime;
        }

        /**
         * 更新指定偏移量的下次可见时间
         */
        @JSONField(serialize = false, deserialize = false)
        public void updateOffsetNextVisibleTime(long queueOffset, long nextVisibleTime) {
            if (this.offsetNextVisibleTime == null) {
                this.offsetNextVisibleTime = new HashMap<>();
            }
            this.offsetNextVisibleTime.put(queueOffset, nextVisibleTime);
        }

        /**
         * 获取下一个需要消费的偏移量
         * 基于ACK状态计算下一个消费位置
         *
         * @return -2:错误, 其他值:下一个偏移量
         */
        @JSONField(serialize = false, deserialize = false)
        public long getNextOffset() {
            if (offsetList == null || offsetList.isEmpty()) {
                return -2;
            }

            int num = offsetList.size();
            int i = 0;

            // 找到第一个未ACK的消息
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    break;
                }
            }

            if (i == num) {
                // 所有消息都已ACK，返回最后一个消息的下一个偏移量
                return getQueueOffset(num - 1) + 1;
            }

            // 返回第一个未ACK消息的偏移量
            return getQueueOffset(i);
        }

        /**
         * 将偏移量列表中的索引转换为实际的队列偏移量
         *
         * @param offsetIndex 偏移量列表中的索引
         * @return 实际的队列偏移量
         */
        @JSONField(serialize = false, deserialize = false)
        public long getQueueOffset(int offsetIndex) {
            return getQueueOffset(this.offsetList, offsetIndex);
        }

        /**
         * 静态方法：将压缩格式的偏移量转换为实际偏移量
         */
        protected static long getQueueOffset(List<Long> offsetList, int offsetIndex) {
            if (offsetIndex == 0) {
                return offsetList.get(0); // 第一个元素就是实际偏移量
            }
            return offsetList.get(0) + offsetList.get(offsetIndex); // 第一个元素 + 差值
        }

        /**
         * 检查指定索引的消息是否未被ACK
         *
         * @param offsetIndex 偏移量列表中的索引
         * @return true表示未ACK
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean isNotAck(int offsetIndex) {
            return (commitOffsetBit & (1L << offsetIndex)) == 0;
        }

        /**
         * 合并消费次数信息
         * 将之前的消费记录与当前消费合并，用于统计消息重试次数
         *
         * @param preAttemptId            之前的尝试ID
         * @param preOffsetList           之前的偏移量列表
         * @param prevOffsetConsumedCount 之前的消费次数映射
         */
        @JSONField(serialize = false, deserialize = false)
        public void mergeOffsetConsumedCount(String preAttemptId, List<Long> preOffsetList,
            Map<Long, Integer> prevOffsetConsumedCount) {
            Map<Long, Integer> offsetConsumedCount = new HashMap<>();
            if (prevOffsetConsumedCount == null) {
                prevOffsetConsumedCount = new HashMap<>();
            }

            // 如果是同一个attemptId，直接使用之前的消费次数
            if (preAttemptId != null && preAttemptId.equals(this.attemptId)) {
                this.offsetConsumedCount = prevOffsetConsumedCount;
                return;
            }

            // 构建之前的队列偏移量集合
            Set<Long> preQueueOffsetSet = new HashSet<>();
            for (int i = 0; i < preOffsetList.size(); i++) {
                preQueueOffsetSet.add(getQueueOffset(preOffsetList, i));
            }

            // 为当前偏移量列表中的每个消息计算消费次数
            for (int i = 0; i < offsetList.size(); i++) {
                long queueOffset = this.getQueueOffset(i);
                if (preQueueOffsetSet.contains(queueOffset)) {
                    // 如果之前消费过这个消息，消费次数+1
                    int count = 1;
                    Integer preCount = prevOffsetConsumedCount.get(queueOffset);
                    if (preCount != null) {
                        count = preCount + 1;
                    }
                    offsetConsumedCount.put(queueOffset, count);
                }
                // 注意：新消息（未在preQueueOffsetSet中）的消费次数为0，不记录在map中
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