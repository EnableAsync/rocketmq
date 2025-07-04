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
package org.apache.rocketmq.broker.pop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pop消费缓存管理类
 *
 * 主要功能：
 * 1. 缓存Pop模式下消费的消息记录，避免频繁访问存储
 * 2. 管理消息的可见性超时，处理消息的重新投递
 * 3. 定期清理过期消息记录，提交消费进度
 * 4. 控制缓存大小，防止内存溢出
 *
 * 调用链路：
 * BrokerController -> PopMessageProcessor -> PopConsumerCache
 * PopConsumerCache.run() (后台线程) -> cleanupRecords() -> reviveConsumer
 */
public class PopConsumerCache extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    // 表示偏移量不存在的常量
    private static final long OFFSET_NOT_EXIST = -1L;

    // Broker控制器，用于访问Broker的各种服务
    private final BrokerController brokerController;

    // 消费者记录的持久化存储服务
    private final PopConsumerKVStore consumerRecordStore;

    // 消费者锁服务，用于控制并发访问
    private final PopConsumerLockService consumerLockService;

    // 消息复活处理器，用于处理超时未确认的消息
    private final Consumer<PopConsumerRecord> reviveConsumer;

    // 缓存大小的估算值，用于控制内存使用
    private final AtomicInteger estimateCacheSize;

    // 核心数据结构：存储每个消费者组-主题-队列的消息记录
    // Key: groupId@topicId@queueId, Value: 该队列的消息记录集合
    private final ConcurrentMap<String, ConsumerRecords> consumerRecordTable;

    /**
     * 构造函数：初始化Pop消费缓存
     *
     * @param brokerController Broker控制器
     * @param consumerRecordStore 消费记录存储
     * @param popConsumerLockService 消费者锁服务
     * @param reviveConsumer 消息复活处理器
     */
    public PopConsumerCache(BrokerController brokerController, PopConsumerKVStore consumerRecordStore,
        PopConsumerLockService popConsumerLockService, Consumer<PopConsumerRecord> reviveConsumer) {
        this.reviveConsumer = reviveConsumer;
        this.brokerController = brokerController;
        this.consumerRecordStore = consumerRecordStore;
        this.consumerLockService = popConsumerLockService;
        this.estimateCacheSize = new AtomicInteger();
        this.consumerRecordTable = new ConcurrentHashMap<>();
    }

    /**
     * 生成缓存Key的方法
     * 意图：为每个消费者组-主题-队列组合生成唯一标识
     *
     * @param groupId 消费者组ID
     * @param topicId 主题ID
     * @param queueId 队列ID
     * @return 格式化的Key字符串
     */
    public String getKey(String groupId, String topicId, int queueId) {
        return groupId + "@" + topicId + "@" + queueId;
    }

    /**
     * 从消费记录中提取Key
     * 意图：统一Key生成逻辑，避免重复代码
     */
    public String getKey(PopConsumerRecord consumerRecord) {
        return consumerRecord.getGroupId() + "@" + consumerRecord.getTopicId() + "@" + consumerRecord.getQueueId();
    }

    /**
     * 获取缓存中的Key数量
     * 意图：监控缓存中有多少个不同的消费者组-主题-队列组合
     */
    public int getCacheKeySize() {
        return this.consumerRecordTable.size();
    }

    /**
     * 获取缓存中记录的总数量
     * 意图：监控缓存使用情况，用于内存管理
     */
    public int getCacheSize() {
        return this.estimateCacheSize.intValue();
    }

    /**
     * 判断缓存是否已满
     * 意图：防止内存溢出，当缓存超过配置的最大值时停止接收新记录
     */
    public boolean isCacheFull() {
        return this.estimateCacheSize.intValue() > brokerController.getBrokerConfig().getPopCkMaxBufferSize();
    }

    /**
     * 获取指定队列在缓存中的最小偏移量
     * 意图：用于消费进度管理，确定可以安全提交的偏移量
     *
     * 调用链路：PopMessageProcessor -> getMinOffsetInCache()
     */
    public long getMinOffsetInCache(String groupId, String topicId, int queueId) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        return consumerRecords != null ? consumerRecords.getMinOffsetInBuffer() : OFFSET_NOT_EXIST;
    }

    /**
     * 获取正在处理中的消息数量
     * 意图：监控系统负载，了解有多少消息正在等待确认
     */
    public long getPopInFlightMessageCount(String groupId, String topicId, int queueId) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        return consumerRecords != null ? consumerRecords.getInFlightRecordCount() : 0L;
    }

    /**
     * 批量写入消费记录到缓存
     * 意图：当消息被Pop出去但尚未确认时，将记录缓存起来，等待确认或超时处理
     *
     * 调用链路：PopMessageProcessor.popMessage() -> writeRecords()
     */
    public void writeRecords(List<PopConsumerRecord> consumerRecordList) {
        // 更新缓存大小估算值
        this.estimateCacheSize.addAndGet(consumerRecordList.size());

        // 遍历每个消费记录
        consumerRecordList.forEach(consumerRecord -> {
            // 获取或创建对应的ConsumerRecords对象
            ConsumerRecords consumerRecords = ConcurrentHashMapUtils.computeIfAbsent(consumerRecordTable,
                this.getKey(consumerRecord), k -> new ConsumerRecords(brokerController.getBrokerConfig(),
                    consumerRecord.getGroupId(), consumerRecord.getTopicId(), consumerRecord.getQueueId()));
            assert consumerRecords != null;
            // 将记录写入到对应的ConsumerRecords中
            consumerRecords.write(consumerRecord);
        });
    }

    /**
     * 删除消费记录（通常在消息被确认时调用）
     * 意图：当消费者确认消息时，从缓存中移除对应的记录
     *
     * 调用链路：PopMessageProcessor.ackMessage() -> deleteRecords()
     *
     * @param consumerRecordList 要删除的消费记录列表
     * @return 未能删除的记录列表（可能在缓存中不存在）
     */
    public List<PopConsumerRecord> deleteRecords(List<PopConsumerRecord> consumerRecordList) {
        int total = consumerRecordList.size();
        List<PopConsumerRecord> remain = new ArrayList<>();

        consumerRecordList.forEach(consumerRecord -> {
            ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(consumerRecord));
            // 如果记录不存在或删除失败，加入到剩余列表中
            if (consumerRecords == null || !consumerRecords.delete(consumerRecord)) {
                remain.add(consumerRecord);
            }
        });

        // 更新缓存大小估算值（减去成功删除的数量）
        this.estimateCacheSize.addAndGet(remain.size() - total);
        return remain;
    }

    /**
     * 清理过期记录的核心方法
     * 意图：
     * 1. 处理消费者离线的情况，清理其相关记录
     * 2. 处理超时的消息，将其发送给revive处理器重新投递
     * 3. 将长时间未处理的记录持久化到存储
     * 4. 提交消费进度
     *
     * 调用链路：run() -> cleanupRecords()
     */
    public int cleanupRecords(Consumer<PopConsumerRecord> consumer) {
        int remain = 0;
        Iterator<Map.Entry<String, ConsumerRecords>> iterator = consumerRecordTable.entrySet().iterator();

        while (iterator.hasNext()) {
            ConsumerRecords records = iterator.next().getValue();

            // 检查消费者是否离线超时
            boolean timeout = consumerLockService.isLockTimeout(
                records.getGroupId(), records.getTopicId());

            if (timeout) {
                // 消费者离线，清理所有相关记录
                List<PopConsumerRecord> removeExpiredRecords =
                    records.removeExpiredRecords(Long.MAX_VALUE);
                if (removeExpiredRecords != null) {
                    // 将记录写入持久化存储
                    consumerRecordStore.writeRecords(removeExpiredRecords);
                }
                log.info("PopConsumerOffline, so clean expire records, groupId={}, topic={}, queueId={}, records={}",
                    records.getGroupId(), records.getTopicId(), records.getQueueId(),
                    removeExpiredRecords != null ? removeExpiredRecords.size() : 0);
                iterator.remove();
                continue;
            }

            long currentTime = System.currentTimeMillis();
            List<PopConsumerRecord> writeConsumerRecords = new ArrayList<>();

            // 移除过期的记录
            List<PopConsumerRecord> consumerRecords = records.removeExpiredRecords(currentTime);
            if (consumerRecords != null) {
                consumerRecords.forEach(consumerRecord -> {
                    if (consumerRecord.getVisibilityTimeout() <= currentTime) {
                        // 消息已超过可见性超时时间，发送给revive处理器重新投递
                        consumer.accept(consumerRecord);
                    } else {
                        // 消息尚未超时但在缓存中待太久，写入存储等待后续处理
                        writeConsumerRecords.add(consumerRecord);
                    }
                });
            }

            // 将记录写入持久化存储
            consumerRecordStore.writeRecords(writeConsumerRecords);

            // 提交该队列的最小偏移量作为消费进度
            long offset = records.getMinOffsetInBuffer();
            if (offset > OFFSET_NOT_EXIST) {
                this.commitOffset("PopConsumerCache",
                    records.getGroupId(), records.getTopicId(), records.getQueueId(), offset);
            }

            // 累计剩余的记录数量
            remain += records.getInFlightRecordCount();
        }
        return remain;
    }

    /**
     * 提交消费偏移量
     * 意图：将消费进度持久化，确保消息不会重复消费
     *
     * 调用链路：cleanupRecords() -> commitOffset()
     */
    public void commitOffset(String clientHost, String groupId, String topicId, int queueId, long offset) {
        // 尝试获取锁，避免并发提交导致的问题
        if (!consumerLockService.tryLock(groupId, topicId)) {
            return;
        }
        try {
            ConsumerOffsetManager consumerOffsetManager = brokerController.getConsumerOffsetManager();
            // 检查当前存储的偏移量
            long commit = consumerOffsetManager.queryOffset(groupId, topicId, queueId);
            if (commit != OFFSET_NOT_EXIST && offset < commit) {
                // 如果要提交的偏移量小于已存储的偏移量，记录警告
                log.info("PopConsumerCache, consumer offset less than store, " +
                    "groupId={}, topicId={}, queueId={}, offset={}", groupId, topicId, queueId, offset);
            }
            // 提交新的偏移量
            consumerOffsetManager.commitOffset(clientHost, groupId, topicId, queueId, offset);
        } finally {
            consumerLockService.unlock(groupId, topicId);
        }
    }

    /**
     * 移除指定队列的所有记录
     * 意图：当队列被删除或重置时清理相关缓存
     */
    public void removeRecords(String groupId, String topicId, int queueId) {
        this.consumerRecordTable.remove(this.getKey(groupId, topicId, queueId));
    }

    @Override
    public String getServiceName() {
        return PopConsumerCache.class.getSimpleName();
    }

    /**
     * 后台线程的主循环
     * 意图：定期清理过期记录，处理超时消息，维护缓存健康状态
     *
     * 调用链路：ServiceThread.start() -> run() -> cleanupRecords()
     */
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 等待1秒或直到被唤醒
                this.waitForRunning(TimeUnit.SECONDS.toMillis(1));

                // 执行清理操作，返回剩余的记录数量
                int cacheSize = this.cleanupRecords(reviveConsumer);

                // 更新缓存大小估算值
                this.estimateCacheSize.set(cacheSize);
            } catch (Exception e) {
                log.error("PopConsumerCacheService revive error", e);
            }
        }
    }

    /**
     * 内部类：管理单个消费者组-主题-队列的消息记录
     * 意图：
     * 1. 使用TreeMap按偏移量有序存储消息记录
     * 2. 提供线程安全的读写操作
     * 3. 支持过期记录的批量移除
     */
    protected static class ConsumerRecords {
        // 保护并发访问的锁
        private final Lock lock;
        private final String groupId;
        private final String topicId;
        private final int queueId;
        private final BrokerConfig brokerConfig;

        // 核心数据结构：按偏移量排序的消息记录
        // Key: 消息偏移量, Value: 消费记录
        private final TreeMap<Long /* offset */, PopConsumerRecord> recordTreeMap;

        public ConsumerRecords(BrokerConfig brokerConfig, String groupId, String topicId, int queueId) {
            this.groupId = groupId;
            this.topicId = topicId;
            this.queueId = queueId;
            this.lock = new ReentrantLock();
            this.brokerConfig = brokerConfig;
            this.recordTreeMap = new TreeMap<>();
        }

        /**
         * 写入消费记录
         * 意图：线程安全地添加新的消费记录
         */
        public void write(PopConsumerRecord record) {
            lock.lock();
            try {
                recordTreeMap.put(record.getOffset(), record);
            } finally {
                lock.unlock();
            }
        }

        /**
         * 删除消费记录
         * 意图：线程安全地移除已确认的消费记录
         *
         * @return true表示删除成功，false表示记录不存在
         */
        public boolean delete(PopConsumerRecord record) {
            PopConsumerRecord popConsumerRecord;
            lock.lock();
            try {
                popConsumerRecord = recordTreeMap.remove(record.getOffset());
            } finally {
                lock.unlock();
            }
            return popConsumerRecord != null;
        }

        /**
         * 获取缓存中的最小偏移量
         * 意图：用于确定可以安全提交的消费进度
         */
        public long getMinOffsetInBuffer() {
            Map.Entry<Long, PopConsumerRecord> entry = recordTreeMap.firstEntry();
            return entry != null ? entry.getKey() : OFFSET_NOT_EXIST;
        }

        /**
         * 获取正在处理中的记录数量
         */
        public int getInFlightRecordCount() {
            return recordTreeMap.size();
        }

        /**
         * 移除过期的消费记录
         * 意图：
         * 1. 移除超过可见性超时时间的记录（需要重新投递）
         * 2. 移除在缓存中停留时间过长的记录（写入存储）
         *
         * @param currentTime 当前时间戳
         * @return 被移除的记录列表
         */
        public List<PopConsumerRecord> removeExpiredRecords(long currentTime) {
            List<PopConsumerRecord> result = null;
            lock.lock();
            try {
                Iterator<Map.Entry<Long, PopConsumerRecord>> iterator = recordTreeMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, PopConsumerRecord> entry = iterator.next();

                    // 检查是否过期：
                    // 1. 超过可见性超时时间
                    // 2. 在缓存中停留时间超过配置的最大时间
                    if (entry.getValue().getVisibilityTimeout() <= currentTime ||
                        entry.getValue().getPopTime() + brokerConfig.getPopCkStayBufferTime() <= currentTime) {
                        if (result == null) {
                            result = new ArrayList<>();
                        }
                        result.add(entry.getValue());
                        iterator.remove();
                    }
                }
            } finally {
                lock.unlock();
            }
            return result;
        }

        // Getter方法
        public String getGroupId() {
            return groupId;
        }

        public String getTopicId() {
            return topicId;
        }

        public int getQueueId() {
            return queueId;
        }

        @Override
        public String toString() {
            return "ConsumerRecords{" +
                "lock=" + lock +
                ", topicId=" + topicId +
                ", groupId=" + groupId +
                ", queueId=" + queueId +
                ", recordTreeMap=" + recordTreeMap.size() +
                '}';
        }
    }
}
