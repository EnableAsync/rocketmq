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

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;

/**
 * Sharding Key锁管理器
 * 负责管理所有sharding key级别的锁，支持锁的创建、检查、释放和过期处理
 * 集成重试次数持久化功能
 */
public class ShardingKeyLockManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final int ALL_QUEUES = -1;
    private static final int TIMER_TICK_MS = 100;

    /**
     * 主锁存储结构：topic@group -> queueId -> shardingKeyHash -> ShardingKeyLock
     */
    private final ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Integer/* queueId */,
            ConcurrentHashMap<String/* shardingKeyHash */, ShardingKeyLock>>> shardingKeyLockMap;

    /**
     * offset到sharding key的映射，用于ACK时快速查找对应的sharding key
     * topic@group -> queueId -> offset -> shardingKeyHash
     */
    private final ConcurrentHashMap<String/* topic@group */,
        ConcurrentHashMap<Integer/* queueId */,
            ConcurrentSkipListMap<Long/* offset */, String/* shardingKeyHash */>>> offsetToShardingKeyMap;

    /**
     * attemptId到shardingKey的映射，用于处理重复请求
     * attemptId -> AttemptInfo(topic, group, queueId, shardingKey)
     */
    private final ConcurrentHashMap<String/* attemptId */, AttemptInfo> attemptIdToShardingKeyMap;

    /**
     * 时间轮定时器，用于处理锁过期
     */
    private final Timer timer;

    /**
     * 超时任务映射，用于取消和更新定时任务
     */
    private final ConcurrentHashMap<String/* lockKey */, Timeout> timeoutMap;

    private final BrokerController brokerController;

    private final ShardingKeyCache cache;

    /**
     * 过期消息缓存，存储可直接消费的过期消息
     * topic@group@queueId -> shardingKeyHash list
     */
    private final ConcurrentHashMap<String, Set<String>> expiredShardingKeyCache;

    /**
     * 重试次数持久化存储
     */
    private final ShardingKeyRetryStorage retryStorage;

    public ShardingKeyLockManager(BrokerController brokerController, ShardingKeyCache cache) {
        this.brokerController = brokerController;
        this.cache = cache;
        this.shardingKeyLockMap = new ConcurrentHashMap<>(128);
        this.offsetToShardingKeyMap = new ConcurrentHashMap<>(128);
        this.attemptIdToShardingKeyMap = new ConcurrentHashMap<>(1024);
        this.timeoutMap = new ConcurrentHashMap<>(1024);
        this.expiredShardingKeyCache = new ConcurrentHashMap<>(256);

        this.retryStorage = new ShardingKeyRetryStorage(
            brokerController.getMessageStoreConfig().getStorePathRootDir());

        this.timer = new HashedWheelTimer(
            new ThreadFactoryImpl("ConsumerShardingKeyOrderInfoLockManager_"),
            TIMER_TICK_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 启动锁管理器
     */
    public boolean load() {
        // 启动重试次数持久化存储
        boolean storageStarted = retryStorage.load();
        if (!storageStarted) {
            log.warn("Failed to start ShardingKeyRetryStorage, retry times persistence will be disabled");
        }

        log.info("ShardingKeyLockManager started, retry storage: {}", storageStarted ? "enabled" : "disabled");
        return true;
    }

    private ShardingKeyLock getLock(String topic, String group, int queueId, String shardingKeyHash) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        // 测试时，shardingKey 用明文
//        String shardingKeyHash = MessageShardingKeyUtil.calculateHashKey(shardingKey);
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap =
            shardingKeyLockMap.get(topicGroupKey);
        if (queueMap == null) {
            return null;
        }
        ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.get(queueId);
        if (shardingKeyMap == null) {
            return null;
        }
        return shardingKeyMap.get(shardingKeyHash);
    }

    /**
     * 检查指定sharding key是否被锁定
     */
    public boolean isLocked(String topic, String group, int queueId, String shardingKey, String attemptId) {
        if (attemptId != null && attemptIdToShardingKeyMap.containsKey(attemptId)) {
            // 重复的 attemptId，需要处理之前的 shardingKey 锁
            AttemptInfo previousAttemptInfo = attemptIdToShardingKeyMap.get(attemptId);
            if (previousAttemptInfo != null) {
                log.info("检测到重复attemptId: {}, 之前的shardingKey: {}, 当前shardingKey: {}",
                    attemptId, previousAttemptInfo.shardingKey, shardingKey);

                // 处理之前的shardingKey锁，让其消息变为可用
                handleDuplicateAttemptId(previousAttemptInfo.topic, previousAttemptInfo.group,
                    previousAttemptInfo.queueId, previousAttemptInfo.shardingKey);

                // 更新映射为当前的 shardingKey
                attemptIdToShardingKeyMap.put(attemptId, new AttemptInfo(topic, group, queueId, shardingKey));
            }
            return false;
        }

        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKey);
        if (lock == null) {
            return false;
        }

        return lock.needBlock(attemptId);
    }

    public void increaseRetryTimes(String topic, String group, int queueId, String shardingKeyHash,
        List<Long> offsets) {
        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKeyHash);
        if (lock != null) {
            int newRetryTimes = lock.getRetryTimes() + 1;
            lock.setRetryTimes(newRetryTimes);
            retryStorage.setRetryTimes(topic, group, queueId, shardingKeyHash, newRetryTimes);
            log.debug("重试次数增加: {}, 当前为: {}", shardingKeyHash, newRetryTimes);
        }
    }

    public void buildRetryTimesInfo(String topic, String group, int queueId, String shardingKeyHash, List<Long> offsets,
        StringBuilder orderInfoBuilder) {
        offsets.forEach(offset -> {
            ShardingKeyLock lock = getLock(topic, group, queueId, shardingKeyHash);
            if (lock != null) {
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, lock.getRetryTimes());
            }
        });
    }

    public void createOrUpdateLock(String topic, String group, int queueId, String shardingKey,
        long popTime, long invisibleTime, String attemptId, List<Long> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
//      String shardingKeyHash = MessageShardingKeyUtil.calculateHashKey(shardingKey);
        // TODO: 测试时 hash 用明文，替换成 hash 值
        String shardingKeyHash = shardingKey;

        // 记录attemptId到shardingKey的映射
        if (attemptId != null) {
            attemptIdToShardingKeyMap.put(attemptId, new AttemptInfo(topic, group, queueId, shardingKeyHash));
        }

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap =
            shardingKeyLockMap.computeIfAbsent(topicGroupKey, k -> new ConcurrentHashMap<>());

        ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap =
            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>());

        // 计算锁释放时间戳
        long lockFreeTimestamp = popTime + invisibleTime;

        // 创建或更新锁
        ShardingKeyLock lock = shardingKeyMap.computeIfAbsent(shardingKeyHash, k -> {
            ShardingKeyLock newLock = new ShardingKeyLock(popTime, lockFreeTimestamp, attemptId);
            // 从持久化存储中读取重试次数
            int persistedRetryTimes = retryStorage.getRetryTimes(topic, group, queueId, shardingKeyHash);
            newLock.setRetryTimes(persistedRetryTimes);
            log.debug("创建新锁，从持久化存储读取重试次数: shardingKey={}, retryTimes={}",
                shardingKeyHash, persistedRetryTimes);
            return newLock;
        });

        log.debug("增加 shardingKey 的锁: " + shardingKeyHash);

        lock.setPopTime(popTime);

        for (long offset : offsets) {
            lock.addOffset(offset);
            updateOffsetShardingKeyMapping(topicGroupKey, queueId, offset, shardingKeyHash);
        }

        scheduleExpireTask(topic, group, queueId, shardingKeyHash, lockFreeTimestamp);

        log.debug("Created/Updated lock for shardingKey: {} with {} offsets, lockFreeTime: {}",
            shardingKey, offsets.size(), lockFreeTimestamp);
    }

    /**
     * 释放指定 offset 对应的 sharding key 锁
     *
     * @return true 如果这个 offset 是该 shardingKey 下的最后一个 offset，锁被彻底释放
     */
    public boolean releaseLock(String topic, String group, int queueId, long offset, long popTime) {
        log.debug("确认消息: {}|{}|{}|{}", topic, group, queueId, offset);
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        String shardingKeyHash = findShardingKeyByOffset(topicGroupKey, queueId, offset);
        if (shardingKeyHash == null) {
            log.warn("Cannot find sharding key for offset: {} in topic: {}, group: {}, queueId: {}",
                offset, topic, group, queueId);
            log.debug("没有 offset 到 shardingKey 的映射，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap = shardingKeyLockMap.get(topicGroupKey);
        if (queueMap == null) {
            log.debug("没有订阅关系上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.get(queueId);
        if (shardingKeyMap == null) {
            notifyLongPolling(topic, group, ALL_QUEUES);
            log.debug("没有 queue 上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ShardingKeyLock lock = shardingKeyMap.get(shardingKeyHash);
        if (lock == null) {
            notifyLongPolling(topic, group, ALL_QUEUES);
            log.debug("没有 shardingKey 上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        if (lock.getPopTime() != popTime) {
            log.warn("PopTime mismatch for offset: {}, expected: {}, actual: {}",
                offset, lock.getPopTime(), popTime);
            return false;
        }

        boolean removed = lock.removeOffset(offset);
        if (removed) {
            removeOffsetToShardingKey(topicGroupKey, queueId, offset);

            if (lock.isEmpty()) {
                cancelExpireTask(topic, group, queueId, shardingKeyHash);

                boolean activated = cache.activateMessages(topic, group, queueId, shardingKeyHash);
                log.debug("释放了 shardingKey 的锁: {}", shardingKeyHash);
                shardingKeyMap.remove(shardingKeyHash);

                retryStorage.removeRetryTimes(topic, group, queueId, shardingKeyHash);
                log.debug("清理持久化重试次数记录: shardingKey={}", shardingKeyHash);

                if (activated) {
                    log.debug("消息ACK成功，激活ShardingKey缓存消息但是不唤醒长轮询，更新完 offset 在 ackOrderlyNew 中唤醒: shardingKey={}, offset={}", shardingKeyHash, offset);
                } else {
                    log.warn("消息ACK成功，激活ShardingKey缓存消息失败: shardingKey={}, offset={}", shardingKeyHash, offset);
                }

                return true; // 返回 true 表示锁已完全释放
            }
        }

        return removed; // 表示有没有被 ack 成功
    }

    /**
     * 更新消息的下次可见时间
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long offset,
        long popTime, long nextVisibleTime) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        String shardingKeyHash = findShardingKeyByOffset(topicGroupKey, queueId, offset);

        if (shardingKeyHash == null) {
            log.warn("Cannot find sharding key for offset: {} when updating visible time", offset);
            return;
        }

        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKeyHash);
        if (lock == null || lock.getPopTime() != popTime) {
            log.warn("更新不可见时间时 pop time 不一致: {}", offset);
            return;
        }

        lock.updateLockFreeTimestamp(nextVisibleTime);
        scheduleExpireTask(topic, group, queueId, shardingKeyHash, nextVisibleTime);
        log.debug("更新不可见时间 shardingKey: {}, offset: {}, nextVisibleTime: {}",
            shardingKeyHash, offset, nextVisibleTime);
    }

    public void clearQueueLocks(String topic, String group, int queueId) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap = shardingKeyLockMap.get(topicGroupKey);
        if (queueMap != null) {
            ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.remove(queueId);
            if (shardingKeyMap != null) {
                for (String shardingKeyHash : shardingKeyMap.keySet()) {
                    cancelExpireTask(topic, group, queueId, shardingKeyHash);
                }

                log.debug("Cleared all locks for topic: {}, group: {}, queueId: {}", topic, group, queueId);
            }
        }

        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMap = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMap != null) {
            groupOffsetMap.remove(queueId);
        }
    }

    /**
     * 调度锁过期任务
     */
    private void scheduleExpireTask(String topic, String group, int queueId, String shardingKeyHash, long expireTime) {
        String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);
        Timeout oldTimeout = timeoutMap.get(lockKey);
        if (oldTimeout != null && !oldTimeout.isCancelled()) {
            oldTimeout.cancel();
        }

        long delay = expireTime - System.currentTimeMillis();
        if (delay > 0) {
            Timeout timeout = timer.newTimeout(new ExpireTimerTask(topic, group, queueId, shardingKeyHash),
                delay, TimeUnit.MILLISECONDS);
            timeoutMap.put(lockKey, timeout);
            log.debug("增加定时任务: {}", lockKey);
        }
    }

    private void cancelExpireTask(String topic, String group, int queueId, String shardingKeyHash) {
        String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);
        Timeout timeout = timeoutMap.remove(lockKey);
        if (timeout != null && !timeout.isCancelled()) {
            timeout.cancel();
            log.debug("取消定时任务成功: {}", lockKey);
        }
    }

    private void handleExpiredLock(String topic, String group, int queueId, String shardingKeyHash) {
        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKeyHash);
        if (lock == null) {
            log.error("消息过期，但是该消息的锁已经不存在: {}", shardingKeyHash);
            return;
        }

        Set<Long> offsets = lock.getOffsetSet();
        if (offsets.isEmpty()) {
            log.warn("消息过期，但是该shardingKey无 offset: {}", shardingKeyHash);
            return;
        }

        // 增加过期消息到可用消息中，这里没有消息具体内容，只有 offset，和 found 的 GetMessageResult
        log.info("锁已经过期，将过期的sharding key添加到可用缓存中: {}, offsets: {}", shardingKeyHash, offsets);
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        cache.addAvailableMessage(topic, group, queueId, shardingKeyHash, result, new ArrayList<>(offsets));

        if (brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            notifyLongPolling(topic, group, ALL_QUEUES);
        }
    }

    public void updateOffsetToShardingKeyMapping(String topic, String group, int queueId, long offset,
        String shardingKeyHash) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        updateOffsetShardingKeyMapping(topicGroupKey, queueId, offset, shardingKeyHash);
    }

    private void updateOffsetShardingKeyMapping(String topicGroupKey, int queueId, long offset,
        String shardingKeyHash) {
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps =
            offsetToShardingKeyMap.computeIfAbsent(topicGroupKey, k -> new ConcurrentHashMap<>());

        ConcurrentSkipListMap<Long, String> queueOffsetMap =
            groupOffsetMaps.computeIfAbsent(queueId, k -> new ConcurrentSkipListMap<>());

        queueOffsetMap.put(offset, shardingKeyHash);
    }

    private void removeOffsetToShardingKey(String topicGroupKey, int queueId, long offset) {
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps != null) {
            ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
            if (queueOffsetMap != null) {
                queueOffsetMap.remove(offset);
            }
        }
    }

    private String findShardingKeyByOffset(String topicGroupKey, int queueId, long offset) {
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps == null) {
            return null;
        }

        ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
        if (queueOffsetMap == null) {
            return null;
        }

        return queueOffsetMap.get(offset);
    }

    private String buildLockKey(String topic, String group, int queueId, String shardingKeyHash) {
        return topic + "@" + group + "@" + queueId + "@" + shardingKeyHash;
    }

    private void notifyLongPolling(String topic, String group, int queueId) {
        if (brokerController != null && brokerController.getPopMessageProcessor() != null) {
            log.info("从锁中唤醒长轮询: topic: {}, group: {}, queueId: {}", topic, group, queueId);
            brokerController.getPopMessageProcessor().notifyMessageArriving(topic, queueId, group);
        }
    }

    /**
     * 获取统计信息
     */
    public String getStatistics() {
        int totalLocks = 0;
        int totalQueues = 0;
        int totalGroups = shardingKeyLockMap.size();

        for (ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap : shardingKeyLockMap.values()) {
            totalQueues += queueMap.size();
            for (ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap : queueMap.values()) {
                totalLocks += shardingKeyMap.size();
            }
        }

        return String.format("ShardingKeyLockManager stats: groups=%d, queues=%d, locks=%d, attemptIds=%d",
            totalGroups, totalQueues, totalLocks, attemptIdToShardingKeyMap.size());
    }

    /**
     * 关闭管理器
     */
    public void shutdown() {
        for (Timeout timeout : timeoutMap.values()) {
            if (!timeout.isCancelled()) {
                timeout.cancel();
            }
        }
        timeoutMap.clear();
        timer.stop();
        retryStorage.shutdown();
        log.debug("ShardingKeyLockManager shutdown completed");
    }

    /**
     * 过期定时任务
     */
    private class ExpireTimerTask implements TimerTask {
        private final String topic;
        private final String group;
        private final int queueId;
        private final String shardingKeyHash;

        public ExpireTimerTask(String topic, String group, int queueId, String shardingKeyHash) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
            this.shardingKeyHash = shardingKeyHash;
        }

        @Override
        public void run(Timeout timeout) {
            if (timeout.isCancelled()) {
                return;
            }
            handleExpiredLock(topic, group, queueId, shardingKeyHash);

            // 清理timeout映射
            String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);
            timeoutMap.computeIfPresent(lockKey, (key1, curTimeout) -> {
                if (curTimeout == timeout) {
                    // remove from map
                    return null;
                }
                return curTimeout;
            });
        }
    }

    /**
     * 获取指定队列中所有飞行中消息的最小 offset
     * 用于 commitAndNext 返回正确的消费位点
     */
    public long getMinInFlightOffset(String topic, String group, int queueId, long queueOffset) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        log.debug("开始获取最小飞行中消息 offset: {}", topicGroupKey);

        // 从 offset 映射中获取所有飞行中的 offset
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps == null) {
            log.debug("所有消息已确认，返回下一个消费位点: {}", queueOffset + 1);
            return queueOffset + 1; // 没有飞行中的消息
        }

        ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
        if (queueOffsetMap == null) {
            log.debug("所有消息已确认，返回下一个消费位点: {}", queueOffset + 1);
            return queueOffset + 1; // 没有飞行中的消息
        }

        // 要确保给出去的消息的 offset 都存了 shardingKey
        Map.Entry<Long, String> entry = queueOffsetMap.firstEntry();
        if (entry != null) {
            log.debug("返回当前未被ACK的最小offset: {}", entry.getKey());
            return entry.getKey();
        } else {
            return -3; // 返回 -3 表示当前 offset 被确认，然后没有其他消息可以被消费了，这个时候提交 pull offset
        }
    }

    /**
     * 获取重试次数存储的统计信息
     */
    public String getRetryStorageStatistics() {
        return String.format("RetryStorage: cacheSize=%d",
            retryStorage.getCacheSize());
    }

    /**
     * 处理重复attemptId的情况
     * 与handleExpiredLock类似，将之前shardingKey的消息放入可用缓存
     */
    private void handleDuplicateAttemptId(String topic, String group, int queueId, String shardingKeyHash) {
        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKeyHash);
        if (lock == null) {
            log.warn("处理重复attemptId时，shardingKey的锁已经不存在: {}", shardingKeyHash);
            return;
        }

        Set<Long> offsets = lock.getOffsetSet();
        if (offsets.isEmpty()) {
            log.warn("处理重复attemptId时，shardingKey无offset: {}", shardingKeyHash);
            return;
        }

        log.debug("处理重复attemptId，将shardingKey的消息放入可用缓存: {}, offsets: {}", shardingKeyHash, offsets);

        // 创建GetMessageResult，与handleExpiredLock一样处理
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        cache.addAvailableMessage(topic, group, queueId, shardingKeyHash, result, new ArrayList<>(offsets));

        // 清除该shardingKey的锁
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap = shardingKeyLockMap.get(topicGroupKey);
        if (queueMap != null) {
            ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.get(queueId);
            if (shardingKeyMap != null) {
                shardingKeyMap.remove(shardingKeyHash);
                cancelExpireTask(topic, group, queueId, shardingKeyHash);
                retryStorage.removeRetryTimes(topic, group, queueId, shardingKeyHash);
                log.debug("清理重复attemptId对应的shardingKey锁: {}", shardingKeyHash);
            }
        }
        if (brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            notifyLongPolling(topic, group, ALL_QUEUES);
        }
    }

    /**
     * AttemptInfo内部类，用于存储attemptId对应的信息
     */
    private static class AttemptInfo {
        final String topic;
        final String group;
        final int queueId;
        final String shardingKey;

        AttemptInfo(String topic, String group, int queueId, String shardingKey) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
            this.shardingKey = shardingKey;
        }

        @Override
        public String toString() {
            return String.format("AttemptInfo{topic='%s', group='%s', queueId=%d, shardingKey='%s'}",
                topic, group, queueId, shardingKey);
        }
    }
}