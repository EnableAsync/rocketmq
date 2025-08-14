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
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;

/**
 * Sharding Key锁管理器
 * 负责管理所有sharding key级别的锁，支持锁的创建、检查、释放和过期处理
 */
public class ShardingKeyLockManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final int ALL_QUEUES = -1;

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
     * attemptId集合，用于检查重复请求
     */
    private final Set<String> attemptIdSet;

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

    public ShardingKeyLockManager(BrokerController brokerController, ShardingKeyCache cache) {
        this.brokerController = brokerController;
        this.cache = cache;
        this.shardingKeyLockMap = new ConcurrentHashMap<>(128);
        this.offsetToShardingKeyMap = new ConcurrentHashMap<>(128);
        this.attemptIdSet = ConcurrentHashMap.newKeySet();
        this.timeoutMap = new ConcurrentHashMap<>(1024);
        this.expiredShardingKeyCache = new ConcurrentHashMap<>(256);

        // 初始化时间轮定时器
        this.timer = new HashedWheelTimer(
            new ThreadFactoryImpl("ShardingKeyLockManager_"),
            100, TimeUnit.MILLISECONDS, 512);
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
        // 检查attemptId是否重复
        if (attemptId != null && attemptIdSet.contains(attemptId)) {
            return false; // 重复请求，不阻塞
        }

        ShardingKeyLock lock = getLock(topic, group, queueId, shardingKey);
        if (lock == null) {
            return false;
        }

        return lock.needBlock(attemptId);
    }

    /**
     * 创建或更新sharding key锁
     */
    public void createOrUpdateLock(String topic, String group, int queueId, String shardingKey,
        long popTime, long invisibleTime, String attemptId, List<Long> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
//        String shardingKeyHash = MessageShardingKeyUtil.calculateHashKey(shardingKey);
//      测试时 hash 用明文
        String shardingKeyHash = shardingKey;

        // 记录attemptId
        if (attemptId != null) {
            attemptIdSet.add(attemptId);
        }

        // 获取或创建三级Map结构
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap =
            shardingKeyLockMap.computeIfAbsent(topicGroupKey, k -> new ConcurrentHashMap<>());

        ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap =
            queueMap.computeIfAbsent(queueId, k -> new ConcurrentHashMap<>());

        // 计算锁释放时间戳
        long lockFreeTimestamp = popTime + invisibleTime;

        // 创建或更新锁
        ShardingKeyLock lock = shardingKeyMap.computeIfAbsent(shardingKeyHash,
            k -> new ShardingKeyLock(popTime, lockFreeTimestamp, attemptId));
//        System.out.println("增加 shardingKey 的锁: " + shardingKeyHash);
        log.info("增加 shardingKey 的锁: " + shardingKeyHash);

        // 添加offset到锁中
        for (long offset : offsets) {
            lock.addOffset(offset);

            // 更新 offset 到 sharding key 的映射
            updateOffsetShardingKeyMapping(topicGroupKey, queueId, offset, shardingKeyHash);
        }

        // 创建定时任务
        scheduleExpireTask(topic, group, queueId, shardingKeyHash, lockFreeTimestamp);

        log.debug("Created/Updated lock for shardingKey: {} with {} offsets, lockFreeTime: {}",
            shardingKey, offsets.size(), lockFreeTimestamp);
    }

//    /**
//     * 根据消息过滤结果进行锁的筛选
//     */
//    public MessageShardingKeyUtil.MessageFilterResult filterMessagesByLock(String topic, String group, int queueId,
//        String attemptId, MessageShardingKeyUtil.MessageShardingInfo shardingInfo) {
//        MessageShardingKeyUtil.MessageFilterResult result = new MessageShardingKeyUtil.MessageFilterResult();
//
//        // 按sharding key分组消息
//        Map<String, List<MessageShardingKeyUtil.MessageInfo>> shardingKeyGroups = shardingInfo.getShardingKeyGroups();
//
//        for (Map.Entry<String, List<MessageShardingKeyUtil.MessageInfo>> entry : shardingKeyGroups.entrySet()) {
//            String shardingKey = entry.getKey();
//            List<MessageShardingKeyUtil.MessageInfo> messages = entry.getValue();
//
//            // 检查该sharding key是否被锁定
//            if (isLocked(topic, group, queueId, shardingKey, attemptId)) {
//                // 被锁定，加入阻塞列表
//                result.addBlockedMessages(shardingKey, messages);
//            } else {
//                // 未被锁定，加入可用列表
//                result.addAvailableMessages(shardingKey, messages);
//            }
//        }
//
//        return result;
//    }

    /**
     * 释放指定 offset 对应的 sharding key 锁
     *
     * @return true 如果这个 offset 是该 shardingKey 下的最后一个 offset，锁被彻底释放
     */
    public boolean releaseLock(String topic, String group, int queueId, long offset, long popTime) {
        log.info("确认消息: {}|{}|{}|{}", topic, group, queueId, offset);
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);

        // 查找 offset 对应的 sharding key
        String shardingKeyHash = findShardingKeyByOffset(topicGroupKey, queueId, offset);
        if (shardingKeyHash == null) {
            log.warn("Cannot find sharding key for offset: {} in topic: {}, group: {}, queueId: {}",
                offset, topic, group, queueId);
//            notifyLongPolling(topic, group, ALL_QUEUES);
            log.info("没有 offset 到 shardingKey 的映射，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap = shardingKeyLockMap.get(topicGroupKey);
        if (queueMap == null) {
//            notifyLongPolling(topic, group, ALL_QUEUES);
            log.info("没有订阅关系上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.get(queueId);
        if (shardingKeyMap == null) {
            notifyLongPolling(topic, group, ALL_QUEUES);
            log.info("没有 queue 上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        ShardingKeyLock lock = shardingKeyMap.get(shardingKeyHash);
        if (lock == null) {
            notifyLongPolling(topic, group, ALL_QUEUES);
            log.info("没有 shardingKey 上的锁，不唤醒长轮询，在 ackOrderlyNew 中唤醒", shardingKeyHash, offset);
            return false;
        }

        // 验证popTime
        if (lock.getPopTime() != popTime) {
            log.warn("PopTime mismatch for offset: {}, expected: {}, actual: {}",
                offset, lock.getPopTime(), popTime);
            return false;
        }

        // 从锁中移除offset
        boolean removed = lock.removeOffset(offset);
        if (removed) {
            // 移除offset映射
            removeOffsetToShardingKey(topicGroupKey, queueId, offset);

            // 如果锁为空，则完全释放该sharding key锁
            if (lock.isEmpty()) {
                // 取消定时任务
                cancelExpireTask(topic, group, queueId, shardingKeyHash);

                boolean activated = cache.activateMessages(topic, group, queueId, shardingKeyHash);
                log.info("释放了 shardingKey 的锁: {}", shardingKeyHash);
                shardingKeyMap.remove(shardingKeyHash);
                if (activated) {
                    // 激活成功后，确保有新消息，唤醒长轮询
//                    notifyLongPolling(topic, group, ALL_QUEUES);
                    log.info("消息ACK成功，激活ShardingKey缓存消息但是不唤醒长轮询，更新完 offset 在 ackOrderlyNew 中唤醒: shardingKey={}, offset={}", shardingKeyHash, offset);
                } else {
                    log.warn("消息ACK成功，激活ShardingKey缓存消息失败: shardingKey={}, offset={}", shardingKeyHash, offset);
                }

                return true; // 返回 true 表示锁已完全释放
            }
        }

        return removed; // 表示有没有被 ack 成功
    }

    /**
     * 公开方法：根据 offset 查找对应的 sharding key
     * 用于外部调用（如 ShardingKeyLevelConsumerManager）
     */
    public String findShardingKeyByOffset(String topic, String group, int queueId, long offset) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        return findShardingKeyByOffset(topicGroupKey, queueId, offset);
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
            return;
        }

        // 更新锁的释放时间
        lock.updateLockFreeTimestamp(nextVisibleTime);

        // 重新调度过期任务
        scheduleExpireTask(topic, group, queueId, shardingKeyHash, nextVisibleTime);

        log.debug("Updated next visible time for shardingKey: {}, offset: {}, nextVisibleTime: {}",
            shardingKeyHash, offset, nextVisibleTime);
    }

    /**
     * 清除指定队列的所有锁
     */
    public void clearQueueLocks(String topic, String group, int queueId) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);

        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ShardingKeyLock>> queueMap = shardingKeyLockMap.get(topicGroupKey);
        if (queueMap != null) {
            ConcurrentHashMap<String, ShardingKeyLock> shardingKeyMap = queueMap.remove(queueId);
            if (shardingKeyMap != null) {
                // 取消所有相关的定时任务
                for (String shardingKeyHash : shardingKeyMap.keySet()) {
                    cancelExpireTask(topic, group, queueId, shardingKeyHash);
                }

                log.info("Cleared all locks for topic: {}, group: {}, queueId: {}", topic, group, queueId);
            }
        }

        // 清除offset映射
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMap = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMap != null) {
            groupOffsetMap.remove(queueId);
        }
    }

    /**
     * 获取过期的sharding key列表
     */
    public Set<String> getExpiredShardingKeys(String topic, String group, int queueId) {
        String cacheKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        return expiredShardingKeyCache.getOrDefault(cacheKey, ConcurrentHashMap.newKeySet());
    }

    /**
     * 清除过期的sharding key缓存
     */
    public void clearExpiredShardingKeys(String topic, String group, int queueId) {
        String cacheKey = MessageShardingKeyUtil.buildTopicGroupQueueIdentifier(topic, group, queueId);
        expiredShardingKeyCache.remove(cacheKey);
    }

    /**
     * 调度锁过期任务
     */
    private void scheduleExpireTask(String topic, String group, int queueId, String shardingKeyHash, long expireTime) {
        String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);

        // 取消之前的任务
        Timeout oldTimeout = timeoutMap.get(lockKey);
        if (oldTimeout != null && !oldTimeout.isCancelled()) {
            oldTimeout.cancel();
        }

        long delay = expireTime - System.currentTimeMillis();
        if (delay > 0) {
            Timeout timeout = timer.newTimeout(new ExpireTimerTask(topic, group, queueId, shardingKeyHash),
                delay, TimeUnit.MILLISECONDS);
            timeoutMap.put(lockKey, timeout);
//            System.out.println("定时任务: " + timeoutMap);
            log.info("增加定时任务: {}", lockKey);
        } else {
            // 已过期，直接处理
            handleExpiredLock(topic, group, queueId, shardingKeyHash);
        }
    }

    /**
     * 取消过期任务
     */
    private void cancelExpireTask(String topic, String group, int queueId, String shardingKeyHash) {
        String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);
        Timeout timeout = timeoutMap.remove(lockKey);
        if (timeout != null && !timeout.isCancelled()) {
            timeout.cancel();
            log.info("取消定时任务成功: {}", lockKey);
        }
    }

    /**
     * 处理过期锁
     */
    private void handleExpiredLock(String topic, String group, int queueId, String shardingKeyHash) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
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

        // 过期的时候不用删 offset -> shardingKey
        // offset -> shardingKey 表示会被读到的消息
//        for (Long offset : offsets) {
//            removeOffsetToShardingKey(topicGroupKey, queueId, offset);
//        }

        // 重试次数增加
        lock.setRetryTimes(lock.getRetryTimes() + 1);
        log.info("锁已经过期，重试次数增加: {}, 当前为: {}", shardingKeyHash, lock.getRetryTimes());

        // 增加过期消息到可用消息中，这里没有消息具体内容，只有 offset，和 found 的 GetMessageResult
        log.info("锁已经过期，将过期的sharding key添加到可用缓存中: {}, offsets: {}", shardingKeyHash, offsets);
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        cache.addAvailableMessage(topic, group, queueId, shardingKeyHash, result, new ArrayList<>(offsets));

        // 唤醒长轮询
        notifyLongPolling(topic, group, ALL_QUEUES);
    }

    /**
     * 更新offset到sharding key的映射
     */
    public void updateOffsetToShardingKeyMapping(String topic, String group, int queueId, long offset,
        String shardingKeyHash) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        updateOffsetShardingKeyMapping(topicGroupKey, queueId, offset, shardingKeyHash);
    }

    /**
     * 更新offset到sharding key的映射
     */
    private void updateOffsetShardingKeyMapping(String topicGroupKey, int queueId, long offset,
        String shardingKeyHash) {
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps =
            offsetToShardingKeyMap.computeIfAbsent(topicGroupKey, k -> new ConcurrentHashMap<>());

        ConcurrentSkipListMap<Long, String> queueOffsetMap =
            groupOffsetMaps.computeIfAbsent(queueId, k -> new ConcurrentSkipListMap<>());

        queueOffsetMap.put(offset, shardingKeyHash);
    }

    /**
     * 移除offset到sharding key的映射
     */
    private void removeOffsetToShardingKey(String topicGroupKey, int queueId, long offset) {
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps != null) {
            ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
            if (queueOffsetMap != null) {
                queueOffsetMap.remove(offset);
            }
        }
    }

    /**
     * 查找offset对应的sharding key
     */
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

    /**
     * 构建锁key
     */
    private String buildLockKey(String topic, String group, int queueId, String shardingKeyHash) {
        return topic + "@" + group + "@" + queueId + "@" + shardingKeyHash;
    }

    /**
     * 唤醒长轮询
     */
    private void notifyLongPolling(String topic, String group, int queueId) {
        if (brokerController != null && brokerController.getPopMessageProcessor() != null) {
            log.info("从锁中唤醒长轮询: topic: {}, group: {}, queueId: {}", topic, group, queueId);
            brokerController.getPopMessageProcessor().notifyMessageArriving(topic, queueId, group);
        }
    }

    /**
     * 清理过期的attemptId
     */
    public void cleanExpiredAttemptIds() {
        // 简单实现：定期清理所有attemptId
        // 实际实现中可以考虑基于时间的过期策略
        if (attemptIdSet.size() > 10000) {
            attemptIdSet.clear();
            log.info("Cleared attempt ID set due to size limit");
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
            totalGroups, totalQueues, totalLocks, attemptIdSet.size());
    }

    /**
     * 关闭管理器
     */
    public void shutdown() {
        // 取消所有定时任务
        for (Timeout timeout : timeoutMap.values()) {
            if (!timeout.isCancelled()) {
                timeout.cancel();
            }
        }
        timeoutMap.clear();

        // 关闭时间轮
        timer.stop();

        log.info("ShardingKeyLockManager shutdown completed");
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
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }

            handleExpiredLock(topic, group, queueId, shardingKeyHash);

            // 清理timeout映射
            String lockKey = buildLockKey(topic, group, queueId, shardingKeyHash);
            timeoutMap.remove(lockKey, timeout);
        }
    }

    /**
     * 获取指定队列中所有飞行中消息的最小 offset
     * 用于 commitAndNext 返回正确的消费位点
     */
    public long getMinInFlightOffset(String topic, String group, int queueId, long queueOffset) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);
        log.info("开始获取最小飞行中消息 offset: {}", topicGroupKey);

        // 从 offset 映射中获取所有飞行中的 offset
        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps == null) {
            log.info("所有消息已确认，返回下一个消费位点: {}", queueOffset + 1);
            return queueOffset + 1; // 没有飞行中的消息
//            return -1;
        }

        ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
        if (queueOffsetMap == null) {
            log.info("所有消息已确认，返回下一个消费位点: {}", queueOffset + 1);
            return queueOffset + 1; // 没有飞行中的消息
        }

        // 要确保给出去的消息的 offset 都存了 shardingKey
        Map.Entry<Long, String> entry = queueOffsetMap.firstEntry();
        if (entry != null) {
            log.info("返回当前未被ACK的最小offset: {}", entry.getKey());
            return entry.getKey();
        } else {
            return -3; // 返回 -3 表示当前 offset 被确认，然后没有其他消息可以被消费了，这个时候提交 pull offset
        }
    }

    /**
     * 获取指定队列中所有飞行中消息的数量
     */
    public int getInFlightMessageCount(String topic, String group, int queueId) {
        String topicGroupKey = MessageShardingKeyUtil.buildTopicGroupIdentifier(topic, group);

        ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, String>> groupOffsetMaps = offsetToShardingKeyMap.get(topicGroupKey);
        if (groupOffsetMaps == null) {
            return 0;
        }

        ConcurrentSkipListMap<Long, String> queueOffsetMap = groupOffsetMaps.get(queueId);
        if (queueOffsetMap == null) {
            return 0;
        }

        return queueOffsetMap.size();
    }
}