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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Sharding Key 锁管理器
 * 负责锁的获取、释放、续期和过期清理
 */
public class ShardingKeyLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // 默认锁超时时间：30秒
    private static final long DEFAULT_LOCK_TIMEOUT_MS = 30 * 1000;
    
    // 默认续期间隔：20秒
    private static final long DEFAULT_RENEW_INTERVAL_MS = 20 * 1000;
    
    // 锁清理间隔：10秒
    private static final long LOCK_CLEANUP_INTERVAL_MS = 10 * 1000;
    
    private final ShardingKeyLockWAL lockWAL;
    private final ScheduledExecutorService lockMaintenanceExecutor;
    
    // 当前实例的唯一标识符
    private final String instanceId;
    
    // 锁超时配置
    private final long lockTimeoutMs;
    private final long renewIntervalMs;
    
    // 锁续期任务跟踪
    private final ConcurrentHashMap<String, Object> lockRenewTasks;
    
    public ShardingKeyLockManager(String walPath) {
        this(walPath, DEFAULT_LOCK_TIMEOUT_MS, DEFAULT_RENEW_INTERVAL_MS);
    }
    
    public ShardingKeyLockManager(String walPath, long lockTimeoutMs, long renewIntervalMs) {
        this.lockWAL = new ShardingKeyLockWAL(walPath);
        this.instanceId = UUID.randomUUID().toString();
        this.lockTimeoutMs = lockTimeoutMs;
        this.renewIntervalMs = renewIntervalMs;
        this.lockRenewTasks = new ConcurrentHashMap<>();
        this.lockMaintenanceExecutor = Executors.newScheduledThreadPool(2,
            ThreadUtils.newThreadFactory("ShardingKeyLockManager-", true));
    }
    
    /**
     * 启动锁管理器
     */
    public void start() throws Exception {
        lockWAL.start();
        
        // 启动锁过期清理任务
        lockMaintenanceExecutor.scheduleAtFixedRate(
            this::cleanupExpiredLocks,
            LOCK_CLEANUP_INTERVAL_MS,
            LOCK_CLEANUP_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
        
        log.info("ShardingKeyLockManager started, instanceId={}", instanceId);
    }
    
    /**
     * 关闭锁管理器
     */
    public void shutdown() {
        lockMaintenanceExecutor.shutdown();
        try {
            if (!lockMaintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                lockMaintenanceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            lockMaintenanceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        lockWAL.shutdown();
        log.info("ShardingKeyLockManager shutdown");
    }
    
    /**
     * 尝试获取锁
     * 
     * @param topic 主题
     * @param group 消费组
     * @param queueId 队列ID
     * @param shardingKey sharding key
     * @param offsets 关联的offset列表
     * @return 锁信息，如果获取失败返回null
     */
    public ShardingKeyLock tryAcquireLock(String topic, String group, int queueId, 
                                         String shardingKey, List<Long> offsets) {
        String lockId = generateLockId(topic, group, queueId, shardingKey);
        
        // 检查锁是否已存在且有效
        if (lockWAL.isLockActive(lockId)) {
            log.debug("Lock already exists and active: {}", lockId);
            return null;
        }
        
        // 创建新锁
        long expireTime = System.currentTimeMillis() + lockTimeoutMs;
        ShardingKeyLock lock = new ShardingKeyLock(topic, group, queueId, shardingKey, instanceId, expireTime);
        
        // 设置关联的offset
        if (offsets != null) {
            for (Long offset : offsets) {
                lock.addAssociatedOffset(offset);
            }
            lock.incrementReference(); // 初始引用计数设为1
        }
        
        try {
            // 写入WAL
            lockWAL.writeWALRecord(ShardingKeyLockWAL.WALOperationType.ACQUIRE_LOCK, lockId, lock);
            
            // 启动续期任务
            scheduleRenewTask(lockId);
            
            log.debug("Lock acquired successfully: {}", lockId);
            return lock;
        } catch (Exception e) {
            log.error("Failed to acquire lock: {}", lockId, e);
            return null;
        }
    }
    
    /**
     * 释放锁
     */
    public boolean releaseLock(String topic, String group, int queueId, String shardingKey, long offset) {
        String lockId = generateLockId(topic, group, queueId, shardingKey);
        
        ShardingKeyLock lock = lockWAL.getLock(lockId);
        if (lock == null) {
            log.warn("Lock not found for release: {}", lockId);
            return false;
        }
        
        // 移除关联的offset
        lock.removeAssociatedOffset(offset);
        
        // 减少引用计数
        int refCount = lock.decrementReference();
        
        if (refCount <= 0) {
            // 引用计数为0，释放锁
            try {
                lockWAL.writeWALRecord(ShardingKeyLockWAL.WALOperationType.RELEASE_LOCK, lockId, lock);
                
                // 取消续期任务
                cancelRenewTask(lockId);
                
                log.debug("Lock released completely: {}", lockId);
                return true;
            } catch (Exception e) {
                log.error("Failed to release lock: {}", lockId, e);
                return false;
            }
        } else {
            log.debug("Lock reference count decreased: {} refCount={}", lockId, refCount);
            return true;
        }
    }
    
    /**
     * 续期锁
     */
    public boolean renewLock(String lockId) {
        ShardingKeyLock lock = lockWAL.getLock(lockId);
        if (lock == null || lock.isExpired()) {
            log.debug("Lock not found or expired for renewal: {}", lockId);
            return false;
        }
        
        // 检查锁的拥有者
        if (!instanceId.equals(lock.getOwnerId())) {
            log.warn("Cannot renew lock owned by another instance: {} owner={}", lockId, lock.getOwnerId());
            return false;
        }
        
        long newExpireTime = System.currentTimeMillis() + lockTimeoutMs;
        
        try {
            // 创建续期锁信息
            ShardingKeyLock renewLock = new ShardingKeyLock(
                lock.getTopic(), lock.getGroup(), lock.getQueueId(), 
                lock.getShardingKey(), instanceId, newExpireTime);
            
            lockWAL.writeWALRecord(ShardingKeyLockWAL.WALOperationType.RENEW_LOCK, lockId, renewLock);
            
            log.debug("Lock renewed successfully: {} expireTime={}", lockId, newExpireTime);
            return true;
        } catch (Exception e) {
            log.error("Failed to renew lock: {}", lockId, e);
            return false;
        }
    }
    
    /**
     * 检查锁是否被占用
     */
    public boolean isLockOccupied(String topic, String group, int queueId, String shardingKey) {
        String lockId = generateLockId(topic, group, queueId, shardingKey);
        return lockWAL.isLockActive(lockId);
    }
    
    /**
     * 获取锁信息
     */
    public ShardingKeyLock getLockInfo(String topic, String group, int queueId, String shardingKey) {
        String lockId = generateLockId(topic, group, queueId, shardingKey);
        return lockWAL.getLock(lockId);
    }
    
    /**
     * 清理过期锁
     */
    private void cleanupExpiredLocks() {
        try {
            Map<String, ShardingKeyLock> allLocks = lockWAL.getAllLocks();
            int expiredCount = 0;
            
            for (Map.Entry<String, ShardingKeyLock> entry : allLocks.entrySet()) {
                String lockId = entry.getKey();
                ShardingKeyLock lock = entry.getValue();
                
                if (lock.isExpired()) {
                    try {
                        lockWAL.writeWALRecord(ShardingKeyLockWAL.WALOperationType.EXPIRE_LOCK, lockId, lock);
                        cancelRenewTask(lockId);
                        expiredCount++;
                        
                        log.debug("Expired lock cleaned up: {}", lockId);
                    } catch (Exception e) {
                        log.error("Failed to cleanup expired lock: {}", lockId, e);
                    }
                }
            }
            
            if (expiredCount > 0) {
                log.info("Cleaned up {} expired locks", expiredCount);
            }
        } catch (Exception e) {
            log.error("Failed to cleanup expired locks", e);
        }
    }
    
    /**
     * 调度续期任务
     */
    private void scheduleRenewTask(String lockId) {
        Object task = lockMaintenanceExecutor.scheduleAtFixedRate(
            () -> renewLock(lockId),
            renewIntervalMs,
            renewIntervalMs,
            TimeUnit.MILLISECONDS
        );
        
        lockRenewTasks.put(lockId, task);
    }
    
    /**
     * 取消续期任务
     */
    private void cancelRenewTask(String lockId) {
        Object task = lockRenewTasks.remove(lockId);
        if (task instanceof java.util.concurrent.ScheduledFuture) {
            ((java.util.concurrent.ScheduledFuture<?>) task).cancel(false);
        }
    }
    
    /**
     * 生成锁ID
     */
    private String generateLockId(String topic, String group, int queueId, String shardingKey) {
        return String.format("%s@%s#%d:%s", topic, group, queueId, shardingKey);
    }
    
    /**
     * 获取实例ID
     */
    public String getInstanceId() {
        return instanceId;
    }
    
    /**
     * 获取锁统计信息
     */
    public Map<String, Object> getLockStatistics() {
        Map<String, ShardingKeyLock> allLocks = lockWAL.getAllLocks();
        Map<String, Object> stats = new ConcurrentHashMap<>();
        
        int activeLocks = 0;
        int expiredLocks = 0;
        int ownedLocks = 0;
        
        for (ShardingKeyLock lock : allLocks.values()) {
            if (lock.isExpired()) {
                expiredLocks++;
            } else {
                activeLocks++;
                if (instanceId.equals(lock.getOwnerId())) {
                    ownedLocks++;
                }
            }
        }
        
        stats.put("totalLocks", allLocks.size());
        stats.put("activeLocks", activeLocks);
        stats.put("expiredLocks", expiredLocks);
        stats.put("ownedLocks", ownedLocks);
        stats.put("instanceId", instanceId);
        
        return stats;
    }
}