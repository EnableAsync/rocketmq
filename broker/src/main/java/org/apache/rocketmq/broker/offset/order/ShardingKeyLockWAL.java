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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import com.alibaba.fastjson.JSON;

/**
 * Sharding Key 锁的 WAL 持久化管理器
 * 使用 RocksDB 作为持久化存储，支持故障恢复
 */
public class ShardingKeyLockWAL {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    
    // WAL 操作类型
    public enum WALOperationType {
        ACQUIRE_LOCK,    // 获取锁
        RENEW_LOCK,      // 续期锁
        RELEASE_LOCK,    // 释放锁
        EXPIRE_LOCK      // 锁过期
    }
    
    // WAL 记录
    public static class WALRecord {
        private final long sequenceId;
        private final long timestamp;
        private final WALOperationType operationType;
        private final String lockId;
        private final ShardingKeyLock lockInfo;
        
        public WALRecord(long sequenceId, WALOperationType operationType, String lockId, ShardingKeyLock lockInfo) {
            this.sequenceId = sequenceId;
            this.timestamp = System.currentTimeMillis();
            this.operationType = operationType;
            this.lockId = lockId;
            this.lockInfo = lockInfo;
        }
        
        // Getters
        public long getSequenceId() { return sequenceId; }
        public long getTimestamp() { return timestamp; }
        public WALOperationType getOperationType() { return operationType; }
        public String getLockId() { return lockId; }
        public ShardingKeyLock getLockInfo() { return lockInfo; }
    }
    
    private final String walPath;
    private final AtomicLong sequenceId;
    private final ScheduledExecutorService cleanupExecutor;
    
    // RocksDB 相关
    private RocksDB rocksDB;
    private Options options;
    private WriteOptions writeOptions;
    
    // 内存中的锁信息缓存
    private final ConcurrentHashMap<String, ShardingKeyLock> lockCache;
    
    // WAL 清理配置
    private final long walRetentionTimeMs;
    private final long cleanupIntervalMs;
    
    static {
        RocksDB.loadLibrary();
    }
    
    public ShardingKeyLockWAL(String walPath) {
        this(walPath, TimeUnit.HOURS.toMillis(24), TimeUnit.HOURS.toMillis(1));
    }
    
    public ShardingKeyLockWAL(String walPath, long walRetentionTimeMs, long cleanupIntervalMs) {
        this.walPath = walPath;
        this.sequenceId = new AtomicLong(0);
        this.lockCache = new ConcurrentHashMap<>();
        this.walRetentionTimeMs = walRetentionTimeMs;
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.newThreadFactory("ShardingKeyLockWAL-Cleanup-", true));
    }
    
    /**
     * 启动 WAL
     */
    public void start() throws RocksDBException {
        // 初始化 RocksDB
        options = new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(64 * 1024 * 1024) // 64MB
            .setMaxWriteBufferNumber(3)
            .setLevel0FileNumCompactionTrigger(4)
            .setLevel0SlowdownWritesTrigger(20)
            .setLevel0StopWritesTrigger(30);
        
        writeOptions = new WriteOptions()
            .setSync(true) // 确保数据持久化
            .setDisableWAL(false);
        
        // 创建目录
        File walDir = new File(walPath);
        if (!walDir.exists()) {
            walDir.mkdirs();
        }
        
        rocksDB = RocksDB.open(options, walPath);
        
        // 恢复锁状态
        recoverLocks();
        
        // 启动清理任务
        cleanupExecutor.scheduleAtFixedRate(
            this::cleanupExpiredWALRecords, 
            cleanupIntervalMs, 
            cleanupIntervalMs, 
            TimeUnit.MILLISECONDS
        );
        
        log.info("ShardingKeyLockWAL started, walPath={}", walPath);
    }
    
    /**
     * 关闭 WAL
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (rocksDB != null) {
            rocksDB.close();
        }
        if (options != null) {
            options.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        
        log.info("ShardingKeyLockWAL shutdown");
    }
    
    /**
     * 写入 WAL 记录
     */
    public void writeWALRecord(WALOperationType operationType, String lockId, ShardingKeyLock lockInfo) {
        try {
            long seqId = sequenceId.incrementAndGet();
            WALRecord record = new WALRecord(seqId, operationType, lockId, lockInfo);
            
            String key = String.format("wal_%020d", seqId);
            String value = JSON.toJSONString(record);
            
            rocksDB.put(writeOptions, key.getBytes(StandardCharsets.UTF_8), 
                       value.getBytes(StandardCharsets.UTF_8));
            
            // 更新内存缓存
            updateLockCache(operationType, lockId, lockInfo);
            
            log.debug("WAL record written: {} {} {}", operationType, lockId, seqId);
        } catch (RocksDBException e) {
            log.error("Failed to write WAL record: {} {}", operationType, lockId, e);
            throw new RuntimeException("WAL write failed", e);
        }
    }
    
    /**
     * 更新锁缓存
     */
    private void updateLockCache(WALOperationType operationType, String lockId, ShardingKeyLock lockInfo) {
        switch (operationType) {
            case ACQUIRE_LOCK:
                lockCache.put(lockId, lockInfo);
                break;
            case RENEW_LOCK:
                ShardingKeyLock existingLock = lockCache.get(lockId);
                if (existingLock != null) {
                    existingLock.renewLock(lockInfo.getExpireTime());
                }
                break;
            case RELEASE_LOCK:
            case EXPIRE_LOCK:
                ShardingKeyLock removedLock = lockCache.remove(lockId);
                if (removedLock != null) {
                    removedLock.release();
                }
                break;
        }
    }
    
    /**
     * 获取锁信息
     */
    public ShardingKeyLock getLock(String lockId) {
        return lockCache.get(lockId);
    }
    
    /**
     * 获取所有锁信息
     */
    public Map<String, ShardingKeyLock> getAllLocks() {
        return new HashMap<>(lockCache);
    }
    
    /**
     * 恢复锁状态
     */
    private void recoverLocks() {
        try {
            org.rocksdb.RocksIterator iterator = rocksDB.newIterator();
            iterator.seekToFirst();
            
            long recoveredCount = 0;
            long maxSeqId = 0;
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                String value = new String(iterator.value(), StandardCharsets.UTF_8);
                
                if (key.startsWith("wal_")) {
                    try {
                        WALRecord record = JSON.parseObject(value, WALRecord.class);
                        maxSeqId = Math.max(maxSeqId, record.getSequenceId());
                        
                        // 只恢复未过期的锁
                        if (record.getLockInfo() != null && !record.getLockInfo().isExpired()) {
                            updateLockCache(record.getOperationType(), record.getLockId(), record.getLockInfo());
                            recoveredCount++;
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse WAL record: {}", key, e);
                    }
                }
                
                iterator.next();
            }
            
            iterator.close();
            
            // 设置序列号
            sequenceId.set(maxSeqId);
            
            log.info("Recovered {} locks from WAL, maxSeqId={}", recoveredCount, maxSeqId);
        } catch (Exception e) {
            log.error("Failed to recover locks from WAL", e);
        }
    }
    
    /**
     * 清理过期的 WAL 记录
     */
    private void cleanupExpiredWALRecords() {
        try {
            long currentTime = System.currentTimeMillis();
            long cutoffTime = currentTime - walRetentionTimeMs;
            
            org.rocksdb.RocksIterator iterator = rocksDB.newIterator();
            iterator.seekToFirst();
            
            int deletedCount = 0;
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                String value = new String(iterator.value(), StandardCharsets.UTF_8);
                
                if (key.startsWith("wal_")) {
                    try {
                        WALRecord record = JSON.parseObject(value, WALRecord.class);
                        if (record.getTimestamp() < cutoffTime) {
                            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
                            deletedCount++;
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse WAL record during cleanup: {}", key, e);
                        // 删除无法解析的记录
                        rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
                        deletedCount++;
                    }
                }
                
                iterator.next();
            }
            
            iterator.close();
            
            if (deletedCount > 0) {
                log.info("Cleaned up {} expired WAL records", deletedCount);
            }
        } catch (Exception e) {
            log.error("Failed to cleanup WAL records", e);
        }
    }
    
    /**
     * 检查锁是否存在且未过期
     */
    public boolean isLockActive(String lockId) {
        ShardingKeyLock lock = lockCache.get(lockId);
        return lock != null && !lock.isExpired();
    }
    
    /**
     * 获取锁的过期时间
     */
    public long getLockExpireTime(String lockId) {
        ShardingKeyLock lock = lockCache.get(lockId);
        return lock != null ? lock.getExpireTime() : 0;
    }
}