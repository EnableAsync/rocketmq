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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sharding Key 锁信息
 * 支持自动过期、续期和持久化
 */
public class ShardingKeyLock {
    
    // 锁的唯一标识符
    private final String lockId;
    
    // 锁的主题和消费组
    private final String topic;
    private final String group;
    private final int queueId;
    private final String shardingKey;
    
    // 锁的持有者标识
    private final String ownerId;
    
    // 锁的创建时间
    private final long createTime;
    
    // 锁的过期时间（绝对时间戳）
    private final AtomicLong expireTime;
    
    // 锁的引用计数（有多少个消息正在使用这个锁）
    private final AtomicInteger referenceCount;
    
    // 与这个锁关联的 offset 集合
    private final Set<Long> associatedOffsets;
    
    // 锁的状态
    private volatile LockStatus status;
    
    // 最后续期时间
    private final AtomicLong lastRenewTime;
    
    /**
     * 锁状态枚举
     */
    public enum LockStatus {
        ACTIVE,    // 活跃状态
        EXPIRED,   // 已过期
        RELEASED   // 已释放
    }
    
    public ShardingKeyLock(String topic, String group, int queueId, String shardingKey, 
                          String ownerId, long expireTimeMs) {
        this.lockId = generateLockId(topic, group, queueId, shardingKey);
        this.topic = topic;
        this.group = group;
        this.queueId = queueId;
        this.shardingKey = shardingKey;
        this.ownerId = ownerId;
        this.createTime = System.currentTimeMillis();
        this.expireTime = new AtomicLong(expireTimeMs);
        this.referenceCount = new AtomicInteger(0);
        this.associatedOffsets = ConcurrentHashMap.newKeySet();
        this.status = LockStatus.ACTIVE;
        this.lastRenewTime = new AtomicLong(this.createTime);
    }
    
    /**
     * 生成锁的唯一标识符
     */
    private static String generateLockId(String topic, String group, int queueId, String shardingKey) {
        return String.format("%s@%s#%d:%s", topic, group, queueId, shardingKey);
    }
    
    /**
     * 增加引用计数
     */
    public int incrementReference() {
        return referenceCount.incrementAndGet();
    }
    
    /**
     * 减少引用计数
     */
    public int decrementReference() {
        return referenceCount.decrementAndGet();
    }
    
    /**
     * 续期锁
     */
    public boolean renewLock(long newExpireTime) {
        if (status != LockStatus.ACTIVE) {
            return false;
        }
        
        long currentTime = System.currentTimeMillis();
        if (currentTime > expireTime.get()) {
            status = LockStatus.EXPIRED;
            return false;
        }
        
        expireTime.set(newExpireTime);
        lastRenewTime.set(currentTime);
        return true;
    }
    
    /**
     * 检查锁是否过期
     */
    public boolean isExpired() {
        if (status == LockStatus.EXPIRED || status == LockStatus.RELEASED) {
            return true;
        }
        
        long currentTime = System.currentTimeMillis();
        if (currentTime > expireTime.get()) {
            status = LockStatus.EXPIRED;
            return true;
        }
        
        return false;
    }
    
    /**
     * 释放锁
     */
    public void release() {
        status = LockStatus.RELEASED;
    }
    
    /**
     * 添加关联的 offset
     */
    public void addAssociatedOffset(long offset) {
        associatedOffsets.add(offset);
    }
    
    /**
     * 移除关联的 offset
     */
    public void removeAssociatedOffset(long offset) {
        associatedOffsets.remove(offset);
    }
    
    // Getters
    public String getLockId() { return lockId; }
    public String getTopic() { return topic; }
    public String getGroup() { return group; }
    public int getQueueId() { return queueId; }
    public String getShardingKey() { return shardingKey; }
    public String getOwnerId() { return ownerId; }
    public long getCreateTime() { return createTime; }
    public long getExpireTime() { return expireTime.get(); }
    public int getReferenceCount() { return referenceCount.get(); }
    public Set<Long> getAssociatedOffsets() { return associatedOffsets; }
    public LockStatus getStatus() { return status; }
    public long getLastRenewTime() { return lastRenewTime.get(); }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardingKeyLock that = (ShardingKeyLock) o;
        return Objects.equals(lockId, that.lockId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(lockId);
    }
    
    @Override
    public String toString() {
        return "ShardingKeyLock{" +
                "lockId='" + lockId + '\'' +
                ", ownerId='" + ownerId + '\'' +
                ", expireTime=" + expireTime.get() +
                ", referenceCount=" + referenceCount.get() +
                ", status=" + status +
                ", associatedOffsets=" + associatedOffsets.size() +
                '}';
    }
}