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
import com.google.common.base.MoreObjects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Sharding Key级别的锁结构
 * 用于管理单个sharding key对应的消息锁定状态
 */
public class ShardingKeyLock {
    
    /**
     * 阻塞在这个锁上的消息offset集合
     */
    @JSONField(name = "o")
    private Set<Long> offsetSet;
    
    /**
     * 锁释放的时间戳（不可见时间结束的时间）
     */
    @JSONField(name = "t")
    private long lockFreeTimestamp;
    
    /**
     * 消息被pop的时间
     */
    @JSONField(name = "p")
    private long popTime;
    
    /**
     * 不可见时间（毫秒）
     */
    @JSONField(name = "i")
    private long invisibleTime;
    
    /**
     * 尝试ID，用于处理重复请求
     */
    @JSONField(name = "a")
    private String attemptId;

    /**
     * 消息的重试次数
     */
    @JSONField(name = "r")
    private int retryTimes;

    /**
     * 锁创建时间
     */
    @JSONField(name = "c")
    private long createTime;
    
    public ShardingKeyLock() {
        this.offsetSet = new ConcurrentSkipListSet<>();
        this.createTime = System.currentTimeMillis();
    }
    
    public ShardingKeyLock(long popTime, long lockFreeTimestamp, String attemptId) {
        this();
        this.popTime = popTime;
        this.lockFreeTimestamp = lockFreeTimestamp;
        this.attemptId = attemptId;
        this.invisibleTime = lockFreeTimestamp - popTime;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    /**
     * 通过不可见时间创建锁的静态方法
     */
    public static ShardingKeyLock createWithInvisibleTime(long popTime, long invisibleTime, String attemptId) {
        ShardingKeyLock lock = new ShardingKeyLock();
        lock.popTime = popTime;
        lock.invisibleTime = invisibleTime;
        lock.lockFreeTimestamp = popTime + invisibleTime;
        lock.attemptId = attemptId;
        return lock;
    }
    
    /**
     * 添加一个offset到锁中
     */
    public void addOffset(Long offset) {
        if (offset != null) {
            offsetSet.add(offset);
        }
    }
    
    /**
     * 从锁中移除一个offset
     */
    public boolean removeOffset(Long offset) {
        if (offset != null) {
            return offsetSet.remove(offset);
        }
        return false;
    }
    
    /**
     * 检查是否包含指定的offset
     */
    public boolean containsOffset(Long offset) {
        return offset != null && offsetSet.contains(offset);
    }
    
    /**
     * 检查锁是否已过期
     */
    @JSONField(serialize = false, deserialize = false)
    public boolean isExpired() {
        return System.currentTimeMillis() >= lockFreeTimestamp;
    }
    
    /**
     * 检查锁是否为空（没有阻塞任何offset）
     */
    @JSONField(serialize = false, deserialize = false)
    public boolean isEmpty() {
        return offsetSet.isEmpty();
    }
    
    /**
     * 获取锁中的offset数量
     */
    @JSONField(serialize = false, deserialize = false)
    public int size() {
        return offsetSet.size();
    }
    
    /**
     * 更新锁的释放时间
     */
    public void updateLockFreeTimestamp(long newLockFreeTimestamp) {
        this.lockFreeTimestamp = newLockFreeTimestamp;
        this.invisibleTime = newLockFreeTimestamp - popTime;
    }
    
    /**
     * 更新不可见时间
     */
    public void updateInvisibleTime(long newInvisibleTime) {
        this.invisibleTime = newInvisibleTime;
        this.lockFreeTimestamp = popTime + newInvisibleTime;
    }
    
    /**
     * 检查是否需要阻塞（基于attemptId和过期时间）
     */
    @JSONField(serialize = false, deserialize = false)
    public boolean needBlock(String currentAttemptId) {
        // 如果是相同的attemptId，不阻塞（重复请求）
        if (attemptId != null && attemptId.equals(currentAttemptId)) {
            return false;
        }
        
        // 如果锁已过期，不阻塞
        if (isExpired()) {
            return false;
        }
        
        // 如果锁为空，不阻塞
        if (isEmpty()) {
            return false;
        }
        
        return true;
    }
    
    // Getters and Setters
    public Set<Long> getOffsetSet() {
        return offsetSet;
    }
    
    public void setOffsetSet(Set<Long> offsetSet) {
        this.offsetSet = offsetSet != null ? offsetSet : new TreeSet<>();
    }
    
    public long getLockFreeTimestamp() {
        return lockFreeTimestamp;
    }
    
    public void setLockFreeTimestamp(long lockFreeTimestamp) {
        this.lockFreeTimestamp = lockFreeTimestamp;
    }
    
    public long getPopTime() {
        return popTime;
    }
    
    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }
    
    public long getInvisibleTime() {
        return invisibleTime;
    }
    
    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }
    
    public String getAttemptId() {
        return attemptId;
    }
    
    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }
    
    public long getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("offsetSet", offsetSet)
                .add("lockFreeTimestamp", lockFreeTimestamp)
                .add("popTime", popTime)
                .add("invisibleTime", invisibleTime)
                .add("attemptId", attemptId)
                .add("createTime", createTime)
                .add("expired", isExpired())
                .add("empty", isEmpty())
                .toString();
    }
}