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

import com.google.common.base.MoreObjects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Sharding Key级别的锁结构
 * 用于管理单个sharding key对应的消息锁定状态
 */
public class ShardingKeyLock {
    
    /**
     * 阻塞在这个锁上的消息offset集合
     */
    private Set<Long> offsetSet;
    
    /**
     * 锁释放的时间戳（不可见时间结束的时间）
     */
    private long lockFreeTimestamp;
    
    /**
     * 消息被pop的时间
     */
    private long popTime;
    
    /**
     * 不可见时间（毫秒）
     */
    private long invisibleTime;
    
    /**
     * 尝试ID，用于处理重复请求
     */
    private String attemptId;

    /**
     * 消息的重试次数
     */
    private int retryTimes;

    /**
     * 锁创建时间
     */
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

    public static ShardingKeyLock createWithInvisibleTime(long popTime, long invisibleTime, String attemptId) {
        ShardingKeyLock lock = new ShardingKeyLock();
        lock.popTime = popTime;
        lock.invisibleTime = invisibleTime;
        lock.lockFreeTimestamp = popTime + invisibleTime;
        lock.attemptId = attemptId;
        return lock;
    }

    public void addOffset(Long offset) {
        if (offset != null) {
            offsetSet.add(offset);
        }
    }

    public boolean removeOffset(Long offset) {
        if (offset != null) {
            return offsetSet.remove(offset);
        }
        return false;
    }

    public boolean containsOffset(Long offset) {
        return offset != null && offsetSet.contains(offset);
    }

    public boolean isExpired() {
        return System.currentTimeMillis() >= lockFreeTimestamp;
    }

    public boolean isEmpty() {
        return offsetSet.isEmpty();
    }

    public int size() {
        return offsetSet.size();
    }

    public void updateLockFreeTimestamp(long newLockFreeTimestamp) {
        this.lockFreeTimestamp = newLockFreeTimestamp;
        this.invisibleTime = newLockFreeTimestamp - popTime;
    }

    public void updateInvisibleTime(long newInvisibleTime) {
        this.invisibleTime = newInvisibleTime;
        this.lockFreeTimestamp = popTime + newInvisibleTime;
    }

    public boolean needBlock(String currentAttemptId) {
        if (attemptId != null && attemptId.equals(currentAttemptId)) {
            return false;
        }

        if (isExpired()) {
            return false;
        }

        return !isEmpty();
    }

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