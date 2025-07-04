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

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pop消费者锁服务
 *
 * 【设计目的】
 * 这是一个专为Pop消费模式设计的分布式锁服务，主要用于：
 * 1. 防止同一消费组对同一Topic的并发Pop操作造成的数据竞争
 * 2. 确保Pop消息记录的写入和删除操作的原子性
 * 3. 避免缓存和持久化存储之间的数据不一致
 * 4. 提供超时自动清理机制，防止死锁和内存泄漏
 *
 * 【锁的粒度】
 * 锁的粒度是：消费组 + Topic 级别
 * Key格式：groupId@topicId
 *
 * 【使用场景】
 * - Pop消息请求处理时获取锁
 * - 缓存清理操作时获取锁
 * - 消费偏移量重置时获取锁
 * - 消费者服务关闭时清理超时锁
 *
 * 【核心调用链路】
 * 1. Pop请求: PopConsumerService.popAsync() -> tryLock() -> unlock()
 * 2. 缓存清理: PopConsumerService.clearCache() -> tryLock() -> unlock()
 * 3. 超时清理: PopConsumerService.run() -> removeTimeout() (定时调用)
 *
 * 【线程安全保证】
 * 使用ConcurrentHashMap + AtomicBoolean实现无锁并发控制
 */
public class PopConsumerLockService {
    /** Pop消费专用日志器 */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    /**
     * 锁超时时间（毫秒）
     * 【用途】超过此时间的锁将被认为是超时锁，会被自动清理
     * 【设置建议】通常设置为2分钟，平衡性能和资源占用
     */
    private final long timeout;

    /**
     * 锁存储表
     * 【Key格式】groupId@topicId
     * 【Value类型】TimedLock（带时间戳的锁）
     * 【并发安全】使用ConcurrentHashMap保证线程安全
     */
    private final ConcurrentMap<String /* groupId@topicId */, TimedLock> lockTable;

    /**
     * 构造函数
     *
     * 【调用链路】
     * PopConsumerService构造函数 -> new PopConsumerLockService(2分钟)
     *
     * @param timeout 锁超时时间（毫秒）
     */
    public PopConsumerLockService(long timeout) {
        this.timeout = timeout;
        this.lockTable = new ConcurrentHashMap<>();
    }

    /**
     * 尝试获取锁 - 非阻塞锁获取
     *
     * 【核心逻辑】
     * 1. 构建锁键：groupId@topicId
     * 2. 如果锁不存在，创建新锁并尝试获取
     * 3. 如果锁已存在，直接尝试获取
     * 4. 获取成功返回true，失败返回false
     *
     * 【调用链路】
     * PopConsumerService.popAsync() -> tryLock() -> 判断是否可以继续处理
     * PopConsumerService.clearCache() -> tryLock() -> 确保缓存清理的原子性
     *
     * 【设计亮点】
     * - 使用computeIfAbsent原子操作，避免竞争条件
     * - 非阻塞设计，失败立即返回，不会阻塞调用线程
     * - 自动创建锁实例，简化使用逻辑
     *
     * 【为什么需要锁】
     * Pop模式下，同一消费组可能有多个消费者同时操作同一Topic，
     * 如果不加锁可能导致：
     * - 缓存和持久化存储数据不一致
     * - 消费偏移量计算错误
     * - 消息重复或丢失
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID
     * @return true表示获取锁成功，false表示锁已被其他线程持有
     */
    public boolean tryLock(String groupId, String topicId) {
        // 构建锁键，使用@分隔符连接groupId和topicId
        String lockKey = groupId + PopAckConstants.SPLIT + topicId;

        // 使用computeIfAbsent原子操作：
        // 1. 如果key不存在，创建新的TimedLock
        // 2. 如果key已存在，直接使用现有的TimedLock
        // 3. 然后调用tryLock()尝试获取锁
        return Objects.requireNonNull(ConcurrentHashMapUtils.computeIfAbsent(lockTable,
            lockKey, s -> new TimedLock())).tryLock();
    }

    /**
     * 释放锁 - 释放指定的消费组和Topic锁
     *
     * 【核心逻辑】
     * 1. 根据groupId和topicId查找对应的锁
     * 2. 如果锁存在，调用unlock()释放
     * 3. 如果锁不存在，什么都不做（防御性编程）
     *
     * 【调用链路】
     * PopConsumerService.popAsync().whenComplete() -> unlock() -> 确保锁被释放
     * PopConsumerService.clearCache().finally -> unlock() -> 清理操作完成后释放锁
     *
     * 【重要性】
     * 必须在操作完成后及时释放锁，否则会导致：
     * - 其他线程无法获取锁，造成阻塞
     * - 锁长时间占用，最终被超时清理机制清除
     *
     * 【防御性设计】
     * 即使锁不存在也不会抛异常，保证程序的健壮性
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID
     */
    public void unlock(String groupId, String topicId) {
        // 构建锁键
        String lockKey = groupId + PopAckConstants.SPLIT + topicId;

        // 查找对应的锁
        TimedLock lock = lockTable.get(lockKey);

        // 防御性检查：如果锁存在才释放
        if (lock != null) {
            lock.unlock();
        }
    }

    /**
     * 检查锁是否超时 - 专门为重试Topic设计的超时检查
     *
     * 【特殊处理】
     * 对于重试Topic，需要检查原始Topic的锁状态，而不是重试Topic本身的锁
     * 这是因为重试消息的处理逻辑与原始消息共享同一把锁
     *
     * 【调用链路】
     * PopConsumerService内部逻辑 -> isLockTimeout() -> 判断是否可以处理重试消息
     *
     * 【设计原理】
     * 重试Topic的命名规则：%RETRY%groupId_topicId 或 %RETRY%groupId%topicId
     * 通过KeyBuilder.parseNormalTopic()可以从重试Topic名称中解析出原始Topic名称
     *
     * 【超时判断逻辑】
     * 1. 锁不存在 -> 认为超时（可以获取新锁）
     * 2. 锁存在但超过超时时间 -> 认为超时
     * 3. 锁存在且未超时 -> 未超时
     *
     * @param groupId 消费组ID
     * @param topicId Topic ID（可能是重试Topic）
     * @return true表示锁超时或不存在，false表示锁未超时
     */
    public boolean isLockTimeout(String groupId, String topicId) {
        // 【关键步骤】解析原始Topic名称
        // 如果是重试Topic，解析出原始Topic；如果是普通Topic，保持不变
        topicId = KeyBuilder.parseNormalTopic(topicId, groupId);

        // 构建锁键并查找锁
        String lockKey = groupId + PopAckConstants.SPLIT + topicId;
        TimedLock lock = lockTable.get(lockKey);

        // 超时判断：锁不存在 或 锁时间超过超时阈值
        return lock == null || System.currentTimeMillis() - lock.getLockTime() > timeout;
    }

    /**
     * 清理超时锁 - 定期维护机制，防止内存泄漏
     *
     * 【清理策略】
     * 1. 遍历所有锁条目
     * 2. 检查每个锁的创建时间
     * 3. 超过超时时间的锁将被移除
     * 4. 记录清理日志便于监控和调试
     *
     * 【调用链路】
     * PopConsumerService.run() -> 每分钟调用一次 -> removeTimeout() -> 清理过期锁
     *
     * 【为什么需要清理】
     * 1. 防止内存泄漏：长时间运行会积累大量锁对象
     * 2. 避免死锁：异常情况下未释放的锁需要被清理
     * 3. 提高性能：减少不必要的锁查找开销
     *
     * 【线程安全】
     * 使用Iterator进行安全遍历和删除，避免ConcurrentModificationException
     *
     * 【清理时机】
     * 通常每分钟调用一次，平衡清理效果和性能开销
     */
    public void removeTimeout() {
        // 获取线程安全的迭代器
        Iterator<Map.Entry<String, TimedLock>> iterator = lockTable.entrySet().iterator();

        // 遍历所有锁条目
        while (iterator.hasNext()) {
            Map.Entry<String, TimedLock> entry = iterator.next();

            // 检查锁是否超时
            if (System.currentTimeMillis() - entry.getValue().getLockTime() > timeout) {
                // 记录清理日志，包含锁键和锁状态信息
                log.info("PopConsumerLockService remove timeout lock, " +
                    "key={}, locked={}", entry.getKey(), entry.getValue().lock.get());

                // 安全删除超时锁
                iterator.remove();
            }
        }
    }

    /**
     * 带时间戳的锁实现 - 支持超时检测的轻量级锁
     *
     * 【设计特点】
     * 1. 使用AtomicBoolean实现无锁并发控制
     * 2. 记录锁获取时间，支持超时检测
     * 3. 非阻塞设计，tryLock失败立即返回
     * 4. 轻量级实现，性能开销小
     *
     * 【与传统锁的区别】
     * - 传统锁：获取失败会阻塞等待
     * - TimedLock：获取失败立即返回，由调用方决定处理策略
     *
     * 【线程安全保证】
     * 使用AtomicBoolean的CAS操作保证原子性，volatile保证可见性
     */
    static class TimedLock {
        /**
         * 锁获取时间戳
         * 【用途】用于超时检测和清理判断
         * 【可见性】使用volatile保证多线程间的可见性
         */
        private volatile long lockTime;

        /**
         * 锁状态标识
         * 【状态】false=未锁定，true=已锁定
         * 【原子性】使用AtomicBoolean保证CAS操作的原子性
         */
        private final AtomicBoolean lock;

        /**
         * 构造函数 - 初始化锁为未锁定状态
         *
         * 【初始状态】
         * - lockTime: 当前时间戳（用于后续超时计算）
         * - lock: false（未锁定状态）
         */
        public TimedLock() {
            this.lockTime = System.currentTimeMillis();
            this.lock = new AtomicBoolean(false);
        }

        /**
         * 尝试获取锁 - 非阻塞锁获取
         *
         * 【CAS操作】
         * compareAndSet(false, true)：
         * - 如果当前值是false，设置为true并返回true（获取成功）
         * - 如果当前值是true，不做任何操作并返回false（获取失败）
         *
         * 【时间戳更新】
         * 只有在成功获取锁时才更新lockTime，确保时间戳的准确性
         *
         * 【原子性保证】
         * CAS操作保证了检查和设置的原子性，避免竞争条件
         *
         * @return true表示获取锁成功，false表示锁已被持有
         */
        public boolean tryLock() {
            // 使用CAS操作尝试将锁状态从false改为true
            if (lock.compareAndSet(false, true)) {
                // 获取成功，更新锁时间戳
                this.lockTime = System.currentTimeMillis();
                return true;
            }
            // 获取失败，锁已被其他线程持有
            return false;
        }

        /**
         * 释放锁 - 直接设置锁状态为未锁定
         *
         * 【简单释放】
         * 直接设置为false，不需要CAS操作
         * 因为只有持有锁的线程才会调用unlock()
         *
         * 【注意事项】
         * 不更新lockTime，保持获取锁时的时间戳
         * 这样可以准确计算锁的持有时间
         */
        public void unlock() {
            lock.set(false);
        }

        /**
         * 获取锁时间戳 - 用于超时检测
         *
         * 【用途】
         * - 超时检测：计算锁持有时间
         * - 清理判断：确定是否需要清理超时锁
         * - 监控统计：跟踪锁的使用情况
         *
         * @return 锁获取时的时间戳
         */
        public long getLockTime() {
            return lockTime;
        }
    }
}
