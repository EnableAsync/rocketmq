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
package org.apache.rocketmq.broker.offset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 消费者顺序信息锁管理器
 *
 * 核心功能：
 * 1. 管理POP消费模式下顺序消息的锁释放时机
 * 2. 当锁释放时，通知等待的长轮询请求继续处理
 * 3. 使用时间轮定时器实现精确的锁释放时间控制
 *
 * 业务场景：
 * 在POP消费模式下，为了保证顺序消费，当前面的消息还未被ACK时，
 * 后续的消息会被阻塞。这个管理器负责在消息的不可见时间到期后，
 * 通知系统可以继续处理该队列的消息。
 */
public class ConsumerOrderInfoLockManager {
    // POP消费专用日志记录器
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    // Broker控制器引用，用于获取配置和调用其他组件
    private final BrokerController brokerController;

    /**
     * 超时任务映射表
     * Key: 队列标识符（topic + group + queueId）
     * Value: Netty的Timeout对象，代表一个定时任务
     *
     * 作用：跟踪每个队列的锁释放定时任务
     */
    private final Map<Key, Timeout> timeoutMap = new ConcurrentHashMap<>();

    /**
     * Netty的时间轮定时器
     * 优势：
     * 1. 高性能：O(1)时间复杂度的任务调度
     * 2. 内存友好：相比传统定时器，内存占用更少
     * 3. 适合大量定时任务的场景
     */
    private final Timer timer;

    // 时间轮的tick间隔，100毫秒
    private static final int TIMER_TICK_MS = 100;

    /**
     * 构造函数
     * @param brokerController Broker控制器
     */
    public ConsumerOrderInfoLockManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        // 初始化时间轮定时器
        this.timer = new HashedWheelTimer(
            new ThreadFactoryImpl("ConsumerOrderInfoLockManager_"), // 自定义线程工厂，便于线程命名和监控
            TIMER_TICK_MS, TimeUnit.MILLISECONDS); // 100ms的tick间隔
    }

    /**
     * 恢复方法：当ConsumerOrderInfoManager从磁盘加载数据时调用
     *
     * 目的：Broker重启后，需要根据持久化的数据重新设置定时任务
     * 确保之前未完成的锁释放通知能够正常执行
     *
     * @param table 从磁盘加载的顺序信息表
     *
     * 调用链路：
     * 1. Broker启动 ->
     * 2. ConfigManager.load() ->
     * 3. ConsumerOrderInfoManager.decode() ->
     * 4. ConsumerOrderInfoLockManager.recover()
     */
    public void recover(Map<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo>> table) {
        // 检查是否启用了锁释放后通知功能
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }

        // 遍历所有的topic@group组合
        for (Map.Entry<String, ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo>> entry : table.entrySet()) {
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = entry.getValue();

            // 解析topic和group
            String[] arrays = ConsumerOrderInfoManager.decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];

            // 遍历每个队列的顺序信息
            for (Map.Entry<Integer, ConsumerOrderInfoManager.OrderInfo> qsEntry : qs.entrySet()) {
                Long lockFreeTimestamp = qsEntry.getValue().getLockFreeTimestamp();

                // 只有锁释放时间在未来的才需要设置定时任务
                if (lockFreeTimestamp == null || lockFreeTimestamp <= System.currentTimeMillis()) {
                    continue;
                }

                // 重新设置锁释放定时任务
                this.updateLockFreeTimestamp(topic, group, qsEntry.getKey(), lockFreeTimestamp);
            }
        }
    }

    /**
     * 更新锁释放时间戳（重载方法1）
     * 从OrderInfo对象中获取锁释放时间戳
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param orderInfo 顺序信息对象
     *
     * 调用链路：
     * 1. ConsumerOrderInfoManager.update() ->
     * 2. ConsumerOrderInfoManager.updateLockFreeTimestamp() ->
     * 3. ConsumerOrderInfoLockManager.updateLockFreeTimestamp()
     */
    public void updateLockFreeTimestamp(String topic, String group, int queueId, ConsumerOrderInfoManager.OrderInfo orderInfo) {
        this.updateLockFreeTimestamp(topic, group, queueId, orderInfo.getLockFreeTimestamp());
    }

    /**
     * 更新锁释放时间戳（重载方法2）
     * 核心方法：设置或更新队列的锁释放定时任务
     *
     * @param topic 主题名称
     * @param group 消费者组名称
     * @param queueId 队列ID
     * @param lockFreeTimestamp 锁释放时间戳
     *
     * 业务逻辑：
     * 1. 计算延迟时间 = 锁释放时间 - 当前时间
     * 2. 创建定时任务，在延迟时间后执行通知
     * 3. 如果已存在旧的定时任务，则取消旧任务
     * 4. 将新任务加入时间轮
     */
    public void updateLockFreeTimestamp(String topic, String group, int queueId, Long lockFreeTimestamp) {
        // 检查功能开关
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }

        // 锁释放时间戳为空，无需设置定时任务
        if (lockFreeTimestamp == null) {
            return;
        }

        try {
            // 使用compute方法原子性地更新timeout映射
            // 这样可以确保同一个Key的定时任务更新是线程安全的
            this.timeoutMap.compute(new Key(topic, group, queueId), (key, oldTimeout) -> {
                try {
                    // 计算延迟时间：锁释放时间 - 当前时间
                    long delay = lockFreeTimestamp - System.currentTimeMillis();

                    // 创建新的定时任务
                    Timeout newTimeout = this.timer.newTimeout(
                        new NotifyLockFreeTimerTask(key), // 定时任务实现
                        delay,                           // 延迟时间
                        TimeUnit.MILLISECONDS           // 时间单位
                    );

                    // 如果存在旧的定时任务，取消它
                    if (oldTimeout != null) {
                        oldTimeout.cancel();
                    }

                    return newTimeout; // 返回新的定时任务
                } catch (Exception e) {
                    POP_LOGGER.warn("add timeout task failed. key:{}, lockFreeTimestamp:{}", key, lockFreeTimestamp, e);
                    return oldTimeout; // 出错时保持原有任务
                }
            });
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when updateLockFreeTimestamp. topic:{}, group:{}, queueId:{}, lockFreeTimestamp:{}",
                topic, group, queueId, lockFreeTimestamp, e);
        }
    }

    /**
     * 通知锁已释放
     * 当定时任务触发时调用此方法
     *
     * @param key 队列标识符
     *
     * 作用：通知PopMessageProcessor有新的消息可以被消费
     * 这会唤醒等待在该队列上的长轮询请求
     */
    protected void notifyLockIsFree(Key key) {
        try {
            // 调用PopMessageProcessor的通知方法
            // 这会检查是否有长轮询请求在等待该队列的消息
            this.brokerController.getPopMessageProcessor().notifyLongPollingRequestIfNeed(key.topic, key.group, key.queueId);
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when notifyLockIsFree. key:{}", key, e);
        }
    }

    /**
     * 关闭管理器
     * 停止时间轮定时器，释放资源
     *
     * 调用时机：Broker关闭时
     */
    public void shutdown() {
        this.timer.stop();
    }

    /**
     * 测试方法：获取超时任务映射
     * 仅用于单元测试
     */
    @VisibleForTesting
    protected Map<Key, Timeout> getTimeoutMap() {
        return timeoutMap;
    }

    /**
     * 锁释放通知定时任务
     * 实现TimerTask接口，当定时器触发时执行run方法
     */
    private class NotifyLockFreeTimerTask implements TimerTask {
        private final Key key; // 要通知的队列标识

        private NotifyLockFreeTimerTask(Key key) {
            this.key = key;
        }

        /**
         * 定时任务执行方法
         *
         * @param timeout 超时对象
         * <p>
         * 执行逻辑：
         * 1. 检查任务是否被取消
         * 2. 检查功能开关是否开启
         * 3. 执行锁释放通知
         * 4. 从映射表中清理已完成的任务
         */
        @Override
        public void run(Timeout timeout) throws Exception {
            // 检查任务状态和功能开关
            if (timeout.isCancelled() || !brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
                return;
            }

            // 执行锁释放通知
            notifyLockIsFree(key);

            // 从映射表中移除已完成的任务
            // 使用computeIfPresent确保线程安全
            timeoutMap.computeIfPresent(key, (key1, curTimeout) -> {
                if (curTimeout == timeout) {
                    // 如果是当前任务，则从map中移除
                    return null;
                }
                // 如果不是当前任务（可能已被新任务替换），保持不变
                return curTimeout;
            });
        }
    }

    /**
     * 队列标识符类
     * 用作timeoutMap的键，唯一标识一个消费队列
     *
     * 组成：topic + group + queueId
     * 这三个字段组合可以唯一确定一个消费队列
     */
    private static class Key {
        private final String topic;   // 主题名称
        private final String group;   // 消费者组名称
        private final int queueId;    // 队列ID

        public Key(String topic, String group, int queueId) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
        }

        /**
         * 重写equals方法
         * 用于HashMap/ConcurrentHashMap的键比较
         * 只有三个字段都相等时，两个Key才相等
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return queueId == key.queueId &&
                Objects.equal(topic, key.topic) &&
                Objects.equal(group, key.group);
        }

        /**
         * 重写hashCode方法
         * 用于HashMap/ConcurrentHashMap的哈希计算
         * 确保相等的对象有相同的哈希码
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(topic, group, queueId);
        }

        /**
         * 重写toString方法
         * 便于日志记录和调试
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("group", group)
                .add("queueId", queueId)
                .toString();
        }
    }
}
