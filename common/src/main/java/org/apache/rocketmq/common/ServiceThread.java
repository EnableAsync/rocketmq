package org.apache.rocketmq.common;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * ServiceThread 类是一个抽象类，提供线程服务的基础实现
 * 在RocketMQ中作为后台服务线程的通用父类，用于处理定期执行的任务
 */
public abstract class ServiceThread implements Runnable {
    // 日志记录器，使用RocketMQ公共日志名称
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // 线程加入等待的最大时间，90秒
    private static final long JOIN_TIME = 90 * 1000;

    // 服务线程实例
    protected Thread thread;

    // 等待点，用于线程等待和唤醒机制
    // CountDownLatch2是RocketMQ中对CountDownLatch的扩展，可以重置使用
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    // 表示是否已经通知过线程，防止重复通知
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    // 表示线程是否已停止
    protected volatile boolean stopped = false;

    // 标识线程是否为守护线程
    protected boolean isDaemon = false;

    // 表示线程是否已启动，使用原子变量保证线程安全
    // 使线程能够重新启动
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 默认构造函数
     */
    public ServiceThread() {
    }

    /**
     * 抽象方法，由子类实现，用于获取服务名称
     * 通常用于命名线程，便于调试和日志分析
     */
    public abstract String getServiceName();

    /**
     * 启动服务线程
     * 调用链路：外部代码 -> start() -> 创建并启动新线程 -> run()（子类实现）
     */
    public void start() {
        // 记录启动尝试的日志
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);

        // CAS操作确保线程只被启动一次
        if (!started.compareAndSet(false, true)) {
            return;
        }

        // 重置停止标志
        stopped = false;

        // 创建并启动线程
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();

        // 记录启动成功的日志
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    /**
     * 关闭服务线程，默认不中断
     * 调用链路：外部代码 -> shutdown() -> shutdown(false)
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务线程
     * 调用链路：外部代码 -> shutdown(interrupt) -> wakeup() -> (可能的)thread.interrupt() -> 等待线程结束
     *
     * @param interrupt 是否中断线程
     */
    public void shutdown(final boolean interrupt) {
        // 记录关闭尝试的日志
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);

        // CAS操作确保线程只被关闭一次
        if (!started.compareAndSet(true, false)) {
            return;
        }

        // 设置停止标志
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        // 唤醒可能处于等待状态的线程
        wakeup();

        try {
            // 如果需要，中断线程
            if (interrupt) {
                this.thread.interrupt();
            }

            // 记录开始等待线程结束的时间
            long beginTime = System.currentTimeMillis();

            // 如果不是守护线程，则等待线程结束
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }

            // 记录等待时间
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    /**
     * 获取等待线程结束的超时时间
     * 可被子类重写以自定义等待时间
     *
     * @return 等待时间，毫秒
     */
    public long getJoinTime() {
        return JOIN_TIME;
    }

    /**
     * 设置停止标志，但不主动关闭线程
     * 线程将在下一次检查stopped标志时自行退出
     * 调用链路：外部代码 -> makeStop()
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    /**
     * 唤醒等待中的线程
     * 调用链路：外部代码 -> wakeup() -> waitPoint.countDown()
     */
    public void wakeup() {
        // CAS操作确保只通知一次
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // 通知等待的线程
        }
    }

    /**
     * 使当前线程等待指定的时间，或者直到被唤醒
     * 调用链路：子类run方法 -> waitForRunning() -> waitPoint.await()
     *
     * @param interval 最长等待时间，毫秒
     */
    protected void waitForRunning(long interval) {
        // 如果已经被通知，则直接返回
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        // 重置等待点，准备进入等待状态
        waitPoint.reset();
        try {
            // 等待指定时间或被唤醒
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 无论如何，重置通知标志，并调用等待结束回调
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    /**
     * 等待结束后的回调方法，可由子类重写以实现自定义逻辑
     * 调用链路：waitForRunning() -> onWaitEnd()
     */
    protected void onWaitEnd() {
        // 默认为空实现，子类可重写
    }

    /**
     * 检查线程是否已停止
     *
     * @return 如果已停止，返回true
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * 检查线程是否为守护线程
     *
     * @return 如果是守护线程，返回true
     */
    public boolean isDaemon() {
        return isDaemon;
    }

    /**
     * 设置线程的守护状态
     *
     * @param daemon 如果为true，线程将设为守护线程
     */
    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
