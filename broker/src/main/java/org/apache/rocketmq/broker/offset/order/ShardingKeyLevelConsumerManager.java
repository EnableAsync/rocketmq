package org.apache.rocketmq.broker.offset.order;

import java.util.List;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.store.GetMessageResult;

public class ShardingKeyLevelConsumerManager implements OrderedConsumptionManager {
    @Override
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime,
        long invisibleTime, List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder,
        GetMessageResult getMessageResult) {

    }

    @Override
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        return false;
    }

    @Override
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        return 0;
    }

    @Override
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {

    }

    @Override
    public void clearBlock(String topic, String group, int queueId) {

    }

    @Override
    public OrderedConsumptionLevel getOrderedConsumptionLevel() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void persist() {

    }

    @Override
    public boolean load() {
        return false;
    }
}