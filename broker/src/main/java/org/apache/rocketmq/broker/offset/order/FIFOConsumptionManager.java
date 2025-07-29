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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.OrderedConsumptionLevel;
import org.apache.rocketmq.store.GetMessageResult;

public class FIFOConsumptionManager {
    private final OrderedConsumptionManager orderedConsumptionManager;

    public FIFOConsumptionManager() {
        this.orderedConsumptionManager = new QueueLevelConsumerManager();
    }

    public FIFOConsumptionManager(BrokerController brokerController) {
        if (brokerController.getBrokerConfig().getOrderedConsumptionLevel() == OrderedConsumptionLevel.SHARDING_KEY) {
            this.orderedConsumptionManager = new ShardingKeyLevelConsumerManager(brokerController);
        } else {
            this.orderedConsumptionManager = new QueueLevelConsumerManager(brokerController);
        }
    }

    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime,
        long invisibleTime, List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder, GetMessageResult getMessageResult) {
        this.orderedConsumptionManager.update(attemptId, isRetry, topic, group, queueId, popTime, invisibleTime, msgQueueOffsetList, orderInfoBuilder, getMessageResult);
    }

    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        return this.orderedConsumptionManager.checkBlock(attemptId, topic, group, queueId, invisibleTime);
    }

    public void clearBlock(String topic, String group, int queueId) {
        this.orderedConsumptionManager.clearBlock(topic, group, queueId);
    }

    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        return this.orderedConsumptionManager.commitAndNext(topic, group, queueId, queueOffset, popTime);
    }

    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime,
        long nextVisibleTime) {
        this.orderedConsumptionManager.updateNextVisibleTime(topic, group, queueId, queueOffset, popTime, nextVisibleTime);
    }

    public void persist() {
        this.orderedConsumptionManager.persist();
    }

    public boolean load() {
        return this.orderedConsumptionManager.load();
    }

    public void shutdown() {
        this.orderedConsumptionManager.shutdown();
    }

}
