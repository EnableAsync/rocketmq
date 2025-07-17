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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList;
    private final List<ByteBuffer> messageBufferList;
    private final List<Long> messageQueueOffset;

    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;

    private int messageCount = 0;

    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;
    private int commercialSizePerMsg = 4 * 1024;

    private long coldDataSum = 0L;

    private int filterMessageCount;

    public static final GetMessageResult NO_MATCH_LOGIC_QUEUE =
        new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList());

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
        messageQueueOffset = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMapedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
        messageQueueOffset = new ArrayList<>(resultSize);
    }

    private GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset,
        List<SelectMappedBufferResult> messageMapedList, List<ByteBuffer> messageBufferList, List<Long> messageQueueOffset) {
        this.status = status;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messageMapedList = messageMapedList;
        this.messageBufferList = messageBufferList;
        this.messageQueueOffset = messageQueueOffset;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
        this.messageQueueOffset.add(queueOffset);
    }


    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset, final int batchNum) {
        addMessage(mapedBuffer, queueOffset);
        messageCount += batchNum - 1;
    }

    /**
     * 根据一个 index 列表，同步地从多个列表中移除多个 index 元素。
     * @param indexList 要删除的 list
     */
    public void removeMessageByIndexList(List<Integer> indexList) {
        if (indexList == null || indexList.isEmpty()) {
            return;
        }

        // 1. 先对索引列表进行降序排序。
        // 这样可以确保从后往前删除，避免因元素移除导致的前方元素索引变化问题。
        // 例如，删除 index=5 的元素，不会影响 index=3 的元素的位置。
        indexList.sort(Collections.reverseOrder());

        for (int index : indexList) {
            // 检查索引有效性，增加代码健壮性
            if (index < 0 || index >= this.messageBufferList.size()) {
                // 可以选择记录日志或忽略无效索引
                // log.warn("Invalid index {} found in indexList, skipping.", index);
                continue;
            }

            // 2. 按降序索引安全地删除元素
            this.messageBufferList.remove(index);
            SelectMappedBufferResult buffer = this.messageMapedList.remove(index);

            // 3. 更新统计数据和释放资源（这部分逻辑本身是正确的）
            this.bufferTotalSize -= buffer.getSize();
            this.msgCount4Commercial -= (int) Math.ceil(
                buffer.getSize() / (double) commercialSizePerMsg);
            this.messageCount--;
            buffer.release();
        }

        if (this.messageBufferList.isEmpty()) {
            this.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
        }
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    public List<Long> getMessageQueueOffset() {
        return messageQueueOffset;
    }

    public long getColdDataSum() {
        return coldDataSum;
    }

    public void setColdDataSum(long coldDataSum) {
        this.coldDataSum = coldDataSum;
    }

    public int getFilterMessageCount() {
        return filterMessageCount;
    }

    public void setFilterMessageCount(int filterMessageCount) {
        this.filterMessageCount = filterMessageCount;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize + ", messageCount=" + messageCount
            + ", filterMessageCount=" + filterMessageCount + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}
