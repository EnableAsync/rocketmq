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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * ShardingKeyRetryStorage 单元测试类
 */
public class ShardingKeyRetryStorageTest {

    private ShardingKeyRetryStorage retryStorage;
    private String tempDir;

    @Before
    public void setUp() throws IOException {
        // 创建临时目录
        Path tempPath = Files.createTempDirectory("sharding-key-retry-test");
        tempDir = tempPath.toString();
        
        retryStorage = new ShardingKeyRetryStorage(tempDir);
        assertTrue("Storage should start successfully", retryStorage.load());
    }

    @After
    public void tearDown() throws IOException {
        if (retryStorage != null) {
            retryStorage.shutdown();
        }
        
        // 清理临时目录
        if (tempDir != null) {
            Path tempPath = new File(tempDir).toPath();
            if (Files.exists(tempPath)) {
                Files.walk(tempPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }
    }

    @Test
    public void testBasicOperations() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";

        // 测试初始值
        int initialRetryTimes = retryStorage.getRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Initial retry times should be 0", 0, initialRetryTimes);

        // 测试设置重试次数
        retryStorage.setRetryTimes(topic, group, queueId, shardingKey, 3);
        int retrievedRetryTimes = retryStorage.getRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Retrieved retry times should match set value", 3, retrievedRetryTimes);

        // 测试增加重试次数
        int newRetryTimes = retryStorage.incrementRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Incremented retry times should be 4", 4, newRetryTimes);

        // 验证增加后的值
        int finalRetryTimes = retryStorage.getRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Final retry times should be 4", 4, finalRetryTimes);
    }

    @Test
    public void testRemoveRetryTimes() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user456";

        // 设置重试次数
        retryStorage.setRetryTimes(topic, group, queueId, shardingKey, 5);
        assertEquals("Retry times should be set", 5, 
            retryStorage.getRetryTimes(topic, group, queueId, shardingKey));

        // 删除重试次数记录
        retryStorage.removeRetryTimes(topic, group, queueId, shardingKey);
        
        // 验证删除后返回0
        int retryTimesAfterRemoval = retryStorage.getRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Retry times should be 0 after removal", 0, retryTimesAfterRemoval);
    }

    @Test
    public void testMultipleShardingKeys() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 测试多个不同的 shardingKey
        retryStorage.setRetryTimes(topic, group, queueId, "user1", 1);
        retryStorage.setRetryTimes(topic, group, queueId, "user2", 2);
        retryStorage.setRetryTimes(topic, group, queueId, "user3", 3);

        // 验证每个 shardingKey 的重试次数
        assertEquals("user1 retry times should be 1", 1, 
            retryStorage.getRetryTimes(topic, group, queueId, "user1"));
        assertEquals("user2 retry times should be 2", 2, 
            retryStorage.getRetryTimes(topic, group, queueId, "user2"));
        assertEquals("user3 retry times should be 3", 3, 
            retryStorage.getRetryTimes(topic, group, queueId, "user3"));
    }

    @Test
    public void testPersistenceAcrossRestart() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "persistentUser";

        // 设置重试次数
        retryStorage.setRetryTimes(topic, group, queueId, shardingKey, 10);
        assertEquals("Retry times should be set", 10, 
            retryStorage.getRetryTimes(topic, group, queueId, shardingKey));

        // 关闭并重新启动存储
        retryStorage.shutdown();
        retryStorage = new ShardingKeyRetryStorage(tempDir);
        assertTrue("Storage should restart successfully", retryStorage.load());

        // 验证数据持久化
        int persistedRetryTimes = retryStorage.getRetryTimes(topic, group, queueId, shardingKey);
        assertEquals("Retry times should persist across restart", 10, persistedRetryTimes);
    }

    @Test
    public void testCacheStatistics() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;

        // 初始缓存大小应该为0
        assertEquals("Initial cache size should be 0", 0, retryStorage.getCacheSize());

        // 设置一些重试次数
        retryStorage.setRetryTimes(topic, group, queueId, "user1", 1);
        retryStorage.setRetryTimes(topic, group, queueId, "user2", 2);

        // 缓存大小应该增加
        assertTrue("Cache size should be greater than 0", retryStorage.getCacheSize() > 0);

        // 清理缓存
        retryStorage.clearCache();
        assertEquals("Cache size should be 0 after clear", 0, retryStorage.getCacheSize());

        // 验证清理缓存后仍能从持久化存储读取
        int retryTimes = retryStorage.getRetryTimes(topic, group, queueId, "user1");
        assertEquals("Should still be able to read from persistent storage", 1, retryTimes);
    }

    @Test
    public void testStorageNotStarted() {
        // 创建未启动的存储
        ShardingKeyRetryStorage notStartedStorage = new ShardingKeyRetryStorage(tempDir + "_not_started");
        
        // 未启动的存储应该返回默认值
        int retryTimes = notStartedStorage.getRetryTimes("topic", "group", 1, "key");
        assertEquals("Not started storage should return 0", 0, retryTimes);
        
        // 设置操作应该不会崩溃
        notStartedStorage.setRetryTimes("topic", "group", 1, "key", 5);
        
        // 删除操作应该不会崩溃
        notStartedStorage.removeRetryTimes("topic", "group", 1, "key");
    }
}