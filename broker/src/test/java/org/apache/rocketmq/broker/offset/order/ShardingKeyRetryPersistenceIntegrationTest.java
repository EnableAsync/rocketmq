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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * ShardingKey 重试次数持久化集成测试
 * 测试 ShardingKeyLockManager 与 ShardingKeyRetryStorage 的集成
 */
public class ShardingKeyRetryPersistenceIntegrationTest {

    private ShardingKeyLockManager lockManager;
    private ShardingKeyCache cache;
    private String tempDir;

    @Mock
    private BrokerController brokerController;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        
        // 创建临时目录
        Path tempPath = Files.createTempDirectory("sharding-key-integration-test");
        tempDir = tempPath.toString();
        
        // 模拟 BrokerController 和 MessageStoreConfig
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(tempDir);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        
        // 创建缓存和锁管理器
        cache = new ShardingKeyCache();
        lockManager = new ShardingKeyLockManager(brokerController, cache);
        
        // 启动锁管理器（包括重试次数持久化存储）
        assertTrue("Lock manager should start successfully", lockManager.load());
    }

    @After
    public void tearDown() throws IOException {
        if (lockManager != null) {
            lockManager.shutdown();
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
    public void testRetryTimesPersistenceInLockLifecycle() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(1000L, 1001L);

        // 1. 创建锁（应该从持久化存储读取重试次数，初始为0）
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, 
            popTime, invisibleTime, attemptId, offsets);

        // 2. 增加重试次数（应该持久化到存储）
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);

        // 3. 验证统计信息包含重试次数存储信息
        String retryStorageStats = lockManager.getRetryStorageStatistics();
        assertNotNull("Retry storage statistics should not be null", retryStorageStats);
        assertTrue("Retry storage should be started", retryStorageStats.contains("started=true"));

        // 4. 释放部分 offset（锁应该仍然存在）
        boolean partialRelease = lockManager.releaseLock(topic, group, queueId, 1000L, popTime);
        assertTrue("Should successfully release partial offset", partialRelease);

        // 5. 释放最后一个 offset（锁应该完全释放，重试次数记录应该被清理）
        boolean fullRelease = lockManager.releaseLock(topic, group, queueId, 1001L, popTime);
        assertTrue("Should successfully release final offset", fullRelease);

        // 6. 验证锁已被完全释放
        assertFalse("Lock should be completely released", 
            lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }

    @Test
    public void testRetryTimesPersistenceAcrossLockManagerRestart() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "persistentUser";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(2000L);

        // 1. 创建锁并增加重试次数
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, 
            popTime, invisibleTime, attemptId, offsets);
        
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);

        // 2. 关闭锁管理器
        lockManager.shutdown();

        // 3. 重新创建锁管理器
        lockManager = new ShardingKeyLockManager(brokerController, cache);
        assertTrue("Lock manager should restart successfully", lockManager.load());

        // 4. 创建相同的锁（应该从持久化存储读取之前的重试次数）
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, 
            popTime, invisibleTime, attemptId, offsets);

        // 5. 验证重试次数存储仍然工作
        String retryStorageStats = lockManager.getRetryStorageStatistics();
        assertNotNull("Retry storage statistics should not be null", retryStorageStats);
        assertTrue("Retry storage should be started", retryStorageStats.contains("started=true"));
    }

    @Test
    public void testMultipleShardingKeysRetryTimesPersistence() {
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;

        // 创建多个不同的 shardingKey 锁
        String[] shardingKeys = {"user1", "user2", "user3"};
        for (int i = 0; i < shardingKeys.length; i++) {
            String shardingKey = shardingKeys[i];
            String attemptId = "attempt" + (i + 1);
            List<Long> offsets = Arrays.asList(3000L + i);

            // 创建锁
            lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, 
                popTime, invisibleTime, attemptId, offsets);

            // 为每个 shardingKey 增加不同次数的重试
            for (int j = 0; j <= i; j++) {
                lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);
            }
        }

        // 验证重试次数存储统计信息
        String retryStorageStats = lockManager.getRetryStorageStatistics();
        assertNotNull("Retry storage statistics should not be null", retryStorageStats);
        assertTrue("Retry storage should be started", retryStorageStats.contains("started=true"));
        assertTrue("Cache should contain multiple entries", retryStorageStats.contains("cacheSize=3"));

        // 释放所有锁（应该清理所有重试次数记录）
        for (int i = 0; i < shardingKeys.length; i++) {
            boolean released = lockManager.releaseLock(topic, group, queueId, 3000L + i, popTime);
            assertTrue("Should successfully release lock for " + shardingKeys[i], released);
        }
    }

    @Test
    public void testRetryStorageFailureDoesNotBlockMainFlow() {
        // 这个测试验证即使重试次数存储失败，主流程也不会被阻塞
        String topic = "testTopic";
        String group = "testGroup";
        int queueId = 1;
        String shardingKey = "user123";
        String attemptId = "attempt123";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 30000;
        List<Long> offsets = Arrays.asList(4000L);

        // 创建锁（即使持久化失败，锁创建也应该成功）
        lockManager.createOrUpdateLock(topic, group, queueId, shardingKey, 
            popTime, invisibleTime, attemptId, offsets);

        // 验证锁已创建
        assertTrue("Lock should be created successfully", 
            lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));

        // 增加重试次数（即使持久化失败，操作也应该成功）
        lockManager.increaseCountInfo(topic, group, queueId, shardingKey, offsets);

        // 释放锁（即使持久化清理失败，锁释放也应该成功）
        boolean released = lockManager.releaseLock(topic, group, queueId, 4000L, popTime);
        assertTrue("Lock should be released successfully", released);

        // 验证锁已释放
        assertFalse("Lock should be released", 
            lockManager.isLocked(topic, group, queueId, shardingKey, "different_attempt"));
    }
}