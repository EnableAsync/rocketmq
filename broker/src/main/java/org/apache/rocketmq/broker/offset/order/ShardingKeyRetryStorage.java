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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/**
 * ShardingKey 重试次数持久化存储
 * 基于 RocksDB 实现高性能的重试次数持久化
 */
public class ShardingKeyRetryStorage {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final String dbPath;
    private RocksDB db;
    private Options options;
    private WriteOptions writeOptions;

    // 内存缓存，减少 RocksDB 读取频率
    private final ConcurrentHashMap<String, Integer> retryTimesCache = new ConcurrentHashMap<>();

    public ShardingKeyRetryStorage(String storePathRootDir) {
        this.dbPath = storePathRootDir + File.separator + "kvStore" + File.separator + "shardingKeyRetry";
    }

    public boolean load() {
        try {
            UtilAll.ensureDirOK(dbPath);

            RocksDB.loadLibrary();
            options = new Options();
            options.setCreateIfMissing(true);
            options.setMaxOpenFiles(-1);
            options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
            options.setMaxWriteBufferNumber(3);
            options.setMaxBackgroundCompactions(10);
            options.setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);

            writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(false);

            db = RocksDB.open(options, dbPath);

            log.debug("ShardingKeyRetryStorage started successfully, dbPath: {}", dbPath);
            return true;
        } catch (Exception e) {
            log.error("Failed to start ShardingKeyRetryStorage, dbPath: {}", dbPath, e);
            return false;
        }
    }

    public void shutdown() {
        try {
            if (writeOptions != null) {
                writeOptions.close();
            }
            if (db != null) {
                db.close();
            }
            if (options != null) {
                options.close();
            }

            retryTimesCache.clear();
            log.debug("ShardingKeyRetryStorage shutdown successfully");
        } catch (Exception e) {
            log.error("Failed to shutdown ShardingKeyRetryStorage", e);
        }
    }

    private String buildKey(String topic, String group, int queueId, String shardingKey) {
        return topic + "@" + group + "@" + queueId + "@" + shardingKey;
    }

    /**
     * 获取重试次数
     * 优先从缓存读取，缓存未命中则从 RocksDB 读取
     */
    public int getRetryTimes(String topic, String group, int queueId, String shardingKey) {
        String key = buildKey(topic, group, queueId, shardingKey);

        Integer cachedValue = retryTimesCache.get(key);
        if (cachedValue != null) {
            log.debug("从缓存中读出重试次数: key={}, retryTimes={}", key, cachedValue);
            return cachedValue;
        }

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = db.get(keyBytes);

            if (valueBytes != null && valueBytes.length >= 4) {
                int retryTimes = ByteBuffer.wrap(valueBytes).getInt();
                log.debug("从 rocksdb 中读出重试次数: key={}, retryTimes={}", key, retryTimes);
                retryTimesCache.put(key, retryTimes);
                return retryTimes;
            }
        } catch (RocksDBException e) {
            log.warn("Failed to get retry times from RocksDB, key: {}, fallback to 0", key, e);
        }

        return 0;
    }

    /**
     * 设置重试次数
     * 同时更新缓存和 RocksDB
     */
    public void setRetryTimes(String topic, String group, int queueId, String shardingKey, int retryTimes) {
        String key = buildKey(topic, group, queueId, shardingKey);

        try {
            retryTimesCache.put(key, retryTimes);

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = ByteBuffer.allocate(4).putInt(retryTimes).array();

            db.put(writeOptions, keyBytes, valueBytes);

            log.debug("Set retry times: key={}, retryTimes={}", key, retryTimes);
        } catch (RocksDBException e) {
            log.warn("Failed to set retry times to RocksDB, key: {}, retryTimes: {}", key, retryTimes, e);
        }
    }

    public int incrementRetryTimes(String topic, String group, int queueId, String shardingKey) {
        int currentRetryTimes = getRetryTimes(topic, group, queueId, shardingKey);
        int newRetryTimes = currentRetryTimes + 1;
        setRetryTimes(topic, group, queueId, shardingKey, newRetryTimes);
        return newRetryTimes;
    }

    /**
     * 删除重试次数记录
     * 在锁完全释放时调用
     */
    public void removeRetryTimes(String topic, String group, int queueId, String shardingKey) {
        String key = buildKey(topic, group, queueId, shardingKey);

        try {
            retryTimesCache.remove(key);

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            db.delete(writeOptions, keyBytes);

            log.debug("Removed retry times: key={}", key);
        } catch (RocksDBException e) {
            log.warn("Failed to remove retry times from RocksDB, key: {}", key, e);
        }
    }

    /**
     * 获取缓存统计信息
     */
    public int getCacheSize() {
        return retryTimesCache.size();
    }

    public void clearCache() {
        retryTimesCache.clear();
        log.info("ShardingKeyRetryStorage cache cleared");
    }
}