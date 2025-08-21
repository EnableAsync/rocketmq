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
    private volatile boolean started = false;
    
    // 内存缓存，减少 RocksDB 读取频率
    private final ConcurrentHashMap<String, Integer> retryTimesCache = new ConcurrentHashMap<>();
    
    public ShardingKeyRetryStorage(String storePathRootDir) {
        this.dbPath = storePathRootDir + File.separator + "kvStore" + File.separator + "shardingKeyRetry";
    }
    
    /**
     * 启动存储
     */
    public boolean load() {
        if (started) {
            return true;
        }
        
        try {
            // 确保目录存在
            UtilAll.ensureDirOK(dbPath);
            
            // 初始化 RocksDB 选项
            RocksDB.loadLibrary();
            options = new Options();
            options.setCreateIfMissing(true);
            options.setMaxOpenFiles(-1);
            options.setWriteBufferSize(64 * 1024 * 1024); // 64MB
            options.setMaxWriteBufferNumber(3);
            options.setMaxBackgroundCompactions(10);
            options.setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);
            
            writeOptions = new WriteOptions();
            writeOptions.setSync(false); // 异步写入，提高性能
            writeOptions.setDisableWAL(false); // 启用 WAL 保证数据安全
            
            // 打开数据库
            db = RocksDB.open(options, dbPath);
            
            started = true;
            log.info("ShardingKeyRetryStorage started successfully, dbPath: {}", dbPath);
            return true;
        } catch (Exception e) {
            log.error("Failed to start ShardingKeyRetryStorage, dbPath: {}", dbPath, e);
            return false;
        }
    }
    
    /**
     * 关闭存储
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        
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
            started = false;
            log.info("ShardingKeyRetryStorage shutdown successfully");
        } catch (Exception e) {
            log.error("Failed to shutdown ShardingKeyRetryStorage", e);
        }
    }
    
    /**
     * 构建存储 Key
     */
    private String buildKey(String topic, String group, int queueId, String shardingKey) {
        return topic + "@" + group + "@" + queueId + "@" + shardingKey;
    }
    
    /**
     * 获取重试次数
     * 优先从缓存读取，缓存未命中则从 RocksDB 读取
     */
    public int getRetryTimes(String topic, String group, int queueId, String shardingKey) {
        if (!started) {
            return 0;
        }
        
        String key = buildKey(topic, group, queueId, shardingKey);
        
        // 先从缓存读取
        Integer cachedValue = retryTimesCache.get(key);
        if (cachedValue != null) {
            log.info("从缓存中读出重试次数: key={}, retryTimes={}", key, cachedValue);
            return cachedValue;
        }
        
        // 缓存未命中，从 RocksDB 读取
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = db.get(keyBytes);
            
            if (valueBytes != null && valueBytes.length >= 4) {
                int retryTimes = ByteBuffer.wrap(valueBytes).getInt();
                // 更新缓存
                log.info("从 rocksdb 中读出重试次数: key={}, retryTimes={}", key, retryTimes);
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
        if (!started) {
            return;
        }
        
        String key = buildKey(topic, group, queueId, shardingKey);
        
        try {
            // 更新缓存
            retryTimesCache.put(key, retryTimes);
            
            // 更新 RocksDB
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = ByteBuffer.allocate(4).putInt(retryTimes).array();
            
            db.put(writeOptions, keyBytes, valueBytes);
            
            log.debug("Set retry times: key={}, retryTimes={}", key, retryTimes);
        } catch (RocksDBException e) {
            log.warn("Failed to set retry times to RocksDB, key: {}, retryTimes: {}", key, retryTimes, e);
            // 持久化失败不影响主流程，仅记录日志
        }
    }
    
    /**
     * 增加重试次数
     */
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
        if (!started) {
            return;
        }
        
        String key = buildKey(topic, group, queueId, shardingKey);
        
        try {
            // 从缓存删除
            retryTimesCache.remove(key);
            
            // 从 RocksDB 删除
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            db.delete(writeOptions, keyBytes);
            
            log.debug("Removed retry times: key={}", key);
        } catch (RocksDBException e) {
            log.warn("Failed to remove retry times from RocksDB, key: {}", key, e);
            // 删除失败不影响主流程，仅记录日志
        }
    }
    
    /**
     * 获取缓存统计信息
     */
    public int getCacheSize() {
        return retryTimesCache.size();
    }
    
    /**
     * 清理缓存（可定期调用以释放内存）
     */
    public void clearCache() {
        retryTimesCache.clear();
        log.info("ShardingKeyRetryStorage cache cleared");
    }
    
    /**
     * 检查存储是否已启动
     */
    public boolean isStarted() {
        return started;
    }
}