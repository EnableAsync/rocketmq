# Apache RocketMQ 主要功能模块

Apache RocketMQ 是一个分布式消息和流处理平台，提供多种功能特性以满足不同的业务需求。

## 核心功能

### 1. 消息发布与订阅

RocketMQ 支持传统的发布/订阅模式，生产者将消息发布到指定的主题，消费者订阅感兴趣的主题并消费消息。

相关代码目录:
- `org.apache.rocketmq.client.producer`: 生产者实现
- `org.apache.rocketmq.client.consumer`: 消费者实现
- `org.apache.rocketmq.broker`: 消息代理，负责消息的存储和传递

主要特点:
- 支持多种消息类型：普通消息、顺序消息、事务消息
- 支持集群消费和广播消费模式
- 提供高效的消息路由机制

### 2. 消息存储

RocketMQ 提供高可靠的消息存储机制，确保消息不丢失。

相关代码目录:
- `org.apache.rocketmq.store`: 消息存储模块
- `org.apache.rocketmq.store.config`: 存储配置
- `org.apache.rocketmq.store.stats`: 存储统计信息

主要特点:
- 基于文件的存储机制
- 支持顺序写入，提高写入性能
- 提供多种刷盘策略：同步刷盘和异步刷盘

### 3. 高可用性

RocketMQ 提供多种高可用性保障机制。

相关代码目录:
- `org.apache.rocketmq.namesrv`: 名称服务器
- `org.apache.rocketmq.ha`: 高可用服务
- `org.apache.rocketmq.controller`: 控制器模块

主要特点:
- NameServer 集群部署，无状态设计
- Broker 主从复制机制
- 基于 DLedger 的控制器模式

### 4. 消息过滤

RocketMQ 支持多种消息过滤机制，允许消费者只接收感兴趣的消息。

相关代码目录:
- `org.apache.rocketmq.filter`: 消息过滤模块
- `org.apache.rocketmq.filter.expression`: 过滤表达式解析

主要特点:
- 支持 Tag 过滤
- 支持 SQL 表达式过滤
- 支持自定义过滤器

## 扩展功能

### 5. 事务消息

RocketMQ 支持分布式事务消息，确保消息发送与本地事务的一致性。

相关代码目录:
- `org.apache.rocketmq.client.producer.transaction`: 事务消息生产者
- `org.apache.rocketmq.broker.transaction`: 事务消息处理

主要特点:
- 基于两阶段提交协议
- 提供事务状态回查机制
- 支持自定义事务监听器

### 6. 流处理

RocketMQ 提供流处理能力，支持实时数据处理。

相关代码目录:
- `org.apache.rocketmq.streams`: 流处理模块

主要特点:
- 支持窗口计算
- 提供丰富的流处理算子
- 支持状态管理和容错机制

### 7. 多协议支持

RocketMQ 支持多种消息协议，方便不同系统集成。

相关代码目录:
- `org.apache.rocketmq.remoting`: 远程通信模块
- `org.apache.rocketmq.grpc`: gRPC 协议支持
- `org.apache.rocketmq.openmessaging`: OpenMessaging 协议实现

主要特点:
- 支持 Remoting 协议（原生协议）
- 支持 gRPC 协议
- 支持 OpenMessaging 协议
- 支持 MQTT 协议

### 8. 认证与授权

RocketMQ 提供安全机制，保护消息系统的安全。

相关代码目录:
- `org.apache.rocketmq.auth`: 认证授权模块

主要特点:
- 支持 ACL（访问控制列表）
- 提供身份认证机制
- 支持权限管理

## 运维与监控

### 9. 管理工具

RocketMQ 提供丰富的管理工具，方便系统运维。

相关代码目录:
- `org.apache.rocketmq.tools`: 命令行工具
- `org.apache.rocketmq.admin`: 管理工具

主要特点:
- 提供命令行管理工具
- 支持集群管理
- 提供监控指标

### 10. 监控与追踪

RocketMQ 提供完善的监控和追踪能力。

相关代码目录:
- `org.apache.rocketmq.broker.metrics`: Broker 指标收集
- `org.apache.rocketmq.client.trace`: 消息追踪

主要特点:
- 集成 OpenTelemetry
- 提供 Prometheus 指标导出
- 支持消息追踪