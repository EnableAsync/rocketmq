# Apache RocketMQ 项目结构

Apache RocketMQ 是一个分布式消息和流处理平台，具有低延迟、高性能和可靠性，支持万亿级容量和灵活的可扩展性。

## 主要模块

- `broker`: 消息代理，负责消息的存储、传递和查询
- `client`: 客户端实现，包括生产者和消费者
- `common`: 公共组件和工具类
- `namesrv`: 名称服务器，提供路由注册和发现服务
- `remoting`: 远程通信模块，处理客户端与服务器之间的通信
- `store`: 消息存储模块，负责消息的持久化存储
- `tools`: 命令行工具
- `filter`: 消息过滤模块
- `srvutil`: 服务器工具类
- `example`: 使用示例
- `controller`: 控制器模块，提供高可用配置选项
- `proxy`: 代理模块，支持多种协议
- `tieredstore`: 分层存储模块
- `auth`: 认证授权模块
- `container`: 容器化支持
- `openmessaging`: OpenMessaging API 实现
- `distribution`: 发行版构建配置

## 关键文件

- `pom.xml`: Maven 项目配置文件，定义了项目的依赖和构建配置
- `README.md`: 项目说明文件，包含快速开始指南和功能介绍
- `docs/`: 文档目录，包含详细的技术文档和设计说明
- `style/`: 代码风格配置文件目录，包含 Checkstyle 和 IDE 代码格式化配置

## 构建和运行

Apache RocketMQ 使用 Maven 进行项目构建。主要的构建命令包括：

```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 构建发行版
mvn clean package -DskipTests
```

要运行 RocketMQ，需要先启动 NameServer，然后启动 Broker。详细步骤请参考 README.md 中的快速开始指南。