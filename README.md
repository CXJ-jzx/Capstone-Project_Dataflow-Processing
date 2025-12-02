# 地震数据实时处理系统 (Dataflow-Processing)

1.本项目实现了一个基于 **Apache Flink** 与 **RocketMQ** 的实时地震监测数据处理系统。  
2.系统能够模拟多传感器地震数据的生产、传输、清洗、实时聚合，并输出统计结果。  
3.当前版本（version1）针对 **RocketMQ 5.3.0** 和 **Flink 1.17.2** 进行了适配与性能优化。

---

# 🚀 项目亮点

- **支持高吞吐消息传输**（RocketMQ 5.3.0）
- **支持 Protobuf 高效二进制序列化**
- **Flink 实现实时流处理**（过滤、清洗、窗口聚合）
- **优化状态管理与 Exactly-Once 语义**
- **1 秒滚动窗口进行地震振幅统计**
- **支持本地文件输出 + 控制台输出 + 可扩展 MQ 回写**

---

# 🧱 技术栈
| 类型 | 技术 |
|-----|------|
| 流处理框架 | Apache Flink 1.17.2 |
| 消息队列 | Apache RocketMQ 5.3.0 |
| 数据格式 | Protocol Buffers 3.21.7 |
| 构建工具 | Maven |
| 语言 | Java |

---

# 📂 项目结构

```proto
src/main/
├── java/org/jzx/
│   ├── version1/                      # 优化后的正式版本
│   │   ├── SeismicDataFlinkConsumer.java   # Flink 实时计算主逻辑
│   │   └── SeismicDataGenerator.java       # RocketMQ 地震数据模拟生成器
│   └── version0/                      # 初始版本（仅作参考）
└── proto/
    └── seismic_data.proto             # Protobuf 数据结构定义
```

---

# 🧩 核心模块详解

## 1. 数据结构定义（Protocol Buffers）
项目使用 Protocol Buffers 定义了两类核心数据结构，用于描述实时地震数据与窗口聚合结果。

### **① SeismicRecord：原始地震数据结构**

包括：

- 传感器 ID
- 采集时间戳
- 经纬度与深度
- 地震振幅
- 频率
- 波形类型（P、S、Noise）
- 数据质量标记（有效/无效）
- 网络状态

### **② SeismicAggRecord：窗口聚合后的地震数据**

包括：

- 聚合窗口时间范围
- 平均振幅、最大振幅、最小振幅
- 各类波形数量统计

### **Protobuf 文件示例（片段）**

```proto
syntax = "proto3";

package com.example.seismic;

option java_package = "com.example.seismic";
option java_outer_classname = "SeismicDataProto";

message SeismicRecord {
  string sensor_id = 1;
  int64 collect_timestamp = 2;
  double longitude = 3;
  double latitude = 4;
  int32 depth = 5;
  float seismic_amplitude = 6;
  int32 frequency = 7;
  string phase_type = 8;  // "P" / "S" / "N"
  int32 data_quality = 9; // 0=无效, 1=有效
  int32 network_status = 10;
}

message SeismicAggRecord {
  string sensor_id = 1;
  int64 window_start_ts = 2;
  int64 window_end_ts = 3;
  float avg_amplitude = 4;
  float max_amplitude = 5;
  float min_amplitude = 6;
  int32 p_phase_count = 7;
  int32 s_phase_count = 8;
  int32 n_phase_count = 9;
}
```
---
## 2. 数据生成器（SeismicDataGenerator）

`SeismicDataGenerator` 是一个高性能地震数据模拟器，用于向 RocketMQ Topic 发送 Protobuf 序列化的地震记录。

### 主要功能

- 自动生成 **3000 条**（可配置）地震数据
- 支持多个虚拟传感器（传感器 ID 自动轮询生成）
- 自动随机生成以下字段：
  - 经纬度（longitude & latitude）
  - 深度（depth）
  - 地震波振幅（seismic_amplitude）
  - 频率（frequency）
  - 波形类型（phase_type = P / S / N）
- 自动生成 **5% 异常数据**用于测试过滤逻辑
- 使用 Protobuf 序列化消息
- 将数据发送至 RocketMQ 的 Topic（例如：`seismic-data-topic`）

### 数据流向
模拟生成器 → RocketMQ Topic → Flink 实时计算


---

## 3. Flink 数据消费者（SeismicDataFlinkConsumer）

这是整个系统的核心模块，负责从 RocketMQ 拉取消息，并使用 Flink 完成实时清洗、聚合和输出。

---

### 3.1 优化版 RocketMQ Source（OptimizedRocketMQSource）

`SeismicDataFlinkConsumer` 内部包含一个专门为 RocketMQ 5.3.0 写的自定义 Source。

#### 特点

- ✔ 兼容 RocketMQ 5.3.0 新 API  
- ✔ 支持 Flink Checkpoint → 对应 RocketMQ Offset 保存  
- ✔ 完整支持 **Exactly-Once** 语义  
- ✔ 支持最大消息数限制（适用于测试）  
- ✔ 异常自动重试，确保稳定性  
- ✔ 自动解析 Protobuf

#### 源码关键点

- 使用 `ClientServiceProvider` 创建 pushConsumer / pullConsumer  
- 使用 Flink 的 `CheckpointedFunction` 持久化 offset  
- 使用 `SourceReader` + `SplitReader` 构建消息读取逻辑  

> 这是本项目的核心技术亮点之一，即 **手写与 RocketMQ 深度适配的 Flink Source**。

---

### 3.2 数据处理流水线

`SeismicDataFlinkConsumer` 完成了一个完整、可扩展的数据处理链条，包括以下步骤：

#### ① 数据源接入（Source）

从 RocketMQ 按顺序拉取消息，并解码为 `SeismicRecord`。

**日志示例：**
Received record from sensor S001 amplitude=3.21 phase=P


#### ② 数据清洗（Data Cleaning）

过滤条件：

- `data_quality = 0`（质量无效）  
- 振幅超出异常范围  
- 经纬度不合法  
- 空字段或无法解析的消息  

> 模拟异常数据也会在这里被过滤掉。

#### ③ 实时聚合（Window Aggregation）

使用 Flink 滚动窗口：

```java
.keyBy(record -> record.getSensorId())
.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
```

#### ② 数据清洗（Data Cleaning）

过滤条件：

- `data_quality = 0`（质量无效）  
- 振幅超出异常范围  
- 经纬度不合法  
- 空字段或无法解析的消息  

> 模拟异常数据也会在这里被过滤掉。

#### ③ 实时聚合（Window Aggregation）

使用 Flink 滚动窗口：

```java
.keyBy(record -> record.getSensorId())
.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
```
实现 1 秒的滚动窗口聚合，统计内容包括：
-平均振幅（avg_amplitude）
-最大振幅（max_amplitude）
-最小振幅（min_amplitude）
-P/S/N 波形计数

#### ④ 聚合结果输出（Sink）

当前版本提供 **3 种输出方式**：

- ✔ **控制台输出（默认）**  
  便于调试：

  ```text
  Window Result: S001 avg=4.2 max=5.0 min=3.2 P=12 S=8 N=5
  ```

- ✔ 写入文件系统（CSV 格式）
输出目录：
output/seismic_agg_result/
每个窗口写入一条 CSV 文件。

- ✔ 可选发送回 RocketMQ
代码中已实现，只需取消注释即可启用。

# 🎉Flink Standalone 弹性伸缩系统 - 完整操作指南

## 目录

- [一、环境概述](#一环境概述)
- [二、前置准备](#二前置准备)
- [三、启动数据源（Socket）](#三启动数据源socket)
- [四、提交 Flink 作业](#四提交-flink-作业)
- [五、更新弹性伸缩配置](#五更新弹性伸缩配置)
- [六、测试指标采集](#六测试指标采集)
- [七、测试弹性决策](#七测试弹性决策)
- [八、测试扩缩容功能](#八测试扩缩容功能)
- [九、验证扩缩容结果](#九验证扩缩容结果)
- [十、启动后台弹性伸缩服务](#十启动后台弹性伸缩服务)
- [十一、常用运维命令汇总](#十一常用运维命令汇总)
- [十二、功能验证清单](#十二功能验证清单)
- [十三、故障排查](#十三故障排查)
- [十四、总结](#十四总结)

---

## 一、环境概述

### 1.1 集群架构

| 节点 | 角色 | 说明 |
|------|------|------|
| node01 | JobManager + 弹性调度 | 主节点，运行调度脚本 |
| node02 | TaskManager + 数据源 | 运行 nc 监听 9999 端口 |
| node03 | TaskManager | 工作节点 |

### 1.2 目录结构

```proto
/data/flink/elastic/
├── conf/
│ └── elastic.conf # 配置文件
├── collect_metrics.sh # 指标采集脚本
├── elastic_decision.sh # 决策脚本
├── elastic_loop.sh # 主循环脚本
├── alert.sh # 告警脚本
├── log/ # 日志目录
├── metrics/ # 指标数据目录
└── savepoints/ # Savepoint 目录
```



