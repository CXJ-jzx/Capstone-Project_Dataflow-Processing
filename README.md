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

---

## 二、前置准备

### 2.1 确保 Flink 集群已启动

```bash
# 在 node01 上执行
# 检查集群状态
curl -s http://node01:8081/overview | jq .

# 如果未启动，启动集群
/opt/app/flink-1.17.2/bin/start-cluster.sh

# 验证 TaskManager 数量（应该是 3 个）
curl -s http://node01:8081/taskmanagers | jq '.taskmanagers | length'
```

### 2.2 确保弹性伸缩脚本已部署

```Bash
# 检查脚本文件
ls -la /data/flink/elastic/

# 确保脚本有执行权限
chmod +x /data/flink/elastic/*.sh
```

---
## 三、启动数据源（Socket）
### 3.1 在 node02 上启动 nc 监听

```bash
# SSH 登录到 node02
ssh root@node02

# 启动 nc 监听 9999 端口（手动输入模式）
nc -lk 9999
```

### 3.2 或使用自动数据生成（可选）

```bash
如果需要自动生成数据流，在 node02 上执行：
#自动生成数据（每秒约 10 条）
(while true; do 
    echo "hello world flink test $(date +%N)"
    sleep 0.1
done) | nc -lk 9999
```


## 四、提交 Flink 作业

### 4.1 在 node01 上提交 WordCount 作业

```bash
# 重新提交 WordCount 作业
JOB_OUTPUT=$(/opt/app/flink-1.17.2/bin/flink run -d \
    -m node01:8081 \
    -p 3 \
    -c org.jzx.WordCount \
    /opt/flink_jobs/wordcount.jar 2>&1)

echo "${JOB_OUTPUT}"
```

预期输出：
```text
Job has been submitted with JobID <32位十六进制ID>
```
---

### 4.2 提取 Job ID

```bash
# 提取新的 Job ID
NEW_JOB_ID=$(echo "${JOB_OUTPUT}" | grep -oE '[a-f0-9]{32}' | tail -1)
echo "新 Job ID: ${NEW_JOB_ID}"
```

### 4.3 验证作业状态
```bash
# 查看所有作业
curl -s http://node01:8081/jobs | jq .

# 查看作业详情
curl -s http://node01:8081/jobs/${JOB_ID} | jq '{id, name, state}'
```

预期输出：
```json
{
  "jobs": [
    {
      "id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      "status": "RUNNING"
    }
  ]
}
```

## 五、更新弹性伸缩配置

### 5.1 更新 Job ID 到配置文件

```bash
# 方法1：使用 sed 自动更新
sed -i "s/^FLINK_JOB_ID=.*/FLINK_JOB_ID=${JOB_ID}/" /data/flink/elastic/conf/elastic.conf

# 验证更新结果
grep "FLINK_JOB_ID" /data/flink/elastic/conf/elastic.conf
```

### 5.2 或手动编辑配置文件

```bash
# 方法2：手动编辑
vim /data/flink/elastic/conf/elastic.conf

# 修改 FLINK_JOB_ID 行为：
# FLINK_JOB_ID=<你的Job ID>
```

### 5.3 完整配置文件参考

```bash
# 查看当前配置
cat /data/flink/elastic/conf/elastic.conf
```
配置内容：
```bash

# Flink集群配置
FLINK_REST_URL=http://node01:8081
FLINK_BIN_PATH=/opt/app/flink-1.17.2/bin
JOB_MANAGER_IP=node01

# TM 配置
TASK_MANAGER_MIN_NUM=3
TASK_MANAGER_MAX_NUM=6
SLOTS_PER_TM=2

# 任务配置
FLINK_JOB_ID=<你的Job ID>
JOB_JAR=/opt/flink_jobs/wordcount.jar
JOB_MAIN_CLASS=org.jzx.WordCount
JOB_ARGS=

# 并行度配置
PARALLELISM_MIN=3
PARALLELISM_MAX=12

# 弹性触发阈值
THROUGHPUT_UPPER=1000
THROUGHPUT_LOWER=200
CPU_LOAD_UPPER=0.80
CPU_LOAD_LOWER=0.30
TRIGGER_DURATION=30

# 目录配置
LOG_DIR=/data/flink/elastic/log
METRICS_DIR=/data/flink/elastic/metrics
SAVEPOINT_DIR=/data/flink/elastic/savepoints

```

## 六、测试指标采集
### 6.1 输入测试数据
在 node02 的 nc 终端 中输入：

```text
hello world
hello flink
test elastic scaling
```
---

### 6.2 测试采集脚本
```bash
# 在 node01 上执行
# 单次采集测试
/data/flink/elastic/collect_metrics.sh
```
预期输出：
---
```text
throughput=10/s | cpu=0.0065 | tm=3 | records=15
```

### 6.3 多次采集（积累样本）
```bash
# 连续采集 10 次（防抖机制需要至少 3 个样本）
for i in {1..10}; do
    echo "--- 采集第 $i 次 ($(date '+%H:%M:%S')) ---"
    /data/flink/elastic/collect_metrics.sh
    sleep 5
done
```

---
### 6.4 查看历史指标
```bash
# 查看采集的历史数据
cat /data/flink/elastic/metrics/history_metrics.log
```
预期输出：
---
```text
1764458303,throughput:10,cpu:0.0070
1764458308,throughput:12,cpu:0.0068
1764458314,throughput:15,cpu:0.0072
```
## 七、测试弹性决策

### 7.1 运行决策脚本
```bash
# 执行决策
/data/flink/elastic/elastic_decision.sh
```
---
### 7.2 查看决策日志
```bash
# 查看决策日志
tail -20 /data/flink/elastic/log/elastic_schedule.log
```
预期输出（正常情况）：
---
```text
[2025-11-30 12:00:00] ========== 开始决策 ==========
[2025-11-30 12:00:00] 当前状态: 并行度=3, TM数量=3
[2025-11-30 12:00:00] 各项指标正常，无需调整
```

## 八、测试扩缩容功能
### 8.1 测试缩容（提高阈值）

```bash
# 临时调低阈值以触发缩容
sed -i 's/CPU_LOAD_LOWER=.*/CPU_LOAD_LOWER=0.50/' /data/flink/elastic/conf/elastic.conf

# 清理历史数据
rm -f /data/flink/elastic/metrics/history_metrics.log

# 采集并决策
for i in {1..8}; do
    /data/flink/elastic/collect_metrics.sh
    sleep 10
done

# 运行决策
/data/flink/elastic/elastic_decision.sh

# 查看结果
tail -10 /data/flink/elastic/log/elastic_schedule.log

```

预期输出：
---
```text
[2025-11-30 12:00:00] CPU 负载低于下限，触发 TM 缩容
[2025-11-30 12:00:00] TM 数量已达下限 (3/3)
```

### 8.2 测试扩容（降低阈值）
```bash
# 临时调低扩容阈值
sed -i 's/CPU_LOAD_UPPER=.*/CPU_LOAD_UPPER=0.005/' /data/flink/elastic/conf/elastic.conf
sed -i 's/CPU_LOAD_LOWER=.*/CPU_LOAD_LOWER=0.001/' /data/flink/elastic/conf/elastic.conf

# 清理历史数据
rm -f /data/flink/elastic/metrics/history_metrics.log

# 采集并决策
for i in {1..8}; do
    /data/flink/elastic/collect_metrics.sh
    sleep 10
done

# 运行决策
/data/flink/elastic/elastic_decision.sh

# 查看结果
tail -10 /data/flink/elastic/log/elastic_schedule.log
```

预期输出：
---
```text
[2025-11-30 12:00:00] CPU 负载超过上限，触发 TM 扩容
[2025-11-30 12:00:00] 在 node02 启动新 TM...
[2025-11-30 12:00:00] node02 新增 TM，当前集群总数: 4
```

### 8.3 恢复正常配置
```bash
# 恢复正式阈值配置
cat > /data/flink/elastic/conf/elastic.conf << 'EOF'
# Flink集群配置
FLINK_REST_URL=http://node01:8081
FLINK_BIN_PATH=/opt/app/flink-1.17.2/bin
JOB_MANAGER_IP=node01

# TM 配置
TASK_MANAGER_MIN_NUM=3
TASK_MANAGER_MAX_NUM=6
SLOTS_PER_TM=2

# 任务配置
FLINK_JOB_ID=<替换为你的Job ID>
JOB_JAR=/opt/flink_jobs/wordcount.jar
JOB_MAIN_CLASS=org.jzx.WordCount
JOB_ARGS=

# 并行度配置
PARALLELISM_MIN=3
PARALLELISM_MAX=12

# 弹性触发阈值
THROUGHPUT_UPPER=1000
THROUGHPUT_LOWER=200
CPU_LOAD_UPPER=0.80
CPU_LOAD_LOWER=0.30
TRIGGER_DURATION=30

# 目录配置
LOG_DIR=/data/flink/elastic/log
METRICS_DIR=/data/flink/elastic/metrics
SAVEPOINT_DIR=/data/flink/elastic/savepoints
EOF

echo "配置已恢复"
```

## 九、验证扩缩容结果
### 9.1 查看 TaskManager 数量
```bash
# 通过 REST API 查看
curl -s http://node01:8081/taskmanagers | jq '.taskmanagers | length'

# 查看 TM 详情
curl -s http://node01:8081/taskmanagers | jq '.taskmanagers[] | {id, slotsNumber, freeSlots}'
```
### 9.2 查看各节点 TM 分布
```bash
# 检查各节点的 TaskManager 进程
for node in node01 node02 node03; do
    count=$(ssh -o ConnectTimeout=3 root@${node} "jps 2>/dev/null | grep -c TaskManagerRunner || echo 0")
    echo "${node}: ${count} 个 TM"
done
```
预期输出（扩容后）：
---
```text
node01: 1 个 TM
node02: 2 个 TM
node03: 1 个 TM
```
### 9.3 查看集群总览
```bash

curl -s http://node01:8081/overview | jq '{
  taskmanagers: .taskmanagers,
  slots_total: .["slots-total"],
  slots_available: .["slots-available"],
  jobs_running: .["jobs-running"]
}'

```

### 9.4阈值触发逻辑总结
| 指标 | 条件 | 动作 |
|------|------|------|
| 吞吐量 > 1000/s | 持续 30 秒 | 增加并行度 (+2) |
| 吞吐量 < 200/s | 持续 30 秒 | 减少并行度 (-1) |
| CPU > 80% | 持续 30 秒 | 增加 TM (+1) |
| CPU < 30% | 持续 30 秒 | 减少 TM (-1) |


## 十、启动后台弹性伸缩服务
### 10.1 启动服务
```bash

# 停止可能存在的旧进程
pkill -f elastic_loop.sh 2>/dev/null

# 清理历史数据
rm -f /data/flink/elastic/metrics/history_metrics.log

# 后台启动弹性伸缩服务
nohup /data/flink/elastic/elastic_loop.sh > /dev/null 2>&1 &

# 确认服务已启动
ps aux | grep elastic_loop | grep -v grep

```
---
### 10.2 实时监控日志
```bash
# 监控决策日志
tail -f /data/flink/elastic/log/elastic_schedule.log
```
---
### 10.3 停止服务
```bash
# 停止弹性伸缩服务
pkill -f elastic_loop.sh

# 确认已停止
ps aux | grep elastic_loop
```

## 十一、常用运维命令汇总
### 11.1 服务管理
| 操作 | 命令 |
|------|------|
| 启动弹性伸缩服务 | `nohup /data/flink/elastic/elastic_loop.sh > /dev/null 2>&1 &` |
| 停止弹性伸缩服务 | `pkill -f elastic_loop.sh` |
| 查看服务状态 | `ps aux \| grep elastic_loop \| grep -v grep` |

---
### 11.2 日志查看
| 日志类型 | 命令 |
|----------|------|
| 决策日志 | `tail -f /data/flink/elastic/log/elastic_schedule.log` |
| 采集日志 | `tail -f /data/flink/elastic/log/collect_metrics.log` |
| 告警日志 | `cat /data/flink/elastic/log/alert.log` |

---
### 11.3 指标查看
| 操作 | 命令 |
|------|------|
| 查看历史指标 | `cat /data/flink/elastic/metrics/history_metrics.log` |
| 手动采集一次 | `/data/flink/elastic/collect_metrics.sh` |

---
### 11.4 集群状态
| 操作 | 命令 |
|------|------|
| 查看集群概览 | `curl -s http://node01:8081/overview \| jq .` |
| 查看 TM 数量 | `curl -s http://node01:8081/taskmanagers \| jq '.taskmanagers \| length'` |
| 查看作业状态 | `curl -s http://node01:8081/jobs \| jq .` |
| 查看作业详情 | `curl -s http://node01:8081/jobs/<JOB_ID> \| jq '{state, name}'` |

---
### 11.5 Flink 集群管理
| 操作 | 命令 |
|------|------|
| 启动集群 | `/opt/app/flink-1.17.2/bin/start-cluster.sh` |
| 停止集群 | `/opt/app/flink-1.17.2/bin/stop-cluster.sh` |
| 单独启动 TM | `/opt/app/flink-1.17.2/bin/taskmanager.sh start` |
| 查看 Flink 进程 | `jps \| grep -E "TaskManager\|StandaloneSession"` |

## 十二、功能验证清单

| 序号 | 测试项 | 验证方法 | 预期结果 |
|:----:|--------|----------|----------|
| 1 | 集群启动 | `curl http://node01:8081/overview` | 3 个 TM，6 个 Slots |
| 2 | 作业提交 | `flink run -d ...` | Job 状态为 RUNNING |
| 3 | 指标采集 | `/data/flink/elastic/collect_metrics.sh` | 显示吞吐量和 CPU |
| 4 | 防抖机制 | 查看日志 | 需要 3+ 样本才触发 |
| 5 | TM 缩容 | 降低 CPU_LOAD_LOWER | TM 数量减少 |
| 6 | TM 扩容 | 降低 CPU_LOAD_UPPER | TM 数量增加 |
| 7 | 下限保护 | 持续触发缩容 | 保持最少 3 个 TM |
| 8 | 上限保护 | 持续触发扩容 | 最多 6 个 TM |

---
> 📝 **文档版本：** v1.0  
> 📅 **更新日期：** 2025-11-30  
> 🔧 **Flink 版本：** 1.17.2  
> 🖥️ **部署模式：** Standalone

