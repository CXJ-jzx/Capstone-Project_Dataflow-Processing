package org.jzx.cache;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.operator.CacheEnhancedProcessFunction;
import org.jzx.cache.operator.CachedWindowAggFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 带缓存优化的地震数据流处理作业
 *
 * 主要特性：
 * 1. 三级缓存体系 (L1热点/L2时间窗口/L3空间邻域)
 * 2. 自适应缓存容量调节
 * 3. LRU-K智能淘汰策略
 * 4. 时空局部性利用
 */
public class SeismicCacheFlinkJob {

    // RocketMQ 配置
    private static final String ROCKETMQ_NAMESRV = "192.168.56.151:9876";
    private static final String ROCKETMQ_TOPIC = "seismic-data-topic";
    private static final String CONSUMER_GROUP = "seismic-cache-consumer-group";

    public static void main(String[] args) throws Exception {

        // ============ 1. 创建执行环境 ============
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(3);

        // 启用 Checkpoint
        env.enableCheckpointing(30000); // 30秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);

        // ============ 2. 创建缓存配置 ============
        CacheConfig cacheConfig = CacheConfig.builder()
                .l1MaxSize(10000)           // L1缓存最大10000条
                .lruK(2)                     // LRU-2策略
                .l1TtlMs(60000)              // L1缓存TTL 60秒
                .maxHistoryWindows(10)       // 保留最近10个历史窗口
                .windowSizeMs(1000)          // 1秒窗口
                .spatialGridSize(0.01)       // 空间网格约1公里
                .neighborRadius(1)           // 邻域半径1个网格
                .maxGrids(1000)              // 最多1000个网格
                .targetHitRate(0.70)         // 目标命中率70%
                .evictionStrategy("LRU_K")   // 使用LRU-K淘汰策略
                .build();

        System.out.println("========================================");
        System.out.println("    地震数据流处理作业 (带缓存优化)      ");
        System.out.println("========================================");
        System.out.println("缓存配置:");
        System.out.printf("  - L1缓存容量: %d, LRU-K: %d, TTL: %dms%n",
                cacheConfig.getL1MaxSize(), cacheConfig.getLruK(), cacheConfig.getL1TtlMs());
        System.out.printf("  - L2历史窗口数: %d%n", cacheConfig.getMaxHistoryWindows());
        System.out.printf("  - L3网格大小: %.4f度, 邻域半径: %d%n",
                cacheConfig.getSpatialGridSize(), cacheConfig.getNeighborRadius());
        System.out.printf("  - 目标命中率: %.0f%%%n", cacheConfig.getTargetHitRate() * 100);
        System.out.println("========================================\n");

        // ============ 3. 创建数据源 ============
        DataStream<SeismicRecord> sourceStream = env.addSource(
                new OptimizedRocketMQSource(ROCKETMQ_NAMESRV, ROCKETMQ_TOPIC, CONSUMER_GROUP)
        ).name("RocketMQ-Source").setParallelism(1);

        // ============ 4. 数据过滤 ============
        DataStream<SeismicRecord> validStream = sourceStream
                .filter(record -> {
                    boolean valid = record.getDataQuality() == 1
                            && record.getSeismicAmplitude() >= -100
                            && record.getSeismicAmplitude() <= 200
                            && record.getSensorId() != null
                            && record.getSensorId().startsWith("sensor_")
                            && record.getLongitude() >= 73 && record.getLongitude() <= 135
                            && record.getLatitude() >= 3 && record.getLatitude() <= 53;
                    return valid;
                })
                .name("Filter-Invalid-Data");

        // ============ 5. 缓存增强处理 ============
        SingleOutputStreamOperator<SeismicRecord> cachedStream = validStream
                .keyBy(SeismicRecord::getSensorId)
                .process(new CacheEnhancedProcessFunction(cacheConfig))
                .name("Cache-Enhanced-Process");

        // ============ 6. 带缓存的窗口聚合 ============
        DataStream<SeismicAggRecord> aggStream = cachedStream
                .keyBy(SeismicRecord::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new CachedWindowAggFunction(cacheConfig))
                .name("Cached-Window-Aggregation");

        // ============ 7. 输出结果 ============
        aggStream
                .map(aggRecord -> String.format(
                        "📊 [聚合] sensor=%s | 窗口=[%d,%d) | 均值=%.2f | 最大=%.2f | P波=%d | S波=%d",
                        aggRecord.getSensorId(),
                        aggRecord.getWindowStartTs(),
                        aggRecord.getWindowEndTs(),
                        aggRecord.getAvgAmplitude(),
                        aggRecord.getMaxAmplitude(),
                        aggRecord.getPPhaseCount(),
                        aggRecord.getSPhaseCount()
                ))
                .name("Format-Output")
                .print("聚合结果");

        // ============ 8. 启动作业 ============
        System.out.println("🚀 启动带缓存优化的地震数据处理作业...\n");
        env.execute("Seismic-Cache-Optimized-Job");
    }

    /**
     * RocketMQ 数据源 (复用之前的实现)
     */
    public static class OptimizedRocketMQSource
            extends RichParallelSourceFunction<SeismicRecord>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);

        private transient DefaultMQPushConsumer consumer;
        private final String namesrvAddr;
        private final String topic;
        private final String consumerGroup;
        private transient SourceContext<SeismicRecord> ctx;

        private transient Map<MessageQueue, Long> offsetMap;
        private transient ListState<Map<MessageQueue, Long>> offsetState;

        public OptimizedRocketMQSource(String namesrvAddr, String topic, String consumerGroup) {
            this.namesrvAddr = namesrvAddr;
            this.topic = topic;
            this.consumerGroup = consumerGroup;
        }

        @Override
        public void run(SourceContext<SeismicRecord> ctx) throws Exception {
            this.ctx = ctx;

            consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setConsumeTimeout(3000L);
            consumer.subscribe(topic, "*");

            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                if (!isRunning.get()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                synchronized (ctx.getCheckpointLock()) {
                    for (MessageExt msg : msgs) {
                        try {
                            SeismicRecord record = SeismicRecord.parseFrom(msg.getBody());
                            ctx.collect(record);

                            MessageQueue mq = context.getMessageQueue();
                            offsetMap.put(mq, msg.getQueueOffset() + 1);
                        } catch (Exception e) {
                            // 忽略解析错误
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            try {
                consumer.start();
                System.out.println("✅ RocketMQ Source 启动成功");
            } catch (MQClientException e) {
                throw new RuntimeException("RocketMQ初始化失败", e);
            }

            while (isRunning.get()) {
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning.set(false);
            if (consumer != null) {
                consumer.shutdown();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.offsetMap = new HashMap<>();
            offsetState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("rocketmq-offsets",
                            TypeInformation.of(new TypeHint<Map<MessageQueue, Long>>() {})));

            if (context.isRestored()) {
                for (Map<MessageQueue, Long> state : offsetState.get()) {
                    offsetMap.putAll(state);
                }
                System.out.println("✅ 从Checkpoint恢复offset：" + offsetMap);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (offsetState != null) {
                offsetState.clear();
                offsetState.add(offsetMap != null ? offsetMap : new HashMap<>());
            }
        }
    }
}