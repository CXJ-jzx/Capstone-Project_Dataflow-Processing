package org.jzx.version2;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 适配RocketMQ 5.3.0 + Flink 1.17.2 的完整版本
 */
public class SeismicDataFlinkConsumer {

    /**
     * 优化后的RocketMQ Source：适配RocketMQ 5.3.0，移除过时方法
     */
    public static class OptimizedRocketMQSource extends RichParallelSourceFunction<SeismicRecord> implements CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        // 去掉 MAX_MSG_NUM 限制，改为无限运行

        private transient DefaultMQPushConsumer consumer;
        private final String namesrvAddr;
        private final String topic;
        private transient SourceContext<SeismicRecord> ctx;

        // Offset状态保存
        private transient Map<MessageQueue, Long> offsetMap;
        private transient ListState<Map<MessageQueue, Long>> offsetState;

        public OptimizedRocketMQSource(String namesrvAddr, String topic) {
            this.namesrvAddr = namesrvAddr;
            this.topic = topic;
            this.offsetMap = new HashMap<>();
        }

        @Override
        public void run(SourceContext<SeismicRecord> ctx) throws Exception {
            this.ctx = ctx;

            // Consumer Group 必须固定，以便多个并发 SubTask 能够负载均衡
            consumer = new DefaultMQPushConsumer("seismic-elastic-consumer-group");
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setConsumeTimeout(3000L);
            consumer.setMaxReconsumeTimes(3);
            consumer.subscribe(topic, "*");

            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                if (!isRunning.get()) return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                synchronized (ctx.getCheckpointLock()) {
                    for (MessageExt msg : msgs) {
                        try {
                            SeismicRecord record = SeismicRecord.parseFrom(msg.getBody());
                            ctx.collect(record);

                            // 更新 Offset
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
                System.out.println("✅ RocketMQ Source 启动 (SubTask: " + getRuntimeContext().getIndexOfThisSubtask() + ")");
            } catch (MQClientException e) {
                throw new RuntimeException("RocketMQ初始化失败", e);
            }

            // 无限循环，直到 cancel() 被调用
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
            // 1. 【关键修复】无论是否是恢复模式，都必须先初始化对象！
            this.offsetMap = new HashMap<>();

            // 2. 获取状态句柄
            offsetState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("rocketmq-offsets",
                            TypeInformation.of(new TypeHint<Map<MessageQueue, Long>>() {})));

            // 3. 如果是从 Checkpoint 恢复，则填充数据
            if (context.isRestored()) {
                for (Map<MessageQueue, Long> state : offsetState.get()) {
                    // 此时 offsetMap 已经被 new HashMap<>() 了，所以不会报错
                    offsetMap.putAll(state);
                }
                System.out.println("✅ 从Checkpoint恢复offset：" + offsetMap);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (offsetState != null) {
                offsetState.clear();
                // 4. 【双重保险】防止 offsetMap 为 null (虽然 initializeState 修复后应该不会为 null 了)
                if (offsetMap != null) {
                    offsetState.add(offsetMap);
                } else {
                    // 如果万一还是 null，存一个空 Map，避免 crash
                    offsetState.add(new HashMap<>());
                }
            }
        }
    }

    /**
     * 适配Flink 1.17.2 的RocketMQ聚合Sink
     */
    static class RocketMQAggSink extends RichSinkFunction<SeismicAggRecord> {
        private transient DefaultMQProducer producer;
        private final String namesrvAddr;
        private final String topic;

        public RocketMQAggSink(String namesrvAddr, String topic) {
            this.namesrvAddr = namesrvAddr;
            this.topic = topic;
        }

        // 【修复4】导入org.apache.flink.configuration.Configuration，适配Flink 1.17.2
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters); // 必须调用父类方法
            producer = new DefaultMQProducer("seismic-agg-producer-group");
            producer.setNamesrvAddr(namesrvAddr);
            // RocketMQ 5.3.0 需设置超时时间
            producer.setSendMsgTimeout(3000);
            producer.start();
            System.out.println("✅ 聚合结果RocketMQ生产者启动成功");
        }

        @Override
        public void invoke(SeismicAggRecord value, Context context) throws Exception {
            byte[] payload = value.toByteArray();
            Message msg = new Message(topic, payload);
            msg.putUserProperty("sensor_id", value.getSensorId());
            producer.send(msg);
            System.out.println("📤 聚合结果发送至RocketMQ：sensorId=" + value.getSensorId());
        }

        @Override
        public void close() throws Exception {
            super.close(); // 必须调用父类方法
            if (producer != null) {
                producer.shutdown();
                System.out.println("🛑 聚合结果RocketMQ生产者已关闭");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. Flink环境初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(3);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 【修复5】Flink 1.17.2 直接使用CheckpointingMode枚举，无需CheckpointConfig前缀
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 2. 添加优化后的RocketMQ Source
        String rocketMQNamesrv = "192.168.56.151:9876";
        String rocketMQTopic = "seismic-data-topic";
        DataStream<SeismicRecord> seismicStream = env.addSource(
                new OptimizedRocketMQSource(rocketMQNamesrv, rocketMQTopic)
        ).name("Optimized-RocketMQ-Source").disableChaining();

        // 3. 过滤无效数据
        DataStream<SeismicRecord> validSeismicStream = seismicStream
                .filter(record -> {
                    boolean qualityValid = record.getDataQuality() == 1;
                    boolean amplitudeValid = record.getSeismicAmplitude() >= -100 && record.getSeismicAmplitude() <= 200;
                    boolean sensorIdValid = record.getSensorId() != null && record.getSensorId().startsWith("sensor_");
                    boolean lngValid = record.getLongitude() >= 73 && record.getLongitude() <= 135;
                    boolean latValid = record.getLatitude() >= 3 && record.getLatitude() <= 53;

                    boolean isValid = qualityValid && amplitudeValid && sensorIdValid && lngValid && latValid;
                    if (!isValid) {
                        System.out.println("🚫 过滤无效数据：sensorId=" + record.getSensorId()
                                + " | 质量=" + record.getDataQuality()
                                + " | 幅值=" + record.getSeismicAmplitude());
                    }
                    return isValid;
                })
                .name("Filter-Invalid-Data");

        // 4. 1秒窗口聚合
        DataStream<SeismicAggRecord> aggStream = validSeismicStream
                .keyBy(SeismicRecord::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new WindowFunction<SeismicRecord, SeismicAggRecord, String, TimeWindow>() {
                    @Override
                    public void apply(String sensorId, TimeWindow window, Iterable<SeismicRecord> records, Collector<SeismicAggRecord> out) throws Exception {
                        int count = 0;
                        float sumAmplitude = 0;
                        float maxAmplitude = Float.MIN_VALUE;
                        float minAmplitude = Float.MAX_VALUE;
                        int pCount = 0, sCount = 0, nCount = 0;

                        for (SeismicRecord record : records) {
                            count++;
                            sumAmplitude += record.getSeismicAmplitude();
                            maxAmplitude = Math.max(maxAmplitude, record.getSeismicAmplitude());
                            minAmplitude = Math.min(minAmplitude, record.getSeismicAmplitude());

                            switch (record.getPhaseType()) {
                                case "P":
                                    pCount++;
                                    break;
                                case "S":
                                    sCount++;
                                    break;
                                case "N":
                                    nCount++;
                                    break;
                                default:
                                    nCount++;
                                    break;
                            }
                        }

                        float avgAmplitude = count == 0 ? 0 : sumAmplitude / count;

                        SeismicAggRecord aggRecord = SeismicAggRecord.newBuilder()
                                .setSensorId(sensorId)
                                .setWindowStartTs(window.getStart())
                                .setWindowEndTs(window.getEnd())
                                .setAvgAmplitude(avgAmplitude)
                                .setMaxAmplitude(maxAmplitude)
                                .setMinAmplitude(minAmplitude)
                                .setPPhaseCount(pCount)
                                .setSPhaseCount(sCount)
                                .setNPhaseCount(nCount)
                                .build();

                        out.collect(aggRecord);
                    }
                })
                .name("1s-Window-Aggregation");

        // 5. 打印聚合结果
        aggStream.map(aggRecord ->
                        String.format("📊 聚合结果：sensorId=%s | 窗口=[%d, %d) | 均值=%.2f | 最大=%.2f | P波=%d | S波=%d | 噪声=%d",
                                aggRecord.getSensorId(),
                                aggRecord.getWindowStartTs(),
                                aggRecord.getWindowEndTs(),
                                aggRecord.getAvgAmplitude(),
                                aggRecord.getMaxAmplitude(),
                                aggRecord.getPPhaseCount(),
                                aggRecord.getSPhaseCount(),
                                aggRecord.getNPhaseCount())
                )
                .name("Format-Agg-Data")
                .print("聚合输出");
        /*
        // 6. 本地文件Sink
        DataStream<String> aggStrStream = aggStream.map(aggRecord ->
                String.join(",",
                        aggRecord.getSensorId(),
                        String.valueOf(aggRecord.getWindowStartTs()),
                        String.valueOf(aggRecord.getWindowEndTs()),
                        String.valueOf(aggRecord.getAvgAmplitude()),
                        String.valueOf(aggRecord.getMaxAmplitude()),
                        String.valueOf(aggRecord.getMinAmplitude()),
                        String.valueOf(aggRecord.getPPhaseCount()),
                        String.valueOf(aggRecord.getSPhaseCount()),
                        String.valueOf(aggRecord.getNPhaseCount())
                )
        ).name("Agg-Data-To-CSV");

//        StreamingFileSink<String> fileSink = StreamingFileSink
//                .forRowFormat(new Path("E:\\Desktop\\sink_output"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofHours(1))
//                                .withInactivityInterval(Duration.ofMinutes(10))
//                                .withMaxPartSize(1024 * 1024 * 100)
//                                .build()
//                )
//                .build();

        StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path("file:///E:/Desktop/sink_output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(30))  // 30秒强制滚动
                                .withInactivityInterval(Duration.ofSeconds(10)) // 10秒无数据滚动
                                .withMaxPartSize(1024 * 1024 * 1) // 1MB就滚动（测试用）
                                .build()
                )
                .build();
        aggStrStream.addSink(fileSink).name("Local-File-Sink");
         */


        /*
        // 7. RocketMQ聚合结果Sink
        aggStream.addSink(new RocketMQAggSink(rocketMQNamesrv, "seismic-agg-topic"))
                .name("RocketMQ-Agg-Sink");

         */
        // 8. 执行任务
        System.out.println("🚀 开始执行Flink全链路任务...");
        env.execute("Seismic-Data-Stream-Processing-Job");
    }
}