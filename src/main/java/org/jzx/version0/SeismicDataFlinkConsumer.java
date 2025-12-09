package org.jzx.version0;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 限定消费10条数据后自动停止的Flink+RocketMQ程序
 */
public class SeismicDataFlinkConsumer {

    // 自定义RocketMQ Source（限定仅消费10条）
    public static class LimitedRocketMQSource implements SourceFunction<byte[]> {
        private static final long serialVersionUID = 1L;
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicInteger msgCount = new AtomicInteger(0); // 消息计数器
        private static final int MAX_MSG_NUM = 5; // 最大消费条数
        private DefaultMQPushConsumer consumer;
        private final String namesrvAddr;
        private final String topic;
        private SourceContext<byte[]> ctx; // 保存SourceContext引用，用于终止

        public LimitedRocketMQSource(String namesrvAddr, String topic) {
            this.namesrvAddr = namesrvAddr;
            this.topic = topic;
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            this.ctx = ctx; // 保存上下文引用

            // 1. 初始化RocketMQ消费者
            consumer = new DefaultMQPushConsumer("seismic-flink-limited-group");
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.subscribe(topic, "*");

            // 2. 消息监听（计数+终止逻辑）
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                synchronized (ctx.getCheckpointLock()) {
                    for (MessageExt msg : msgs) {
                        // 达到10条则停止消费
                        if (msgCount.get() >= MAX_MSG_NUM) {
                            cancel(); // 触发停止
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }

                        // 计数+转发数据
                        int currentCount = msgCount.incrementAndGet();
                        System.out.println("📥 收到第" + currentCount + "条RocketMQ消息：msgId=" + msg.getMsgId());
                        ctx.collect(msg.getBody());

                        // 达到10条立即终止
                        if (currentCount >= MAX_MSG_NUM) {
                            System.out.println("✅ 已消费" + MAX_MSG_NUM + "条数据，准备停止任务");
                            cancel(); // 关闭消费者+终止Source
                            break;
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            // 3. 启动消费者
            try {
                consumer.start();
                System.out.println("✅ RocketMQ消费者启动成功！NameServer=" + namesrvAddr + "，Topic=" + topic);
            } catch (MQClientException e) {
                System.err.println("❌ RocketMQ启动失败！原因：" + e.getErrorMessage());
                throw new RuntimeException("RocketMQ初始化失败", e);
            }

            // 4. 保持运行直到计数达标
            while (isRunning.get() && msgCount.get() < MAX_MSG_NUM) {
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning.set(false);
            // 关闭RocketMQ消费者
            if (consumer != null) {
                consumer.shutdown();
                System.out.println("🛑 RocketMQ消费者已关闭");
            }
            // 触发Flink Source终止（关键：通知Flink任务可以结束）
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. Flink环境初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 2. 添加限定条数的RocketMQ Source
        String rocketMQNamesrv = "192.168.56.151:9876";
        String rocketMQTopic = "seismic-data-topic";
        DataStream<byte[]> rawBytesStream = env.addSource(
                new LimitedRocketMQSource(rocketMQNamesrv, rocketMQTopic)
        ).name("Limited-RocketMQ-Source");

        // 3. 解析Protobuf+过滤+打印
        DataStream<SeismicRecord> seismicStream = rawBytesStream
                .map((MapFunction<byte[], SeismicRecord>) bytes -> {
                    try {
                        return SeismicRecord.parseFrom(bytes);
                    } catch (Exception e) {
                        System.err.println("⚠️ Protobuf解析失败（跳过该条）：" + e.getMessage());
                        return null;
                    }
                })
                .name("Protobuf-Decode")
                .filter((FilterFunction<SeismicRecord>) record -> record != null)
                .name("Filter-Null-Record");

        // 4. 打印最终数据（标注条数）
        seismicStream.map((MapFunction<SeismicRecord, String>) record ->
                        String.format("📤 第%s条解析后数据：sensorId=%s | 幅值=%.2f | 质量=%d",
                                record.getSensorId(), // 也可自定义计数器，这里用sensorId标识
                                record.getSensorId(),
                                record.getSeismicAmplitude(),
                                record.getDataQuality())
                )
                .name("Format-For-Print")
                .print("最终输出");

        // 5. 执行任务
        System.out.println("🚀 开始执行Flink任务（仅消费10条数据）...");
        env.execute("Limited-10-Msg-SeismicData-Flink-Job");
    }
}