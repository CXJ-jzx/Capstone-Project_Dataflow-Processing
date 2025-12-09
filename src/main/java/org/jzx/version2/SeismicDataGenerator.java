package org.jzx.version2;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Random;

/**
 * 支持弹性伸缩测试的地震数据生成器
 * 修改点：
 * 1. 支持无限发送模式
 * 2. 支持通过命令行参数控制发送速率
 */
public class SeismicDataGenerator {

    // 默认 Nameserver 地址
    private static final String DEFAULT_NAMESRV = "192.168.56.151:9876";
    // 默认 Topic
    private static final String TOPIC = "seismic-data-topic";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 1. 解析命令行参数
        // args[0]: 发送间隔(ms)，默认 10ms。设置为 0 表示极速发送。
        // args[1]: Nameserver 地址（可选）
        long sleepTime = args.length > 0 ? Long.parseLong(args[0]) : 1;
        String namesrvAddr = args.length > 1 ? args[1] : DEFAULT_NAMESRV;

        // 2. 初始化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("seismic-producer-group");
        producer.setNamesrvAddr(namesrvAddr);

        // 提高发送超时时间，防止网络波动导致发送失败
        producer.setSendMsgTimeout(5000);

        // 3. 启动消息生产者服务
        producer.start();
        System.out.printf("🚀 Producer started. Target Topic: %s, NameServer: %s%n", TOPIC, namesrvAddr);
        System.out.printf("⏱️  发送间隔: %d ms (0表示全速发送)%n", sleepTime);

        long count = 0;
        try {
            // --- 核心部分：改为无限循环发送 ---
            while (true) {
                // i 用于生成模拟数据的变化参数，这里用 count 取模来替代
                int i = (int) (count % 10000);

                // 为每条消息生成略有不同的模拟数据，以区分它们
                String sensorId = String.format("sensor_%04d_region_%d", i, i % 5); // 循环使用5个区域
                double longitude = 108.0 + (i * 0.001); // 经纬度微小变化
                double latitude = 34.0 + (i * 0.001);
                int depth = new int[]{0, 10, 50, 100}[i % 4]; // 循环使用4种深度
                double baseAmplitude = 20.0 + (i * 0.5); // 基础振幅逐渐变化

                // 生成一条地震记录 (SeismicRecord) - 调用原有的生成逻辑
                SeismicRecord seismicRecord = generateSeismicRecord(sensorId, longitude, latitude, depth, baseAmplitude);

                // 将 Protobuf 对象序列化为字节数组
                byte[] payload = seismicRecord.toByteArray();

                // 4. 创建消息。Topic 必须正确，Body 是序列化后的字节数组
                Message msg = new Message(TOPIC, payload);

                // 保持与原代码一致，添加用户属性
                msg.putUserProperty("sensor_id", sensorId);

                // 5. 发送消息
                // 为了提高吞吐量测试效果，建议使用 sendOneway (单向发送)，速度最快
                // 如果需要可靠性，可以使用 producer.send(msg);
                try {
                    producer.sendOneway(msg);
                    // producer.send(msg); // 同步发送，速度较慢，适合调试
                } catch (Exception e) {
                    System.err.println("发送失败: " + e.getMessage());
                }

                count++;
                // 每发送 1000 条打印一次日志，避免日志刷屏影响性能
                if (count % 1000 == 0) {
                    System.out.printf("已发送 %d 条消息...%n", count);
                }

                // 控制发送速率：模拟高/低负载
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
            }

        } finally {
            // 6. 停止消息生产者服务（虽然无限循环通常通过 Ctrl+C 停止，但保留此逻辑是好习惯）
            producer.shutdown();
            System.out.println("Producer shutdown.");
        }
    }

    /**
     * 生成一条模拟的 SeismicRecord。
     * (代码逻辑保持完全不变)
     */
    private static SeismicRecord generateSeismicRecord(String sensorId, double longitude, double latitude, int depth, double baseAmplitude) {
        SeismicRecord.Builder builder = SeismicRecord.newBuilder();
        builder.setSensorId(sensorId);
        builder.setCollectTimestamp(System.currentTimeMillis());
        builder.setLongitude(longitude);
        builder.setLatitude(latitude);
        builder.setDepth(depth);
        builder.setFrequency(10); // 固定采样频率
        builder.setNetworkStatus(5); // 固定网络状态

        Random random = new Random();
        // 5% 的概率生成异常数据
        if (random.nextDouble() < 0.05) {
            builder.setDataQuality(0);
            builder.setSeismicAmplitude((float) (random.nextDouble() * 200 - 100));
            builder.setPhaseType("N");
        } else {
            builder.setDataQuality(1);
            // 模拟一些变化
            double noise = random.nextGaussian() * 2.5;
            float amplitude = (float) (baseAmplitude + noise);
            builder.setSeismicAmplitude(amplitude);
            builder.setPhaseType(Math.abs(amplitude) > 30 ? (random.nextBoolean() ? "P" : "S") : "N");
        }
        return builder.build();
    }
}