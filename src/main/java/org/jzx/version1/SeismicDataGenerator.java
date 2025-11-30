package org.jzx.version1;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Random;

public class SeismicDataGenerator {

    // 定义要发送的消息数量
    private static final int NUM_MESSAGES_TO_SEND = 3000; // 可修改为更大值（如10/20）

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 1. 初始化一个消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("seismic-producer-group");

        // 2. 指定 nameserver 地址
        producer.setNamesrvAddr("192.168.56.151:9876");

        // 3. 启动消息生产者服务
        producer.start();
        System.out.printf("Producer started.准备发送 %d 条消息.%n", NUM_MESSAGES_TO_SEND);

        try {
            // --- 核心部分：循环发送指定数量的消息 ---
            for (int i = 0; i < NUM_MESSAGES_TO_SEND; i++) {
                // 为每条消息生成略有不同的模拟数据，以区分它们
                String sensorId = String.format("sensor_%04d_region_%d", i, i % 5); // 循环使用5个区域
                double longitude = 108.0 + (i * 0.001); // 经纬度微小变化
                double latitude = 34.0 + (i * 0.001);
                int depth = new int[]{0, 10, 50, 100}[i % 4]; // 循环使用4种深度
                double baseAmplitude = 20.0 + (i * 0.5); // 基础振幅逐渐变化

                // 生成一条地震记录 (SeismicRecord)
                SeismicRecord seismicRecord = generateSeismicRecord(sensorId, longitude, latitude, depth, baseAmplitude);

                // 将 Protobuf 对象序列化为字节数组
                byte[] payload = seismicRecord.toByteArray();

                // 4. 创建消息。Topic 必须正确，Body 是序列化后的字节数组
                Message msg = new Message("seismic-data-topic", payload);

                // 保持与原代码一致，添加用户属性
                msg.putUserProperty("sensor_id", sensorId);

                // 5. 发送消息，获取发送结果
                System.out.printf("Sending message #%d...%n", i + 1);
                producer.send(msg);
                System.out.printf("Message #%d sent successfully.%n", i + 1);

                // 可选：每条消息之间暂停一下，模拟真实环境中的数据产生间隔
                Thread.sleep(100);
            }

        } catch (RemotingException | MQBrokerException e) {
            // 捕获并打印所有可能的异常
            e.printStackTrace();
        } finally {
            // 6. 所有消息发送完毕后，停止消息生产者服务。
            producer.shutdown();
            System.out.println("All messages sent. Producer shutdown.");
        }
    }

    /**
     * 生成一条模拟的 SeismicRecord。
     * 这个方法保留了原代码中复杂的模拟逻辑，以确保消息内容的真实性。
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