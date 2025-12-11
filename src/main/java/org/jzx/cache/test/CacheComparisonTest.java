package org.jzx.cache.test;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.controller.CacheController;

import java.util.Random;

/**
 * 缓存效果对比测试
 *
 * 对比项目：
 * 1. 有缓存 vs 无缓存 的吞吐量
 * 2. 不同缓存容量的命中率
 * 3. 不同淘汰策略的效果
 */
public class CacheComparisonTest {

    private static final Random random = new Random(42); // 固定种子保证可重复
    private static final int TOTAL_RECORDS = 100000;
    private static final int SENSOR_COUNT = 500; // 传感器数量

    public static void main(String[] args) {
        System.out.println("=========================================");
        System.out.println("        缓存效果对比测试                 ");
        System.out.println("=========================================\n");

        // 预生成测试数据
        SeismicRecord[] testData = generateTestData();

        // 测试1: 不同缓存容量对比
        testDifferentCapacities(testData);

        // 测试2: 有/无缓存吞吐量对比
        testThroughputComparison(testData);

        // 测试3: 命中率随时间变化
        testHitRateOverTime(testData);

        System.out.println("\n========== 测试完成 ==========");
    }

    /**
     * 测试不同缓存容量
     */
    private static void testDifferentCapacities(SeismicRecord[] testData) {
        System.out.println("--- 测试1: 不同缓存容量对比 ---");
        System.out.println("容量\t\t命中率\t\t吞吐量(条/秒)");
        System.out.println("----------------------------------------");

        int[] capacities = {100, 500, 1000, 2000, 5000, 10000};

        for (int capacity : capacities) {
            CacheConfig config = CacheConfig.builder()
                    .l1MaxSize(capacity)
                    .lruK(2)
                    .l1TtlMs(60000)
                    .maxHistoryWindows(10)
                    .spatialGridSize(0.01)
                    .build();

            CacheController controller = new CacheController(config);

            long startTime = System.currentTimeMillis();

            for (SeismicRecord record : testData) {
                controller.processRecord(record);
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            double hitRate = controller.getL1Stats().getHitRate();
            double throughput = TOTAL_RECORDS * 1000.0 / duration;

            System.out.printf("%d\t\t%.2f%%\t\t%.0f%n", capacity, hitRate * 100, throughput);
        }
        System.out.println();
    }

    /**
     * 测试有/无缓存吞吐量对比
     */
    private static void testThroughputComparison(SeismicRecord[] testData) {
        System.out.println("--- 测试2: 有/无缓存吞吐量对比 ---");

        // 有缓存
        CacheConfig config = CacheConfig.builder()
                .l1MaxSize(5000)
                .lruK(2)
                .l1TtlMs(60000)
                .maxHistoryWindows(10)
                .spatialGridSize(0.01)
                .build();

        CacheController controller = new CacheController(config);

        long startWithCache = System.currentTimeMillis();
        for (SeismicRecord record : testData) {
            controller.processRecord(record);
        }
        long endWithCache = System.currentTimeMillis();

        // 无缓存 (模拟：每次都当作未命中处理)
        long startWithoutCache = System.currentTimeMillis();
        for (SeismicRecord record : testData) {
            // 模拟无缓存处理：直接处理数据
            processWithoutCache(record);
        }
        long endWithoutCache = System.currentTimeMillis();

        long durationWithCache = endWithCache - startWithCache;
        long durationWithoutCache = endWithoutCache - startWithoutCache;

        double throughputWithCache = TOTAL_RECORDS * 1000.0 / durationWithCache;
        double throughputWithoutCache = TOTAL_RECORDS * 1000.0 / durationWithoutCache;
        double improvement = (throughputWithCache - throughputWithoutCache) / throughputWithoutCache * 100;

        System.out.printf("有缓存: %.0f 条/秒 (%d ms)%n", throughputWithCache, durationWithCache);
        System.out.printf("无缓存: %.0f 条/秒 (%d ms)%n", throughputWithoutCache, durationWithoutCache);
        System.out.printf("性能提升: %.1f%%%n", improvement);
        System.out.printf("L1命中率: %.2f%%%n", controller.getL1Stats().getHitRate() * 100);
        System.out.println();
    }

    /**
     * 测试命中率随时间变化
     */
    private static void testHitRateOverTime(SeismicRecord[] testData) {
        System.out.println("--- 测试3: 命中率随时间变化 ---");
        System.out.println("处理记录数\t命中率\t\t累计淘汰数");
        System.out.println("----------------------------------------");

        CacheConfig config = CacheConfig.builder()
                .l1MaxSize(2000)
                .lruK(2)
                .l1TtlMs(60000)
                .build();

        CacheController controller = new CacheController(config);

        int checkInterval = TOTAL_RECORDS / 10;

        for (int i = 0; i < testData.length; i++) {
            controller.processRecord(testData[i]);

            if ((i + 1) % checkInterval == 0) {
                double hitRate = controller.getL1Stats().getHitRate();
                long evictions = controller.getL1Stats().getEvictionCount();
                System.out.printf("%d\t\t%.2f%%\t\t%d%n", i + 1, hitRate * 100, evictions);
            }
        }
        System.out.println();
    }

    /**
     * 生成测试数据
     */
    private static SeismicRecord[] generateTestData() {
        System.out.println("生成测试数据...");
        SeismicRecord[] data = new SeismicRecord[TOTAL_RECORDS];

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            // 模拟热点分布：20%的传感器产生80%的数据
            int sensorIndex;
            if (random.nextDouble() < 0.8) {
                sensorIndex = random.nextInt(SENSOR_COUNT / 5); // 热点传感器
            } else {
                sensorIndex = SENSOR_COUNT / 5 + random.nextInt(SENSOR_COUNT * 4 / 5);
            }

            String sensorId = String.format("sensor_%04d", sensorIndex);
            double longitude = 108.0 + (sensorIndex % 100) * 0.001;
            double latitude = 34.0 + (sensorIndex / 100) * 0.001;
            float amplitude = 20.0f + random.nextFloat() * 20;

            data[i] = SeismicRecord.newBuilder()
                    .setSensorId(sensorId)
                    .setCollectTimestamp(System.currentTimeMillis())
                    .setLongitude(longitude)
                    .setLatitude(latitude)
                    .setDepth(random.nextInt(100))
                    .setSeismicAmplitude(amplitude)
                    .setFrequency(10)
                    .setPhaseType(random.nextDouble() < 0.3 ? "P" : "N")
                    .setDataQuality(1)
                    .setNetworkStatus(5)
                    .build();
        }

        System.out.printf("生成 %d 条测试数据完成%n%n", TOTAL_RECORDS);
        return data;
    }

    /**
     * 模拟无缓存处理
     */
    private static void processWithoutCache(SeismicRecord record) {
        // 模拟一些计算
        double result = record.getSeismicAmplitude() * 1.5;
        String gridKey = String.format("GRID_%d_%d",
                (int)(record.getLongitude() * 100),
                (int)(record.getLatitude() * 100));
        // 防止编译器优化掉
        if (result < -1000 && gridKey.isEmpty()) {
            System.out.println("Never happens");
        }
    }
}