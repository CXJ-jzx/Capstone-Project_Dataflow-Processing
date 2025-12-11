package org.jzx.cache.test;

import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.controller.CacheController;
import org.jzx.cache.core.L1HotspotCache;
import org.jzx.cache.core.L2TimeWindowCache;
import org.jzx.cache.core.L3SpatialCache;
import org.jzx.cache.model.CacheStats;
import org.jzx.cache.strategy.AdaptiveCapacityStrategy;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import com.example.seismic.SeismicDataProto.SeismicAggRecord;

import java.util.Random;

/**
 * 缓存策略测试类
 */
public class CacheStrategyTest {

    private static final Random random = new Random();

    public static void main(String[] args) {
        System.out.println("========== 缓存策略测试 ==========\n");

        // 测试1: L1 LRU-K 缓存
        testL1Cache();

        // 测试2: L2 时间窗口缓存
        testL2Cache();

        // 测试3: L3 空间缓存
        testL3Cache();

        // 测试4: 自适应容量调节
        testAdaptiveCapacity();

        // 测试5: 综合缓存控制器
        testCacheController();

        System.out.println("\n========== 测试完成 ==========");
    }

    /**
     * 测试L1热点缓存
     */
    private static void testL1Cache() {
        System.out.println("--- 测试1: L1 LRU-K 缓存 ---");

        CacheConfig config = CacheConfig.builder()
                .l1MaxSize(100)
                .lruK(2)
                .l1TtlMs(10000)
                .build();

        L1HotspotCache<String> cache = new L1HotspotCache<>(config);

        // 插入数据
        for (int i = 0; i < 150; i++) {
            cache.put("key_" + i, "value_" + i);
        }

        // 模拟热点访问
        for (int i = 0; i < 50; i++) {
            cache.get("key_10"); // 热点key
            cache.get("key_20");
            cache.get("key_30");
        }

        // 再插入一些数据，触发淘汰
        for (int i = 150; i < 200; i++) {
            cache.put("key_" + i, "value_" + i);
        }

        // 检查热点是否被保留
        System.out.println("热点key_10是否存在: " + cache.contains("key_10"));
        System.out.println("热点key_20是否存在: " + cache.contains("key_20"));
        System.out.println("冷数据key_0是否存在: " + cache.contains("key_0"));

        CacheStats stats = cache.getStats();
        System.out.println(stats.report());
        System.out.println();
    }

    /**
     * 测试L2时间窗口缓存
     */
    private static void testL2Cache() {
        System.out.println("--- 测试2: L2 时间窗口缓存 ---");

        CacheConfig config = CacheConfig.builder()
                .maxHistoryWindows(5)
                .windowSizeMs(1000)
                .build();

        L2TimeWindowCache cache = new L2TimeWindowCache(config);

        String sensorId = "sensor_001";

        // 添加多个窗口
        for (int i = 0; i < 10; i++) {
            SeismicAggRecord agg = SeismicAggRecord.newBuilder()
                    .setSensorId(sensorId)
                    .setWindowStartTs(i * 1000L)
                    .setWindowEndTs((i + 1) * 1000L)
                    .setAvgAmplitude(20.0f + i)
                    .setMaxAmplitude(30.0f + i)
                    .setMinAmplitude(10.0f + i)
                    .setPPhaseCount(5)
                    .setSPhaseCount(3)
                    .setNPhaseCount(2)
                    .build();
            cache.addWindow(agg);
        }

        // 获取历史
        double historyAvg = cache.getHistoryAvgAmplitude(sensorId);
        System.out.printf("传感器 %s 历史平均振幅: %.2f%n", sensorId, historyAvg);

        // 异常检测
        boolean isAnomaly = cache.isAnomaly(sensorId, 50.0f, 0.3);
        System.out.printf("振幅50.0是否异常 (阈值30%%): %s%n", isAnomaly);

        System.out.println(cache.getStats().report());
        System.out.println();
    }

    /**
     * 测试L3空间缓存
     */
    private static void testL3Cache() {
        System.out.println("--- 测试3: L3 空间缓存 ---");

        CacheConfig config = CacheConfig.builder()
                .spatialGridSize(0.01)
                .neighborRadius(1)
                .maxGrids(100)
                .build();

        L3SpatialCache cache = new L3SpatialCache(config);

        // 模拟一个区域的多个传感器数据
        double baseLon = 108.0;
        double baseLat = 34.0;

        for (int i = 0; i < 100; i++) {
            double lon = baseLon + (random.nextDouble() * 0.05);
            double lat = baseLat + (random.nextDouble() * 0.05);
            float amplitude = 20.0f + random.nextFloat() * 10;
            String phase = random.nextDouble() < 0.3 ? "P" : (random.nextDouble() < 0.5 ? "S" : "N");

            SeismicRecord record = SeismicRecord.newBuilder()
                    .setSensorId("sensor_" + i)
                    .setLongitude(lon)
                    .setLatitude(lat)
                    .setSeismicAmplitude(amplitude)
                    .setPhaseType(phase)
                    .build();

            cache.addRecord(record);
        }

        // 查询邻域
        double neighborhoodAvg = cache.getNeighborhoodAvgAmplitude(baseLon + 0.025, baseLat + 0.025);
        System.out.printf("邻域平均振幅: %.2f%n", neighborhoodAvg);

        System.out.println(cache.getStats().report());
        System.out.println();
    }

    /**
     * 测试自适应容量调节
     */
    private static void testAdaptiveCapacity() {
        System.out.println("--- 测试4: 自适应容量调节 ---");

        CacheConfig config = CacheConfig.builder()
                .l1MaxSize(1000)
                .targetHitRate(0.70)
                .build();

        AdaptiveCapacityStrategy strategy = new AdaptiveCapacityStrategy(config);

        // 模拟低命中率场景
        System.out.println("模拟低命中率场景 (50%):");
        for (int i = 0; i < 10; i++) {
            strategy.recordHitRate(0.50);
        }

        if (strategy.shouldAdjust()) {
            CacheStats mockStats = new CacheStats("test", 1000);
            int newCapacity = strategy.calculateNewCapacity(1000, mockStats);
            System.out.printf("建议新容量: %d%n", newCapacity);
        }

        // 模拟高命中率场景
        System.out.println("\n模拟高命中率场景 (85%):");
        for (int i = 0; i < 10; i++) {
            strategy.recordHitRate(0.85);
        }

        // 需要等待调节间隔，这里直接获取报告
        System.out.println(strategy.getAdjustmentReport(1200));
        System.out.println();
    }

    /**
     * 测试综合缓存控制器
     */
    private static void testCacheController() {
        System.out.println("--- 测试5: 综合缓存控制器 ---");

        CacheConfig config = CacheConfig.builder()
                .l1MaxSize(1000)
                .lruK(2)
                .l1TtlMs(60000)
                .maxHistoryWindows(5)
                .spatialGridSize(0.01)
                .neighborRadius(1)
                .targetHitRate(0.70)
                .build();

        CacheController controller = new CacheController(config);

        // 模拟处理大量记录
        int totalRecords = 10000;
        System.out.printf("模拟处理 %d 条记录...%n", totalRecords);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalRecords; i++) {
            SeismicRecord record = generateRandomRecord(i);
            CacheController.CacheProcessResult result = controller.processRecord(record);

            // 每1000条执行一次调优
            if (i > 0 && i % 1000 == 0) {
                controller.periodicTuning();
            }
        }

        long endTime = System.currentTimeMillis();

        System.out.printf("处理完成，耗时: %d ms%n", endTime - startTime);
        System.out.printf("吞吐量: %.0f 条/秒%n", totalRecords * 1000.0 / (endTime - startTime));

        System.out.println("\n最终统计:");
        System.out.println(controller.getL1Stats().report());
        System.out.println(controller.getL2Stats().report());
        System.out.println(controller.getL3Stats().report());
        System.out.println();
    }

    /**
     * 生成随机记录
     */
    private static SeismicRecord generateRandomRecord(int index) {
        String sensorId = String.format("sensor_%04d", index % 100); // 100个传感器
        double longitude = 108.0 + random.nextDouble() * 0.1;
        double latitude = 34.0 + random.nextDouble() * 0.1;
        float amplitude = 20.0f + random.nextFloat() * 20;
        String phase = random.nextDouble() < 0.2 ? "P" : (random.nextDouble() < 0.5 ? "S" : "N");

        return SeismicRecord.newBuilder()
                .setSensorId(sensorId)
                .setCollectTimestamp(System.currentTimeMillis())
                .setLongitude(longitude)
                .setLatitude(latitude)
                .setDepth(random.nextInt(100))
                .setSeismicAmplitude(amplitude)
                .setFrequency(10)
                .setPhaseType(phase)
                .setDataQuality(1)
                .setNetworkStatus(5)
                .build();
    }
}