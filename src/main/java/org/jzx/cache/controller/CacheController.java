package org.jzx.cache.controller;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.core.*;
import org.jzx.cache.model.CacheStats;
import org.jzx.cache.model.HistoryWindow;
import org.jzx.cache.model.SpatialGrid;
import org.jzx.cache.strategy.AdaptiveCapacityStrategy;

import java.io.Serializable;
import java.util.List;

/**
 * 缓存控制器 - 统一管理三级缓存
 */
public class CacheController implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CacheConfig config;

    // 三级缓存
    private final L1HotspotCache<SeismicRecord> l1Cache;
    private final L2TimeWindowCache l2Cache;
    private final L3SpatialCache l3Cache;

    // 自适应调节策略
    private final AdaptiveCapacityStrategy capacityStrategy;

    // 指标收集
    private final CacheMetricsCollector metricsCollector;

    // 上次调优时间
    private long lastTuningTime;

    public CacheController(CacheConfig config) {
        this.config = config;
        this.l1Cache = new L1HotspotCache<>(config);
        this.l2Cache = new L2TimeWindowCache(config);
        this.l3Cache = new L3SpatialCache(config);
        this.capacityStrategy = new AdaptiveCapacityStrategy(config);
        this.metricsCollector = new CacheMetricsCollector();
        this.lastTuningTime = System.currentTimeMillis();
    }

    // ============ L1 热点缓存操作 ============

    /**
     * 尝试从L1缓存获取记录
     */
    public SeismicRecord getFromL1(String sensorId) {
        long start = System.currentTimeMillis();
        SeismicRecord record = l1Cache.get(sensorId);
        metricsCollector.recordLatency(System.currentTimeMillis() - start);
        return record;
    }

    /**
     * 将记录放入L1缓存
     */
    public void putToL1(String sensorId, SeismicRecord record) {
        l1Cache.put(sensorId, record);
    }

    /**
     * 检查L1缓存是否包含该传感器
     */
    public boolean l1Contains(String sensorId) {
        return l1Cache.contains(sensorId);
    }

    // ============ L2 时间窗口缓存操作 ============

    /**
     * 添加窗口聚合结果到L2缓存
     */
    public void addWindowToL2(SeismicAggRecord aggRecord) {
        l2Cache.addWindow(aggRecord);
    }

    /**
     * 获取传感器的历史窗口
     */
    public List<HistoryWindow> getHistoryWindows(String sensorId, int n) {
        return l2Cache.getHistory(sensorId, n);
    }

    /**
     * 获取历史平均振幅
     */
    public double getHistoryAvgAmplitude(String sensorId) {
        return l2Cache.getHistoryAvgAmplitude(sensorId);
    }

    /**
     * 检测是否为时间维度异常
     */
    public boolean isTemporalAnomaly(String sensorId, float amplitude, double threshold) {
        return l2Cache.isAnomaly(sensorId, amplitude, threshold);
    }

    // ============ L3 空间邻域缓存操作 ============

    /**
     * 将记录添加到L3空间缓存
     */
    public void addToL3(SeismicRecord record) {
        l3Cache.addRecord(record);
    }

    /**
     * 获取邻域聚合信息
     */
    public SpatialGrid getNeighborhoodAggregate(double longitude, double latitude) {
        return l3Cache.getNeighborhoodAggregate(longitude, latitude);
    }

    /**
     * 获取邻域平均振幅
     */
    public double getNeighborhoodAvgAmplitude(double longitude, double latitude) {
        return l3Cache.getNeighborhoodAvgAmplitude(longitude, latitude);
    }

    /**
     * 检测是否为空间维度异常
     */
    public boolean isSpatialAnomaly(SeismicRecord record, double threshold) {
        return l3Cache.isRegionalAnomaly(record, threshold);
    }

    // ============ 综合处理 ============

    /**
     * 处理一条记录 (综合利用三级缓存)
     */
    public CacheProcessResult processRecord(SeismicRecord record) {
        CacheProcessResult result = new CacheProcessResult();
        String sensorId = record.getSensorId();

        // 1. 检查L1缓存
        SeismicRecord cached = getFromL1(sensorId);
        if (cached != null) {
            result.setL1Hit(true);
        }

        // 2. 更新L1缓存
        putToL1(sensorId, record);

        // 3. 更新L3空间缓存
        addToL3(record);

        // 4. 时间维度异常检测 (利用L2缓存)
        double historyAvg = getHistoryAvgAmplitude(sensorId);
        if (historyAvg > 0) {
            result.setHistoryAvgAmplitude(historyAvg);
            result.setTemporalDeviation(record.getSeismicAmplitude() / historyAvg);
        }

        // 5. 空间维度异常检测 (利用L3缓存)
        double neighborhoodAvg = getNeighborhoodAvgAmplitude(
                record.getLongitude(), record.getLatitude()
        );
        if (neighborhoodAvg > 0) {
            result.setNeighborhoodAvgAmplitude(neighborhoodAvg);
            result.setSpatialDeviation(record.getSeismicAmplitude() / neighborhoodAvg);
        }

        return result;
    }

    // ============ 自适应调优 ============

    /**
     * 执行周期性调优
     */
    public void periodicTuning() {
        long now = System.currentTimeMillis();
        if (now - lastTuningTime < config.getTuningIntervalMs()) {
            return;
        }

        // 收集各级缓存统计
        CacheStats l1Stats = l1Cache.getStats();
        CacheStats l2Stats = l2Cache.getStats();
        CacheStats l3Stats = l3Cache.getStats();

        // 计算综合命中率
        double overallHitRate = calculateOverallHitRate(l1Stats, l2Stats, l3Stats);
        capacityStrategy.recordHitRate(overallHitRate);

        // 判断是否需要调节L1容量
        if (capacityStrategy.shouldAdjust()) {
            int currentCapacity = config.getL1MaxSize();
            int newCapacity = capacityStrategy.calculateNewCapacity(currentCapacity, l1Stats);

            if (newCapacity != currentCapacity) {
                l1Cache.resizeCapacity(newCapacity);
                config.setL1MaxSize(newCapacity);
            }
        }

        // 清理过期缓存
        l1Cache.cleanExpired();

        // 输出调优报告
        printTuningReport(l1Stats, l2Stats, l3Stats, overallHitRate);

        lastTuningTime = now;
    }

    private double calculateOverallHitRate(CacheStats l1, CacheStats l2, CacheStats l3) {
        long totalHits = l1.getHitCount() + l2.getHitCount() + l3.getHitCount();
        long totalMisses = l1.getMissCount() + l2.getMissCount() + l3.getMissCount();
        long total = totalHits + totalMisses;
        return total == 0 ? 0.0 : (double) totalHits / total;
    }

    private void printTuningReport(CacheStats l1, CacheStats l2, CacheStats l3, double overallHitRate) {
        System.out.println("\n========== 缓存调优报告 ==========");
        System.out.println(l1.report());
        System.out.println(l2.report());
        System.out.println(l3.report());
        System.out.printf("综合命中率: %.2f%%%n", overallHitRate * 100);
        System.out.println(capacityStrategy.getAdjustmentReport(config.getL1MaxSize()));
        System.out.println("==================================\n");
    }

    // ============ 获取统计信息 ============

    public CacheStats getL1Stats() { return l1Cache.getStats(); }
    public CacheStats getL2Stats() { return l2Cache.getStats(); }
    public CacheStats getL3Stats() { return l3Cache.getStats(); }
    public CacheMetricsCollector getMetricsCollector() { return metricsCollector; }

    // ============ 重置统计 ============

    public void resetAllStats() {
        l1Cache.resetStats();
        l2Cache.getStats().resetPeriod();
        l3Cache.getStats().resetPeriod();
        metricsCollector.reset();
    }

    /**
     * 缓存处理结果
     */
    public static class CacheProcessResult implements Serializable {
        private boolean l1Hit;
        private double historyAvgAmplitude;
        private double neighborhoodAvgAmplitude;
        private double temporalDeviation;  // 时间维度偏差 (当前/历史平均)
        private double spatialDeviation;   // 空间维度偏差 (当前/邻域平均)

        public boolean isL1Hit() { return l1Hit; }
        public void setL1Hit(boolean l1Hit) { this.l1Hit = l1Hit; }
        public double getHistoryAvgAmplitude() { return historyAvgAmplitude; }
        public void setHistoryAvgAmplitude(double v) { this.historyAvgAmplitude = v; }
        public double getNeighborhoodAvgAmplitude() { return neighborhoodAvgAmplitude; }
        public void setNeighborhoodAvgAmplitude(double v) { this.neighborhoodAvgAmplitude = v; }
        public double getTemporalDeviation() { return temporalDeviation; }
        public void setTemporalDeviation(double v) { this.temporalDeviation = v; }
        public double getSpatialDeviation() { return spatialDeviation; }
        public void setSpatialDeviation(double v) { this.spatialDeviation = v; }

        public boolean isTemporalAnomaly(double threshold) {
            return temporalDeviation > (1 + threshold) || temporalDeviation < (1 - threshold);
        }

        public boolean isSpatialAnomaly(double threshold) {
            return spatialDeviation > (1 + threshold);
        }
    }
}