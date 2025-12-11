package org.jzx.cache.config;

import java.io.Serializable;

/**
 * 缓存配置类
 */
public class CacheConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // ============ L1 热点缓存配置 ============
    /** L1缓存最大条目数 */
    private int l1MaxSize = 5000;
    /** LRU-K 中的 K 值 (推荐2-3) */
    private int lruK = 2;
    /** L1缓存默认TTL (毫秒) */
    private long l1TtlMs = 60000;

    // ============ L2 时间窗口缓存配置 ============
    /** 保留的历史窗口数量 */
    private int maxHistoryWindows = 10;
    /** 窗口大小 (毫秒) */
    private long windowSizeMs = 1000;

    // ============ L3 空间邻域缓存配置 ============
    /** 空间网格大小 (经纬度度数，约0.01度≈1公里) */
    private double spatialGridSize = 0.01;
    /** 邻域半径 (网格数) */
    private int neighborRadius = 1;
    /** 最大缓存网格数 */
    private int maxGrids = 1000;

    // ============ 自适应调节配置 ============
    /** 目标命中率 */
    private double targetHitRate = 0.70;
    /** 命中率容忍区间 */
    private double hitRateTolerance = 0.05;
    /** 最小缓存容量 */
    private int minCacheSize = 1000;
    /** 最大缓存容量 */
    private int maxCacheSize = 50000;
    /** 调节检查周期 (毫秒) */
    private long tuningIntervalMs = 30000;
    /** 每次调节的比例 */
    private double adjustmentRatio = 0.2;

    // ============ 淘汰策略配置 ============
    /** 淘汰策略类型: LRU_K, TD_LFU, HYBRID */
    private String evictionStrategy = "LRU_K";
    /** 时间衰减因子 (用于TD-LFU) */
    private double timeDecayFactor = 0.95;

    // ============ 构造函数和Builder ============
    public CacheConfig() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private CacheConfig config = new CacheConfig();

        public Builder l1MaxSize(int size) { config.l1MaxSize = size; return this; }
        public Builder lruK(int k) { config.lruK = k; return this; }
        public Builder l1TtlMs(long ttl) { config.l1TtlMs = ttl; return this; }
        public Builder maxHistoryWindows(int n) { config.maxHistoryWindows = n; return this; }
        public Builder windowSizeMs(long ms) { config.windowSizeMs = ms; return this; }
        public Builder spatialGridSize(double size) { config.spatialGridSize = size; return this; }
        public Builder neighborRadius(int r) { config.neighborRadius = r; return this; }
        public Builder maxGrids(int n) { config.maxGrids = n; return this; }
        public Builder targetHitRate(double rate) { config.targetHitRate = rate; return this; }
        public Builder evictionStrategy(String s) { config.evictionStrategy = s; return this; }

        public CacheConfig build() { return config; }
    }

    // ============ Getters ============
    public int getL1MaxSize() { return l1MaxSize; }
    public int getLruK() { return lruK; }
    public long getL1TtlMs() { return l1TtlMs; }
    public int getMaxHistoryWindows() { return maxHistoryWindows; }
    public long getWindowSizeMs() { return windowSizeMs; }
    public double getSpatialGridSize() { return spatialGridSize; }
    public int getNeighborRadius() { return neighborRadius; }
    public int getMaxGrids() { return maxGrids; }
    public double getTargetHitRate() { return targetHitRate; }
    public double getHitRateTolerance() { return hitRateTolerance; }
    public int getMinCacheSize() { return minCacheSize; }
    public int getMaxCacheSize() { return maxCacheSize; }
    public long getTuningIntervalMs() { return tuningIntervalMs; }
    public double getAdjustmentRatio() { return adjustmentRatio; }
    public String getEvictionStrategy() { return evictionStrategy; }
    public double getTimeDecayFactor() { return timeDecayFactor; }

    // ============ Setters (用于动态调节) ============
    public void setL1MaxSize(int l1MaxSize) { this.l1MaxSize = l1MaxSize; }
}