package org.jzx.cache.strategy;

import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.model.CacheStats;

import java.io.Serializable;

/**
 * 自适应容量调节策略
 *
 * 核心逻辑：
 * - 命中率低于目标 → 增大缓存容量
 * - 命中率高于目标 + 内存富余 → 可以缩小缓存容量
 * - 使用滑动窗口平均避免频繁调整
 */
public class AdaptiveCapacityStrategy implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CacheConfig config;

    // 历史命中率记录 (滑动窗口)
    private final double[] hitRateHistory;
    private int historyIndex;
    private int historyCount;

    // 上次调节时间
    private long lastAdjustTime;

    public AdaptiveCapacityStrategy(CacheConfig config) {
        this.config = config;
        this.hitRateHistory = new double[10]; // 保留最近10次记录
        this.historyIndex = 0;
        this.historyCount = 0;
        this.lastAdjustTime = System.currentTimeMillis();
    }

    /**
     * 记录当前命中率
     */
    public void recordHitRate(double hitRate) {
        hitRateHistory[historyIndex] = hitRate;
        historyIndex = (historyIndex + 1) % hitRateHistory.length;
        if (historyCount < hitRateHistory.length) {
            historyCount++;
        }
    }

    /**
     * 计算滑动窗口平均命中率
     */
    public double getAverageHitRate() {
        if (historyCount == 0) {
            return 0.0;
        }
        double sum = 0;
        for (int i = 0; i < historyCount; i++) {
            sum += hitRateHistory[i];
        }
        return sum / historyCount;
    }

    /**
     * 判断是否需要调节
     */
    public boolean shouldAdjust() {
        // 至少积累5次记录再判断
        if (historyCount < 5) {
            return false;
        }

        // 距离上次调节至少间隔一定时间
        if (System.currentTimeMillis() - lastAdjustTime < config.getTuningIntervalMs()) {
            return false;
        }

        double avgHitRate = getAverageHitRate();
        double target = config.getTargetHitRate();
        double tolerance = config.getHitRateTolerance();

        // 命中率偏离目标超过容忍区间
        return Math.abs(avgHitRate - target) > tolerance;
    }

    /**
     * 计算新的缓存容量
     */
    public int calculateNewCapacity(int currentCapacity, CacheStats stats) {
        double avgHitRate = getAverageHitRate();
        double target = config.getTargetHitRate();
        double tolerance = config.getHitRateTolerance();
        double ratio = config.getAdjustmentRatio();

        int newCapacity = currentCapacity;

        if (avgHitRate < target - tolerance) {
            // 命中率过低，增大容量
            int increase = (int) (currentCapacity * ratio);
            increase = Math.max(increase, 100); // 至少增加100
            newCapacity = Math.min(currentCapacity + increase, config.getMaxCacheSize());

            System.out.printf("📈 [容量调节] 命中率 %.2f%% 低于目标 %.2f%%，增大容量: %d → %d%n",
                    avgHitRate * 100, target * 100, currentCapacity, newCapacity);

        } else if (avgHitRate > target + tolerance) {
            // 命中率过高，可以适当缩小容量 (更保守)
            int decrease = (int) (currentCapacity * ratio * 0.5);
            newCapacity = Math.max(currentCapacity - decrease, config.getMinCacheSize());

            System.out.printf("📉 [容量调节] 命中率 %.2f%% 高于目标 %.2f%%，缩小容量: %d → %d%n",
                    avgHitRate * 100, target * 100, currentCapacity, newCapacity);
        }

        lastAdjustTime = System.currentTimeMillis();
        return newCapacity;
    }

    /**
     * 根据数据流量调整策略 (与弹性资源调度联动)
     */
    public int adjustForThroughput(int currentCapacity, long currentThroughput, long baseThroughput) {
        if (baseThroughput == 0) {
            return currentCapacity;
        }

        double throughputRatio = (double) currentThroughput / baseThroughput;

        if (throughputRatio > 2.0) {
            // 流量翻倍以上，考虑增大缓存
            int increase = (int) (currentCapacity * 0.3);
            return Math.min(currentCapacity + increase, config.getMaxCacheSize());
        } else if (throughputRatio < 0.5) {
            // 流量降低一半以下，可以缩小缓存
            int decrease = (int) (currentCapacity * 0.2);
            return Math.max(currentCapacity - decrease, config.getMinCacheSize());
        }

        return currentCapacity;
    }

    /**
     * 获取调节建议报告
     */
    public String getAdjustmentReport(int currentCapacity) {
        double avgHitRate = getAverageHitRate();
        return String.format(
                "[容量调节策略] 当前容量: %d | 平均命中率: %.2f%% | 目标命中率: %.2f%% | 建议: %s",
                currentCapacity,
                avgHitRate * 100,
                config.getTargetHitRate() * 100,
                shouldAdjust() ? "需要调节" : "保持当前"
        );
    }
}