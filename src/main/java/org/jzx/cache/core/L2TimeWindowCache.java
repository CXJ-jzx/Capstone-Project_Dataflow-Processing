package org.jzx.cache.core;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.model.CacheStats;
import org.jzx.cache.model.HistoryWindow;

import java.io.Serializable;
import java.util.*;

/**
 * L2 时间窗口缓存
 *
 * 功能：
 * - 缓存每个传感器最近N个窗口的聚合结果
 * - 支持快速获取历史基线用于异常检测
 * - 基于Flink State实现持久化
 */
public class L2TimeWindowCache implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int maxHistoryWindows;
    private final long windowSizeMs;

    // sensorId -> 按时间排序的历史窗口列表
    private final Map<String, LinkedList<HistoryWindow>> historyCache;
    private final CacheStats stats;

    public L2TimeWindowCache(CacheConfig config) {
        this.maxHistoryWindows = config.getMaxHistoryWindows();
        this.windowSizeMs = config.getWindowSizeMs();
        this.historyCache = new HashMap<>();
        this.stats = new CacheStats("L2-TimeWindow", maxHistoryWindows);
    }

    /**
     * 添加一个窗口聚合结果到缓存
     */
    public void addWindow(SeismicAggRecord aggRecord) {
        String sensorId = aggRecord.getSensorId();
        HistoryWindow hw = HistoryWindow.fromAggRecord(aggRecord);

        LinkedList<HistoryWindow> history = historyCache.computeIfAbsent(
                sensorId, k -> new LinkedList<>()
        );

        // 添加到列表头部 (最新的在前)
        history.addFirst(hw);

        // 保持最大长度
        while (history.size() > maxHistoryWindows) {
            history.removeLast();
            stats.recordEviction();
        }
    }

    /**
     * 获取传感器的最近N个历史窗口
     */
    public List<HistoryWindow> getHistory(String sensorId, int n) {
        LinkedList<HistoryWindow> history = historyCache.get(sensorId);

        if (history == null || history.isEmpty()) {
            stats.recordMiss();
            return Collections.emptyList();
        }

        stats.recordHit();
        int count = Math.min(n, history.size());
        return new ArrayList<>(history.subList(0, count));
    }

    /**
     * 获取传感器的历史平均振幅 (用作基线)
     */
    public double getHistoryAvgAmplitude(String sensorId) {
        LinkedList<HistoryWindow> history = historyCache.get(sensorId);

        if (history == null || history.isEmpty()) {
            stats.recordMiss();
            return 0.0;
        }

        stats.recordHit();
        double sum = 0;
        for (HistoryWindow hw : history) {
            sum += hw.getAvgAmplitude();
        }
        return sum / history.size();
    }

    /**
     * 获取传感器的历史最大振幅
     */
    public double getHistoryMaxAmplitude(String sensorId) {
        LinkedList<HistoryWindow> history = historyCache.get(sensorId);

        if (history == null || history.isEmpty()) {
            stats.recordMiss();
            return 0.0;
        }

        stats.recordHit();
        double max = Double.MIN_VALUE;
        for (HistoryWindow hw : history) {
            max = Math.max(max, hw.getMaxAmplitude());
        }
        return max;
    }

    /**
     * 计算当前振幅与历史基线的偏差比例
     * 返回值 > 1 表示高于历史平均
     */
    public double calculateDeviationRatio(String sensorId, float currentAmplitude) {
        double historyAvg = getHistoryAvgAmplitude(sensorId);
        if (historyAvg == 0) {
            return 1.0;
        }
        return currentAmplitude / historyAvg;
    }

    /**
     * 检测是否为异常数据 (振幅偏离历史基线超过阈值)
     */
    public boolean isAnomaly(String sensorId, float currentAmplitude, double threshold) {
        double ratio = calculateDeviationRatio(sensorId, currentAmplitude);
        return ratio > (1 + threshold) || ratio < (1 - threshold);
    }

    /**
     * 获取缓存的传感器数量
     */
    public int getSensorCount() {
        return historyCache.size();
    }

    /**
     * 获取统计信息
     */
    public CacheStats getStats() {
        int totalWindows = historyCache.values().stream()
                .mapToInt(LinkedList::size)
                .sum();
        stats.setCurrentSize(totalWindows);
        return stats;
    }

    /**
     * 清理指定传感器的历史
     */
    public void clearSensorHistory(String sensorId) {
        historyCache.remove(sensorId);
    }

    /**
     * 清空所有缓存
     */
    public void clear() {
        historyCache.clear();
    }

    /**
     * 获取所有缓存数据 (用于State快照)
     */
    public Map<String, LinkedList<HistoryWindow>> getAllData() {
        return historyCache;
    }

    /**
     * 恢复缓存数据 (用于State恢复)
     */
    public void restoreData(Map<String, LinkedList<HistoryWindow>> data) {
        historyCache.clear();
        historyCache.putAll(data);
    }
}