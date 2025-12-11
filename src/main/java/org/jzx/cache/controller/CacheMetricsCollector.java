package org.jzx.cache.controller;

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

/**
 * 缓存指标收集器
 */
public class CacheMetricsCollector implements Serializable {
    private static final long serialVersionUID = 1L;

    private long totalRecords;
    private long processedRecords;
    private long totalLatencyMs;
    private long maxLatencyMs;
    private long minLatencyMs;

    private long lastReportTime;
    private long recordsSinceLastReport;

    public CacheMetricsCollector() {
        reset();
    }

    public synchronized void recordProcessed() {
        processedRecords++;
        recordsSinceLastReport++;
    }

    public synchronized void recordLatency(long latencyMs) {
        totalLatencyMs += latencyMs;
        maxLatencyMs = Math.max(maxLatencyMs, latencyMs);
        if (minLatencyMs == 0 || latencyMs < minLatencyMs) {
            minLatencyMs = latencyMs;
        }
    }

    public synchronized void incrementTotal() {
        totalRecords++;
    }

    public double getAvgLatency() {
        return processedRecords == 0 ? 0 : (double) totalLatencyMs / processedRecords;
    }

    public long getThroughput() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastReportTime;
        if (elapsed <= 0) return 0;
        return recordsSinceLastReport * 1000 / elapsed;
    }

    public synchronized void reset() {
        totalRecords = 0;
        processedRecords = 0;
        totalLatencyMs = 0;
        maxLatencyMs = 0;
        minLatencyMs = 0;
        lastReportTime = System.currentTimeMillis();
        recordsSinceLastReport = 0;
    }

    public synchronized void resetPeriod() {
        lastReportTime = System.currentTimeMillis();
        recordsSinceLastReport = 0;
    }

    public String report() {
        return String.format(
                "[指标] 总记录: %d | 已处理: %d | 吞吐量: %d/s | 平均延迟: %.2fms | 最大延迟: %dms",
                totalRecords, processedRecords, getThroughput(), getAvgLatency(), maxLatencyMs
        );
    }

    // Getters
    public long getTotalRecords() { return totalRecords; }
    public long getProcessedRecords() { return processedRecords; }
    public long getMaxLatencyMs() { return maxLatencyMs; }
    public long getMinLatencyMs() { return minLatencyMs; }
}