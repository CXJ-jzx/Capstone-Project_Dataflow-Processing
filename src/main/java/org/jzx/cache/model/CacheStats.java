package org.jzx.cache.model;

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

/**
 * 缓存统计信息
 */
public class CacheStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private String cacheName;
    private int currentSize;
    private int maxSize;

    // 使用 long 替代 LongAdder 以便序列化
    private long hitCount;
    private long missCount;
    private long evictionCount;

    private long lastResetTime;

    public CacheStats(String cacheName, int maxSize) {
        this.cacheName = cacheName;
        this.maxSize = maxSize;
        this.currentSize = 0;
        this.hitCount = 0;
        this.missCount = 0;
        this.evictionCount = 0;
        this.lastResetTime = System.currentTimeMillis();
    }

    public synchronized void recordHit() {
        hitCount++;
    }

    public synchronized void recordMiss() {
        missCount++;
    }

    public synchronized void recordEviction() {
        evictionCount++;
    }

    public void setCurrentSize(int size) {
        this.currentSize = size;
    }

    public double getHitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }

    public long getTotalRequests() {
        return hitCount + missCount;
    }

    /**
     * 重置统计周期
     */
    public synchronized void resetPeriod() {
        this.hitCount = 0;
        this.missCount = 0;
        this.evictionCount = 0;
        this.lastResetTime = System.currentTimeMillis();
    }

    /**
     * 生成统计报告
     */
    public String report() {
        return String.format(
                "[%s] Size: %d/%d | Hits: %d | Misses: %d | HitRate: %.2f%% | Evictions: %d",
                cacheName, currentSize, maxSize, hitCount, missCount,
                getHitRate() * 100, evictionCount
        );
    }

    // Getters
    public String getCacheName() { return cacheName; }
    public int getCurrentSize() { return currentSize; }
    public int getMaxSize() { return maxSize; }
    public long getHitCount() { return hitCount; }
    public long getMissCount() { return missCount; }
    public long getEvictionCount() { return evictionCount; }
    public void setMaxSize(int maxSize) { this.maxSize = maxSize; }
}