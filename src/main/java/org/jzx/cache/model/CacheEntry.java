package org.jzx.cache.model;

import java.io.Serializable;

/**
 * 缓存条目 - 包装缓存的值和元数据
 */
public class CacheEntry<V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;
    private V value;
    private final long createTime;
    private long lastAccessTime;
    private int accessCount;
    private final long ttl;

    // 用于LRU-K：记录最近K次访问时间
    private long[] recentAccessTimes;
    private int accessIndex;

    public CacheEntry(String key, V value, long ttl, int k) {
        this.key = key;
        this.value = value;
        this.createTime = System.currentTimeMillis();
        this.lastAccessTime = this.createTime;
        this.accessCount = 1;
        this.ttl = ttl;
        this.recentAccessTimes = new long[k];
        this.recentAccessTimes[0] = this.createTime;
        this.accessIndex = 1;
    }

    /**
     * 记录一次访问
     */
    public V access() {
        this.lastAccessTime = System.currentTimeMillis();
        this.accessCount++;

        // 更新LRU-K访问历史
        if (accessIndex < recentAccessTimes.length) {
            recentAccessTimes[accessIndex++] = this.lastAccessTime;
        } else {
            // 滑动窗口：移除最老的，添加最新的
            System.arraycopy(recentAccessTimes, 1, recentAccessTimes, 0, recentAccessTimes.length - 1);
            recentAccessTimes[recentAccessTimes.length - 1] = this.lastAccessTime;
        }

        return this.value;
    }

    /**
     * 获取第K次访问时间 (用于LRU-K淘汰决策)
     */
    public long getKthAccessTime(int k) {
        if (accessIndex >= k) {
            return recentAccessTimes[recentAccessTimes.length - k];
        } else {
            // 访问次数不足K次，返回0表示优先淘汰
            return 0;
        }
    }

    /**
     * 检查是否过期
     */
    public boolean isExpired() {
        return System.currentTimeMillis() - createTime > ttl;
    }

    /**
     * 计算访问频率 (用于LFU)
     */
    public double getAccessFrequency() {
        long aliveTime = System.currentTimeMillis() - createTime;
        if (aliveTime <= 0) return accessCount;
        return (double) accessCount * 1000 / aliveTime; // 每秒访问次数
    }

    /**
     * 计算时间衰减后的访问频率 (用于TD-LFU)
     */
    public double getDecayedFrequency(double decayFactor) {
        long age = System.currentTimeMillis() - lastAccessTime;
        double decayedCount = accessCount * Math.pow(decayFactor, age / 1000.0);
        return decayedCount;
    }

    // Getters
    public String getKey() { return key; }
    public V getValue() { return value; }
    public void setValue(V value) { this.value = value; }
    public long getCreateTime() { return createTime; }
    public long getLastAccessTime() { return lastAccessTime; }
    public int getAccessCount() { return accessCount; }
    public long getTtl() { return ttl; }
}