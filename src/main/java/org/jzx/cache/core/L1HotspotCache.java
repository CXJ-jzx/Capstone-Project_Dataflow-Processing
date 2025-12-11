package org.jzx.cache.core;

import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.model.CacheEntry;
import org.jzx.cache.model.CacheStats;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * L1 热点缓存 - 基于 LRU-K 算法
 *
 * LRU-K 核心思想：
 * - 记录每个缓存项最近K次访问时间
 * - 淘汰时选择第K次访问时间最久远的条目
 * - 可有效区分偶发访问和真正热点，避免缓存污染
 *
 * @param <V> 缓存值类型
 */
public class L1HotspotCache<V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int k;  // LRU-K 中的 K
    private int maxSize;
    private final long defaultTtl;

    // 主存储
    private final ConcurrentHashMap<String, CacheEntry<V>> cache;
    // 统计信息
    private final CacheStats stats;

    public L1HotspotCache(CacheConfig config) {
        this.k = config.getLruK();
        this.maxSize = config.getL1MaxSize();
        this.defaultTtl = config.getL1TtlMs();
        this.cache = new ConcurrentHashMap<>();
        this.stats = new CacheStats("L1-Hotspot", maxSize);
    }

    /**
     * 获取缓存
     */
    public V get(String key) {
        CacheEntry<V> entry = cache.get(key);

        if (entry == null) {
            stats.recordMiss();
            return null;
        }

        if (entry.isExpired()) {
            cache.remove(key);
            stats.recordMiss();
            stats.recordEviction();
            return null;
        }

        stats.recordHit();
        return entry.access();
    }

    /**
     * 放入缓存
     */
    public void put(String key, V value) {
        put(key, value, defaultTtl);
    }

    public void put(String key, V value, long ttl) {
        // 如果已存在，更新值
        CacheEntry<V> existing = cache.get(key);
        if (existing != null) {
            existing.setValue(value);
            existing.access();
            return;
        }

        // 检查容量，必要时淘汰
        if (cache.size() >= maxSize) {
            evict();
        }

        CacheEntry<V> entry = new CacheEntry<>(key, value, ttl, k);
        cache.put(key, entry);
        stats.setCurrentSize(cache.size());
    }

    /**
     * LRU-K 淘汰策略
     * 淘汰第K次访问时间最久远的条目
     */
    private void evict() {
        String victimKey = null;
        long oldestKthAccess = Long.MAX_VALUE;

        // 首先清理过期条目
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, CacheEntry<V>> entry : cache.entrySet()) {
            if (entry.getValue().isExpired()) {
                expiredKeys.add(entry.getKey());
            }
        }
        for (String key : expiredKeys) {
            cache.remove(key);
            stats.recordEviction();
        }

        // 如果清理后容量足够，直接返回
        if (cache.size() < maxSize) {
            stats.setCurrentSize(cache.size());
            return;
        }

        // LRU-K 淘汰
        for (Map.Entry<String, CacheEntry<V>> entry : cache.entrySet()) {
            long kthAccessTime = entry.getValue().getKthAccessTime(k);
            if (kthAccessTime < oldestKthAccess) {
                oldestKthAccess = kthAccessTime;
                victimKey = entry.getKey();
            }
        }

        if (victimKey != null) {
            cache.remove(victimKey);
            stats.recordEviction();
        }

        stats.setCurrentSize(cache.size());
    }

    /**
     * 批量淘汰 (用于容量缩减时)
     */
    public void evictBatch(int count) {
        // 按第K次访问时间排序
        List<Map.Entry<String, CacheEntry<V>>> entries = new ArrayList<>(cache.entrySet());
        entries.sort(Comparator.comparingLong(e -> e.getValue().getKthAccessTime(k)));

        int evicted = 0;
        for (Map.Entry<String, CacheEntry<V>> entry : entries) {
            if (evicted >= count) break;
            cache.remove(entry.getKey());
            stats.recordEviction();
            evicted++;
        }
        stats.setCurrentSize(cache.size());
    }

    /**
     * 检查是否存在
     */
    public boolean contains(String key) {
        CacheEntry<V> entry = cache.get(key);
        return entry != null && !entry.isExpired();
    }

    /**
     * 清理所有过期条目
     */
    public void cleanExpired() {
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, CacheEntry<V>> entry : cache.entrySet()) {
            if (entry.getValue().isExpired()) {
                expiredKeys.add(entry.getKey());
            }
        }
        for (String key : expiredKeys) {
            cache.remove(key);
            stats.recordEviction();
        }
        stats.setCurrentSize(cache.size());
    }

    /**
     * 动态调整容量
     */
    public void resizeCapacity(int newMaxSize) {
        if (newMaxSize < this.maxSize && cache.size() > newMaxSize) {
            // 需要缩容
            evictBatch(cache.size() - newMaxSize);
        }
        this.maxSize = newMaxSize;
        stats.setMaxSize(newMaxSize);
    }

    /**
     * 获取统计信息
     */
    public CacheStats getStats() {
        stats.setCurrentSize(cache.size());
        return stats;
    }

    /**
     * 重置统计
     */
    public void resetStats() {
        stats.resetPeriod();
    }

    /**
     * 获取当前大小
     */
    public int size() {
        return cache.size();
    }

    /**
     * 清空缓存
     */
    public void clear() {
        cache.clear();
        stats.setCurrentSize(0);
    }
}