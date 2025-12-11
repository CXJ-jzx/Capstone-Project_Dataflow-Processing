package org.jzx.version3;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * L1 Cache (Non-Persistent, JVM Heap)
 * - 基于 LinkedHashMap 的 LRU 缓存 (accessOrder = true)
 * - 注意：该缓存为 TaskManager JVM 本地缓存，非 Flink Checkpoint 的一部分
 */
public class LruMap<K, V> extends LinkedHashMap<K, V> {
    private int capacity;

    public LruMap(int capacity) {
        super(capacity, 0.75f, true); // accessOrder = true
        this.capacity = Math.max(1, capacity);
    }

    public void setCapacity(int newCap) {
        this.capacity = Math.max(1, newCap);
        // 可选：触发同步回收以立刻满足新容量
        evictIfOverLimit();
    }

    public int getCapacity() {
        return this.capacity;
    }

    // 手动触发淘汰，遍历直到 size <= capacity
    public void evictIfOverLimit() {
        while (size() > capacity) {
            // remove eldest entry
            Map.Entry<K, V> eldest = this.entrySet().iterator().next();
            if (eldest != null) {
                this.remove(eldest.getKey());
            } else {
                break;
            }
        }
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > capacity;
    }
}
