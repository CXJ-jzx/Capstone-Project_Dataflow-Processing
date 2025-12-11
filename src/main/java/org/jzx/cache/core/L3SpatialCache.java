package org.jzx.cache.core;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.model.CacheStats;
import org.jzx.cache.model.SpatialGrid;

import java.io.Serializable;
import java.util.*;

/**
 * L3 空间邻域缓存
 *
 * 功能：
 * - 按地理位置将传感器划分到网格
 * - 缓存每个网格的聚合统计信息
 * - 支持查询相邻网格的数据 (空间局部性)
 */
public class L3SpatialCache implements Serializable {
    private static final long serialVersionUID = 1L;

    private final double gridSize;
    private final int neighborRadius;
    private final int maxGrids;

    // gridKey -> SpatialGrid
    private final Map<String, SpatialGrid> gridCache;
    // gridKey -> 最后更新时间 (用于LRU淘汰)
    private final Map<String, Long> gridAccessTime;
    private final CacheStats stats;

    public L3SpatialCache(CacheConfig config) {
        this.gridSize = config.getSpatialGridSize();
        this.neighborRadius = config.getNeighborRadius();
        this.maxGrids = config.getMaxGrids();
        this.gridCache = new HashMap<>();
        this.gridAccessTime = new HashMap<>();
        this.stats = new CacheStats("L3-Spatial", maxGrids);
    }

    /**
     * 添加一条记录到对应的空间网格
     */
    public void addRecord(SeismicRecord record) {
        String gridKey = SpatialGrid.computeGridKey(
                record.getLongitude(),
                record.getLatitude(),
                gridSize
        );

        SpatialGrid grid = gridCache.get(gridKey);
        if (grid == null) {
            // 检查容量
            if (gridCache.size() >= maxGrids) {
                evictOldestGrid();
            }
            grid = new SpatialGrid(record.getLongitude(), record.getLatitude(), gridSize);
            gridCache.put(gridKey, grid);
        }

        grid.addRecord(record.getSeismicAmplitude(), record.getPhaseType());
        gridAccessTime.put(gridKey, System.currentTimeMillis());
    }

    /**
     * 获取指定网格的统计信息
     */
    public SpatialGrid getGrid(String gridKey) {
        SpatialGrid grid = gridCache.get(gridKey);
        if (grid == null) {
            stats.recordMiss();
            return null;
        }
        stats.recordHit();
        gridAccessTime.put(gridKey, System.currentTimeMillis());
        return grid;
    }

    /**
     * 获取指定位置所在网格及其邻域的聚合信息
     */
    public SpatialGrid getNeighborhoodAggregate(double longitude, double latitude) {
        String centerKey = SpatialGrid.computeGridKey(longitude, latitude, gridSize);
        SpatialGrid centerGrid = gridCache.get(centerKey);

        if (centerGrid == null) {
            stats.recordMiss();
            return null;
        }

        stats.recordHit();

        // 创建聚合结果
        SpatialGrid aggregate = new SpatialGrid(
                centerGrid.getGridX(),
                centerGrid.getGridY()
        );
        aggregate.merge(centerGrid);

        // 合并邻域网格
        String[] neighborKeys = centerGrid.getNeighborKeys(neighborRadius);
        for (String neighborKey : neighborKeys) {
            SpatialGrid neighbor = gridCache.get(neighborKey);
            if (neighbor != null) {
                aggregate.merge(neighbor);
            }
        }

        return aggregate;
    }

    /**
     * 获取指定位置邻域的平均振幅
     */
    public double getNeighborhoodAvgAmplitude(double longitude, double latitude) {
        SpatialGrid aggregate = getNeighborhoodAggregate(longitude, latitude);
        return aggregate != null ? aggregate.getAvgAmplitude() : 0.0;
    }

    /**
     * 检测是否为区域异常 (振幅显著高于邻域平均)
     */
    public boolean isRegionalAnomaly(SeismicRecord record, double threshold) {
        double neighborhoodAvg = getNeighborhoodAvgAmplitude(
                record.getLongitude(),
                record.getLatitude()
        );

        if (neighborhoodAvg == 0) {
            return false;
        }

        double ratio = record.getSeismicAmplitude() / neighborhoodAvg;
        return ratio > (1 + threshold);
    }

    /**
     * 淘汰最久未访问的网格 (LRU)
     */
    private void evictOldestGrid() {
        if (gridAccessTime.isEmpty()) {
            return;
        }

        String oldestKey = null;
        long oldestTime = Long.MAX_VALUE;

        for (Map.Entry<String, Long> entry : gridAccessTime.entrySet()) {
            if (entry.getValue() < oldestTime) {
                oldestTime = entry.getValue();
                oldestKey = entry.getKey();
            }
        }

        if (oldestKey != null) {
            gridCache.remove(oldestKey);
            gridAccessTime.remove(oldestKey);
            stats.recordEviction();
        }
    }

    /**
     * 获取统计信息
     */
    public CacheStats getStats() {
        stats.setCurrentSize(gridCache.size());
        return stats;
    }

    /**
     * 清空缓存
     */
    public void clear() {
        gridCache.clear();
        gridAccessTime.clear();
    }

    /**
     * 获取所有网格数据 (用于State快照)
     */
    public Map<String, SpatialGrid> getAllGrids() {
        return gridCache;
    }

    /**
     * 恢复网格数据 (用于State恢复)
     */
    public void restoreGrids(Map<String, SpatialGrid> data) {
        gridCache.clear();
        gridCache.putAll(data);
        // 重置访问时间
        long now = System.currentTimeMillis();
        for (String key : data.keySet()) {
            gridAccessTime.put(key, now);
        }
    }
}