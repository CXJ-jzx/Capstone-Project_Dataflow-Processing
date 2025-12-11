package org.jzx.cache.model;

import java.io.Serializable;

/**
 * 空间网格 - 用于空间局部性缓存
 */
public class SpatialGrid implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int gridX;
    private final int gridY;
    private final String gridKey;

    // 网格内的聚合统计
    private int recordCount;
    private double sumAmplitude;
    private double maxAmplitude;
    private double minAmplitude;
    private int pWaveCount;
    private int sWaveCount;
    private long lastUpdateTime;

    public SpatialGrid(double longitude, double latitude, double gridSize) {
        this.gridX = (int) Math.floor(longitude / gridSize);
        this.gridY = (int) Math.floor(latitude / gridSize);
        this.gridKey = String.format("GRID_%d_%d", gridX, gridY);
        this.recordCount = 0;
        this.sumAmplitude = 0;
        this.maxAmplitude = Double.MIN_VALUE;
        this.minAmplitude = Double.MAX_VALUE;
        this.pWaveCount = 0;
        this.sWaveCount = 0;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public SpatialGrid(int gridX, int gridY) {
        this.gridX = gridX;
        this.gridY = gridY;
        this.gridKey = String.format("GRID_%d_%d", gridX, gridY);
        this.recordCount = 0;
        this.sumAmplitude = 0;
        this.maxAmplitude = Double.MIN_VALUE;
        this.minAmplitude = Double.MAX_VALUE;
        this.pWaveCount = 0;
        this.sWaveCount = 0;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 从经纬度计算网格Key
     */
    public static String computeGridKey(double longitude, double latitude, double gridSize) {
        int gx = (int) Math.floor(longitude / gridSize);
        int gy = (int) Math.floor(latitude / gridSize);
        return String.format("GRID_%d_%d", gx, gy);
    }

    /**
     * 添加一条记录到网格
     */
    public void addRecord(double amplitude, String phaseType) {
        this.recordCount++;
        this.sumAmplitude += amplitude;
        this.maxAmplitude = Math.max(this.maxAmplitude, amplitude);
        this.minAmplitude = Math.min(this.minAmplitude, amplitude);

        if ("P".equals(phaseType)) {
            this.pWaveCount++;
        } else if ("S".equals(phaseType)) {
            this.sWaveCount++;
        }

        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 合并另一个网格的数据
     */
    public void merge(SpatialGrid other) {
        this.recordCount += other.recordCount;
        this.sumAmplitude += other.sumAmplitude;
        this.maxAmplitude = Math.max(this.maxAmplitude, other.maxAmplitude);
        this.minAmplitude = Math.min(this.minAmplitude, other.minAmplitude);
        this.pWaveCount += other.pWaveCount;
        this.sWaveCount += other.sWaveCount;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 获取平均振幅
     */
    public double getAvgAmplitude() {
        return recordCount == 0 ? 0 : sumAmplitude / recordCount;
    }

    /**
     * 获取相邻网格的Key列表
     */
    public String[] getNeighborKeys(int radius) {
        int size = (2 * radius + 1) * (2 * radius + 1) - 1;
        String[] neighbors = new String[size];
        int idx = 0;
        for (int dx = -radius; dx <= radius; dx++) {
            for (int dy = -radius; dy <= radius; dy++) {
                if (dx == 0 && dy == 0) continue;
                neighbors[idx++] = String.format("GRID_%d_%d", gridX + dx, gridY + dy);
            }
        }
        return neighbors;
    }

    // Getters
    public int getGridX() { return gridX; }
    public int getGridY() { return gridY; }
    public String getGridKey() { return gridKey; }
    public int getRecordCount() { return recordCount; }
    public double getSumAmplitude() { return sumAmplitude; }
    public double getMaxAmplitude() { return maxAmplitude; }
    public double getMinAmplitude() { return minAmplitude; }
    public int getPWaveCount() { return pWaveCount; }
    public int getSWaveCount() { return sWaveCount; }
    public long getLastUpdateTime() { return lastUpdateTime; }
}