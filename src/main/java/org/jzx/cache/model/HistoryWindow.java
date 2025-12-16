package org.jzx.cache.model;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import java.io.Serializable;

/**
 * 历史窗口记录 - 用于时间局部性缓存
 */
public class HistoryWindow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String sensorId;
    private final long windowStart;
    private final long windowEnd;

    // 聚合值
    private float avgAmplitude;
    private float maxAmplitude;
    private float minAmplitude;
    private int pPhaseCount;
    private int sPhaseCount;
    private int nPhaseCount;
    private int totalCount;

    public HistoryWindow(String sensorId, long windowStart, long windowEnd) {
        this.sensorId = sensorId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.avgAmplitude = 0;
        this.maxAmplitude = Float.MIN_VALUE;
        this.minAmplitude = Float.MAX_VALUE;
        this.pPhaseCount = 0;
        this.sPhaseCount = 0;
        this.nPhaseCount = 0;
        this.totalCount = 0;
    }

    /**
     * 从聚合结果构建
     */
    public static HistoryWindow fromAggRecord(SeismicAggRecord aggRecord) {
        HistoryWindow hw = new HistoryWindow(
                aggRecord.getSensorId(),
                aggRecord.getWindowStartTs(),
                aggRecord.getWindowEndTs()
        );
        hw.avgAmplitude = aggRecord.getAvgAmplitude();
        hw.maxAmplitude = aggRecord.getMaxAmplitude();
        hw.minAmplitude = aggRecord.getMinAmplitude();
        hw.pPhaseCount = aggRecord.getPPhaseCount();
        hw.sPhaseCount = aggRecord.getSPhaseCount();
        hw.nPhaseCount = aggRecord.getNPhaseCount();
        hw.totalCount = hw.pPhaseCount + hw.sPhaseCount + hw.nPhaseCount;
        return hw;
    }

    /**
     * 转换为聚合记录
     */
    public SeismicAggRecord toAggRecord() {
        return SeismicAggRecord.newBuilder()
                .setSensorId(sensorId)
                .setWindowStartTs(windowStart)
                .setWindowEndTs(windowEnd)
                .setAvgAmplitude(avgAmplitude)
                .setMaxAmplitude(maxAmplitude)
                .setMinAmplitude(minAmplitude)
                .setPPhaseCount(pPhaseCount)
                .setSPhaseCount(sPhaseCount)
                .setNPhaseCount(nPhaseCount)
                .build();
    }


    /**
     * 计算与历史基线的偏差 (用于异常检测)
     * */
    public double calculateDeviation(float currentAmplitude) {
        if (avgAmplitude == 0) return 0;
        return Math.abs(currentAmplitude - avgAmplitude) / Math.abs(avgAmplitude);
    }

    // Getters
    public String getSensorId() { return sensorId; }
    public long getWindowStart() { return windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public float getAvgAmplitude() { return avgAmplitude; }
    public float getMaxAmplitude() { return maxAmplitude; }
    public float getMinAmplitude() { return minAmplitude; }
    public int getPPhaseCount() { return pPhaseCount; }
    public int getSPhaseCount() { return sPhaseCount; }
    public int getNPhaseCount() { return nPhaseCount; }
    public int getTotalCount() { return totalCount; }
}