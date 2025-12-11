package org.jzx.version3;

import java.io.Serializable;

/**
 * 缓存中存储的特征值（增强版）
 */
public class FeatureValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private long lastTs;           // 最后更新时间
    private long lastAccessTs;     // 最后访问时间（用于LRU/语义提升）
    private float lastAmplitude;   // 上次振幅值
    private int accessCount;       // 访问次数
    private boolean important;     // 业务重要标志（如振幅高）
    private byte[] compressedData; // 可选：压缩后的数据

    public FeatureValue() {}

    public FeatureValue(long lastTs, float lastAmplitude) {
        this.lastTs = lastTs;
        this.lastAccessTs = lastTs;
        this.lastAmplitude = lastAmplitude;
        this.accessCount = 1;
        this.important = lastAmplitude > 50.0f; // 默认阈值，可改
    }

    public void onAccess(long ts) {
        this.lastAccessTs = ts;
        this.accessCount++;
    }

    public void onUpdate(long ts, float amplitude) {
        this.lastTs = ts;
        this.lastAccessTs = ts;
        this.lastAmplitude = amplitude;
        this.important = amplitude > 50.0f;
    }

    // ===== getters =====
    public long getLastTs() { return lastTs; }
    public long getLastAccessTs() { return lastAccessTs; }
    public float getLastAmplitude() { return lastAmplitude; }
    public int getAccessCount() { return accessCount; }
    public boolean isImportant() { return important; }
    public byte[] getCompressedData() { return compressedData; }
    public void setCompressedData(byte[] compressedData) { this.compressedData = compressedData; }
}
