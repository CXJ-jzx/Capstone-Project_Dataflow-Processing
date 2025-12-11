package org.jzx.cache.operator;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.model.HistoryWindow;

import java.util.LinkedList;

/**
 * 带缓存的窗口聚合函数
 *
 * 功能：
 * 1. 执行窗口内聚合计算
 * 2. 将聚合结果存入L2时间窗口缓存 (Flink State)
 * 3. 利用历史窗口信息计算与基线的偏差
 */
public class CachedWindowAggFunction
        extends ProcessWindowFunction<SeismicRecord, SeismicAggRecord, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final CacheConfig config;

    // Flink State - 每个传感器的历史窗口缓存
    private ValueState<LinkedList<HistoryWindow>> historyState;

    // 统计计数器
    private transient long windowCount;
    private transient long cacheHitCount;
    private transient long cacheMissCount;

    public CachedWindowAggFunction(CacheConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化历史窗口State
        ValueStateDescriptor<LinkedList<HistoryWindow>> historyDesc =
                new ValueStateDescriptor<>(
                        "history-windows",
                        TypeInformation.of(new TypeHint<LinkedList<HistoryWindow>>() {})
                );
        historyState = getRuntimeContext().getState(historyDesc);

        this.windowCount = 0;
        this.cacheHitCount = 0;
        this.cacheMissCount = 0;

        System.out.println("✅ CachedWindowAggFunction 初始化完成");
    }

    @Override
    public void process(String sensorId, Context context,
                        Iterable<SeismicRecord> records,
                        Collector<SeismicAggRecord> out) throws Exception {

        windowCount++;
        TimeWindow window = context.window();

        // 1. 聚合窗口内数据
        int count = 0;
        float sumAmplitude = 0;
        float maxAmplitude = Float.MIN_VALUE;
        float minAmplitude = Float.MAX_VALUE;
        int pCount = 0, sCount = 0, nCount = 0;

        for (SeismicRecord record : records) {
            count++;
            float amplitude = record.getSeismicAmplitude();
            sumAmplitude += amplitude;
            maxAmplitude = Math.max(maxAmplitude, amplitude);
            minAmplitude = Math.min(minAmplitude, amplitude);

            switch (record.getPhaseType()) {
                case "P": pCount++; break;
                case "S": sCount++; break;
                default: nCount++; break;
            }
        }

        if (count == 0) {
            return; // 空窗口，不输出
        }

        float avgAmplitude = sumAmplitude / count;

        // 2. 构建聚合结果
        SeismicAggRecord aggRecord = SeismicAggRecord.newBuilder()
                .setSensorId(sensorId)
                .setWindowStartTs(window.getStart())
                .setWindowEndTs(window.getEnd())
                .setAvgAmplitude(avgAmplitude)
                .setMaxAmplitude(maxAmplitude)
                .setMinAmplitude(minAmplitude)
                .setPPhaseCount(pCount)
                .setSPhaseCount(sCount)
                .setNPhaseCount(nCount)
                .build();

        // 3. 获取历史窗口缓存
        LinkedList<HistoryWindow> history = historyState.value();
        double historyAvg = 0;
        double deviation = 0;

        if (history != null && !history.isEmpty()) {
            cacheHitCount++;
            // 计算历史平均振幅
            double sum = 0;
            for (HistoryWindow hw : history) {
                sum += hw.getAvgAmplitude();
            }
            historyAvg = sum / history.size();
            deviation = historyAvg > 0 ? (avgAmplitude - historyAvg) / historyAvg : 0;
        } else {
            cacheMissCount++;
            history = new LinkedList<>();
        }

        // 4. 将当前窗口加入历史缓存
        HistoryWindow currentWindow = HistoryWindow.fromAggRecord(aggRecord);
        history.addFirst(currentWindow);

        // 保持最大历史窗口数量
        while (history.size() > config.getMaxHistoryWindows()) {
            history.removeLast();
        }

        // 5. 更新State
        historyState.update(history);

        // 6. 输出聚合结果
        out.collect(aggRecord);

        // 7. 周期性输出统计 (每100个窗口)
        if (windowCount % 100 == 0) {
            double hitRate = (cacheHitCount + cacheMissCount) > 0
                    ? (double) cacheHitCount / (cacheHitCount + cacheMissCount) * 100 : 0;
            System.out.printf("[CachedWindowAgg] 窗口数: %d | 缓存命中率: %.2f%% | " +
                            "最近偏差: %.2f%%%n",
                    windowCount, hitRate, deviation * 100);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        double hitRate = (cacheHitCount + cacheMissCount) > 0
                ? (double) cacheHitCount / (cacheHitCount + cacheMissCount) * 100 : 0;
        System.out.printf("[CachedWindowAgg] 最终统计 - 窗口数: %d | 命中: %d | 未命中: %d | 命中率: %.2f%%%n",
                windowCount, cacheHitCount, cacheMissCount, hitRate);
    }
}