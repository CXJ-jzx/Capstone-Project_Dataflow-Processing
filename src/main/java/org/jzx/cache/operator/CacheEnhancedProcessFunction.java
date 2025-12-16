package org.jzx.cache.operator;

import com.example.seismic.SeismicDataProto.SeismicAggRecord;
import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jzx.cache.config.CacheConfig;
import org.jzx.cache.controller.CacheController;
import org.jzx.cache.controller.CacheController.CacheProcessResult;
import org.jzx.cache.model.HistoryWindow;
import org.jzx.cache.model.SpatialGrid;

import java.util.LinkedList;

/**
 * 缓存增强的处理函数
 *
 * 功能：
 * 1. 利用L1缓存进行热点传感器识别
 * 2. 利用L2缓存进行时间维度异常检测
 * 3. 利用L3缓存进行空间维度异常检测
 * 4. 周期性执行缓存调优
 */
public class CacheEnhancedProcessFunction
        extends KeyedProcessFunction<String, SeismicRecord, SeismicRecord> {

    private static final long serialVersionUID = 1L;

    private final CacheConfig config;

    // 缓存控制器 (transient, 在open中初始化)
    private transient CacheController cacheController;

    // Flink State - L2时间窗口缓存持久化
    private MapState<String, LinkedList<HistoryWindow>> l2WindowState;

    // Flink State - L3空间网格缓存持久化
    private MapState<String, SpatialGrid> l3GridState;

    // 处理计数器
    private transient long processedCount;
    private transient long lastTuningCount;
    private transient long lastLogCount;
    private transient long outputCount;      // 🆕 实际输出数量
    private transient long skippedCount;     // 🆕 跳过数量


    // 异常检测阈值
    private final double temporalAnomalyThreshold = 0.5;  // 时间维度偏差阈值 50%
    private final double spatialAnomalyThreshold = 0.5;   // 空间维度偏差阈值 50%

    public CacheEnhancedProcessFunction(CacheConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化缓存控制器
        this.cacheController = new CacheController(config);
        this.processedCount = 0;
        this.lastTuningCount = 0;
        this.lastLogCount = 0;
        this.outputCount = 0;
        this.skippedCount = 0;

        // 初始化 L2 State (按传感器ID存储历史窗口列表)
        MapStateDescriptor<String, LinkedList<HistoryWindow>> l2Desc =
                new MapStateDescriptor<>(
                        "l2-window-cache",
                        TypeInformation.of(String.class),
                        TypeInformation.of(new TypeHint<LinkedList<HistoryWindow>>() {})
                );
        l2WindowState = getRuntimeContext().getMapState(l2Desc);

        // 初始化 L3 State (按网格Key存储空间聚合信息)
        MapStateDescriptor<String, SpatialGrid> l3Desc =
                new MapStateDescriptor<>(
                        "l3-spatial-cache",
                        TypeInformation.of(String.class),
                        TypeInformation.of(SpatialGrid.class)
                );
        l3GridState = getRuntimeContext().getMapState(l3Desc);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        System.out.printf("✅ CacheEnhancedProcessFunction 初始化完成 (SubTask: %d)%n", subtaskIndex);
        System.out.printf("   📋 去重配置: enabled=%s, amplitudeThreshold=%.2f, timeThreshold=%dms%n",
                config.isDeduplicationEnabled(),
                config.getAmplitudeChangeThreshold(),
                config.getTimeIntervalThreshold());
    }

    @Override
    public void processElement(SeismicRecord record, Context ctx,
                               Collector<SeismicRecord> out) throws Exception {

        processedCount++;
        String sensorId = record.getSensorId();

        // ===== 🆕 步骤1: 判断是否需要跳过输出 (数据去重) =====
        boolean shouldSkip = cacheController.shouldSkipOutput(record);

        if (shouldSkip) {
            skippedCount++;
            // 不输出，但仍然需要更新缓存（可选）
            // 如果不更新缓存，则用最早的数据作为基准
            // 如果更新缓存，则用最新的数据作为基准
            // 这里选择不更新，保持稳定的基准

            // 周期性日志
            if (processedCount - lastLogCount >= 10000) {
                printProcessingLog0();
                lastLogCount = processedCount;
            }
            return;  // ⭐ 跳过输出！
        }

        // ===== 步骤2: 使用缓存控制器处理记录 =====
        // 1. 使用缓存控制器处理记录 (L1缓存 + 异常检测)
        CacheProcessResult cacheResult = cacheController.processRecord(record);

        // 2. 更新Flink State中的L3空间缓存
        updateL3State(record);

        // 3. 记录指标，更新统计
        cacheController.getMetricsCollector().incrementTotal();
        cacheController.getMetricsCollector().recordProcessed();

        // 4. 输出记录
        out.collect(record);

        // 5. 周期性调优 (每处理5000条执行一次)
        if (processedCount - lastTuningCount >= 5000) {
            cacheController.periodicTuning();
            lastTuningCount = processedCount;
        }

        // 6. 周期性日志 (每处理10000条输出一次)
        if (processedCount - lastLogCount >= 10000) {
            printProcessingLog(cacheResult);
            lastLogCount = processedCount;
        }
    }

    /**
     * 更新L3空间缓存状态
     */
    private void updateL3State(SeismicRecord record) throws Exception {
        String gridKey = SpatialGrid.computeGridKey(
                record.getLongitude(),
                record.getLatitude(),
                config.getSpatialGridSize()
        );

        SpatialGrid grid = l3GridState.get(gridKey);
        if (grid == null) {
            grid = new SpatialGrid(
                    record.getLongitude(),
                    record.getLatitude(),
                    config.getSpatialGridSize()
            );
        }
        grid.addRecord(record.getSeismicAmplitude(), record.getPhaseType());
        l3GridState.put(gridKey, grid);
    }


    /**
     * 输出处理日志
     */
    private void printProcessingLog0() {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        double skipRate = processedCount > 0
                ? (double) skippedCount / processedCount * 100
                : 0;
        double hitRate = cacheController.getL1Stats().getHitRate() * 100;

        System.out.printf(
                "[SubTask-%d] 📊 处理: %d | 输出: %d | 跳过: %d (%.1f%%) | L1命中率: %.1f%%%n",
                subtaskIndex, processedCount, outputCount, skippedCount, skipRate, hitRate
        );
    }

    private void printProcessingLog(CacheProcessResult lastResult) {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        System.out.printf("[SubTask-%d] 已处理 %d 条 | L1命中率: %.2f%% | " +
                        "最近时间偏差: %.2f | 最近空间偏差: %.2f%n",
                subtaskIndex,
                processedCount,
                cacheController.getL1Stats().getHitRate() * 100,
                lastResult.getTemporalDeviation(),
                lastResult.getSpatialDeviation()
        );
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 输出最终统计
        if (cacheController != null) {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            double skipRate = processedCount > 0
                    ? (double) skippedCount / processedCount * 100
                    : 0;
            System.out.printf("%n===== [SubTask-%d] 缓存最终统计 =====%n", subtaskIndex);
            System.out.printf("📥 总输入: %d%n", processedCount);
            System.out.printf("📤 实际输出: %d%n", outputCount);
            System.out.printf("⏭️ 跳过数量: %d (%.1f%%)%n", skippedCount, skipRate);
            System.out.printf("📈 数据压缩率: %.1f%%%n", skipRate);
            System.out.println(cacheController.getL1Stats().report());
            System.out.println(cacheController.getL2Stats().report());
            System.out.println(cacheController.getL3Stats().report());
            System.out.println("总处理记录数: " + processedCount);
            System.out.println("========================================\n");
        }
    }

}