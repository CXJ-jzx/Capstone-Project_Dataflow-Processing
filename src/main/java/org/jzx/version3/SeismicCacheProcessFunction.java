package org.jzx.version3;

import com.example.seismic.SeismicDataProto.SeismicRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import java.io.IOException;
/**
 * 自适应缓存处理算子
 * 核心功能：
 * 1. 双层缓存（L1 JVM + L2 RocksDB）
 * 2. LRU 淘汰策略
 * 3. 动态容量调节（基于命中率和内存负载）
 */
public class SeismicCacheProcessFunction
        extends KeyedProcessFunction<String, SeismicRecord, SeismicRecord> {

    // ========== 可配置参数 ==========
    private final double T_HIGH = 0.7;       // 命中率上阈值
    private final double T_LOW = 0.3;        // 命中率下阈值
    private final double M_SAFE = 0.70;      // 安全内存占用
    private final double M_DANGER = 0.90;    // 危险内存占用
    private final long COOLDOWN_MS = 10000;  // 冷却时间
    private final double CRITICAL_AMP = 50.0; // 高振幅阈值（优先保留）
    private final int INIT_CAPACITY = 10000; // 初始容量

    // ========== 缓存与状态 ==========
    private transient LruMap<String, FeatureValue> l1;
    private transient MapState<String, FeatureValue> l2;
    private transient ValueState<Integer> capacityState;
    private transient ValueState<Long> lastAdjustTs;

    // timer registered flag (avoid registering many timers)
    private transient ValueState<Boolean> timerRegistered;

    // cooldown counter (number of periods to wait)
    private transient ValueState<Integer> cooldownCounterState;

    // sliding-window counters for smoothing: store recent hits and misses aggregated per period
    // simple implementation: keep last 3 period hits and totalAccess arrays
    private transient ValueState<long[]> recentHitsState;   // len = N*2 ? We'll use [hits, accesses] per slot flat
    private final int SMOOTH_PERIODS = 3; // sliding window size

    // ========== 指标 ==========
    private transient Counter hits;
    private transient Counter misses;
    private transient Counter duplicatesFiltered; // 新增：过滤的重复数据计数
    private transient org.apache.flink.metrics.Gauge<Integer> capacityGauge;

    @Override
    public void open(Configuration parameters) throws Exception {
        // L1 初始化
        this.l1 = new LruMap<>(INIT_CAPACITY);

        // L2 状态初始化 (明确类型)
        MapStateDescriptor<String, FeatureValue> mapDesc =
                new MapStateDescriptor<>("l2-cache", TypeInformation.of(String.class), TypeInformation.of(FeatureValue.class));
        this.l2 = getRuntimeContext().getMapState(mapDesc);

        ValueStateDescriptor<Integer> capDesc =
                new ValueStateDescriptor<>("capacity", Integer.class);
        capacityState = getRuntimeContext().getState(capDesc);

        ValueStateDescriptor<Long> lastTsDesc =
                new ValueStateDescriptor<>("lastAdjustTs", Long.class);
        lastAdjustTs = getRuntimeContext().getState(lastTsDesc);

        // timerRegistered
        ValueStateDescriptor<Boolean> timerRegDesc =
                new ValueStateDescriptor<>("timerRegistered", Boolean.class);
        timerRegistered = getRuntimeContext().getState(timerRegDesc);

        // cooldown counter
        ValueStateDescriptor<Integer> cooldownDesc =
                new ValueStateDescriptor<>("cooldownCounter", Integer.class);
        cooldownCounterState = getRuntimeContext().getState(cooldownDesc);

        // sliding window state (flat array: [hits0, access0, hits1, access1, ...])
        ValueStateDescriptor<long[]> recentDesc =
                new ValueStateDescriptor<>("recentHits", long[].class);
        recentHitsState = getRuntimeContext().getState(recentDesc);

        // 初始化默认值（只在首次 open 时）
        if (capacityState.value() == null) capacityState.update(INIT_CAPACITY);
        if (lastAdjustTs.value() == null) lastAdjustTs.update(0L);
        if (timerRegistered.value() == null) timerRegistered.update(false);
        if (cooldownCounterState.value() == null) cooldownCounterState.update(0);
        if (recentHitsState.value() == null) {
            long[] init = new long[SMOOTH_PERIODS * 2];
            for (int i = 0; i < init.length; i++) init[i] = 0L;
            recentHitsState.update(init);
        }

        // 指标注册（保留并扩充）
        this.hits = getRuntimeContext().getMetricGroup().counter("cache_hits");
        this.misses = getRuntimeContext().getMetricGroup().counter("cache_misses");
        this.duplicatesFiltered = getRuntimeContext().getMetricGroup().counter("duplicates_filtered");

        getRuntimeContext().getMetricGroup().gauge("hit_rate", (Gauge<Double>) () -> {
            long h = hits.getCount();
            long m = misses.getCount();
            long total = h + m;
            return total == 0 ? 0.0 : (double) h / total;
        });

        getRuntimeContext().getMetricGroup().gauge("heap_load", (Gauge<Double>) () -> {
            MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = mbean.getHeapMemoryUsage();
            return heap.getMax() > 0 ? heap.getUsed() * 1.0 / heap.getMax() : 0.0;
        });

        // capacity gauge
        getRuntimeContext().getMetricGroup().gauge("cache_capacity", (Gauge<Integer>) () -> {
            try {
                Integer c = capacityState.value();
                return c == null ? INIT_CAPACITY : c;
            } catch (IOException e) {
                // 处理异常：返回默认值或抛出 RuntimeException
                System.err.println("Failed to get capacity from state: " + e.getMessage());
                return INIT_CAPACITY; // 返回默认值，避免指标崩溃
            }
        });

// 注册周期定时器：只注册一次（移除无效的 getProcessingTimeService 调用）
        if (Boolean.FALSE.equals(timerRegistered.value())) {
            // 直接标记为未注册，定时器将在 processElement 中首次触发时注册
            timerRegistered.update(false);
            System.out.println("⏲️ cache operator opened, timerRegistered=false");
        }
        Integer capInit = capacityState.value() == null ? INIT_CAPACITY : capacityState.value();
        l1.setCapacity(capInit);
    }

    @Override
    public void processElement(SeismicRecord record, Context ctx, Collector<SeismicRecord> out) throws Exception {
        String key = buildCacheKey(record);

        // ========== L1 查询 ==========
        FeatureValue fv = l1.get(key);
        if (fv != null) {
            hits.inc();

            // update sliding window current slot (append to last slot)
            long[] recent = recentHitsState.value();
            if (recent == null) {
                recent = new long[SMOOTH_PERIODS * 2];
            }
            int lastIdx = recent.length - 2;
            recent[lastIdx] += 1;       // hits
            recent[lastIdx + 1] += 1;   // access also increments
            recentHitsState.update(recent);

            // 语义感知：高振幅数据刷新优先级
            if (record.getSeismicAmplitude() > CRITICAL_AMP) {
                l1.put(key, fv); // 刷新访问顺序
            }
            // 可选：如果数据完全一致，直接丢弃（去重）
            if (isDuplicate(record, fv)) {
                duplicatesFiltered.inc();
                return; // 不向下游发送
            }
        } else {

            // ========== L2 查询 ==========
            FeatureValue l2v = l2.get(key);
            if (l2v != null) {
                hits.inc();

                // update sliding window current slot (append to last slot)
                long[] recent = recentHitsState.value();
                if (recent == null) {
                    recent = new long[SMOOTH_PERIODS * 2];
                }
                int lastIdx = recent.length - 2;
                recent[lastIdx] += 1;       // hits
                recent[lastIdx + 1] += 1;   // access also increments
                recentHitsState.update(recent);

                l1.put(key, l2v); // 回填 L1
            } else {
                // ========== 未命中：计算并缓存 ==========
                misses.inc();

                // update sliding window current slot: access++ (hit not incremented)
                long[] recent = recentHitsState.value();
                if (recent == null) {
                    recent = new long[SMOOTH_PERIODS * 2];
                }
                int lastIdx = recent.length - 1 - 1; // same as above
                recent[lastIdx + 1] += 1; // access
                recentHitsState.update(recent);

                FeatureValue computed = computeFeature(record);
                l1.put(key, computed);
                l2.put(key, computed);
            }
        }

        // 向下游发送（保持原始数据流）
        out.collect(record);

        // —— 定时器注册：只在第一个元素到来时注册一次 —— //
        Boolean reg = timerRegistered.value();
        if (reg == null || !reg) {
            // register first timer at next boundary
            long nowProc = ctx.timerService().currentProcessingTime();
            long next = nowProc + 5000L;
            ctx.timerService().registerProcessingTimeTimer(next);
            timerRegistered.update(true);
            System.out.println("⏲️ 定时器首次注册 at " + next);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SeismicRecord> out) throws Exception {
        long now = timestamp;
        Long last = lastAdjustTs.value();
        // cooldown check: also consider cooldownCounterState
        Integer cooldown = cooldownCounterState.value() == null ? 0 : cooldownCounterState.value();
        if (last != null && last > 0 && cooldown > 0) {
            // decrease cooldown and re-register next timer
            cooldownCounterState.update(cooldown - 1);
            long next = ctx.timerService().currentProcessingTime() + 5000L;
            ctx.timerService().registerProcessingTimeTimer(next);
            return;
        }

        // compute smoothed hitRate from recentHitsState
        long[] recent = recentHitsState.value();
        long totalHits = 0, totalAccess = 0;
        if (recent != null) {
            for (int i = 0; i < recent.length; i += 2) {
                totalHits += recent[i];
                totalAccess += recent[i + 1];
            }
        }
        double smoothedHr = (totalAccess == 0) ? 0.0 : (double) totalHits / totalAccess;
        double heapLoad = getHeapLoad();

        int cap = capacityState.value() != null ? capacityState.value() : INIT_CAPACITY;
        int newCap = cap;

        // protective bounds
        final int MIN_CAP = 100;      // 下界，避免为0
        final int MAX_CAP = 100_000;  // 上界

        // dynamic adjustment decision
        if (heapLoad > M_DANGER) {
            newCap = Math.max(MIN_CAP, (int) (cap * 0.5));
        } else if (smoothedHr > T_HIGH && heapLoad < M_SAFE) {
            newCap = Math.min(MAX_CAP, (int) Math.ceil(cap * 1.2));
        } else if (smoothedHr < T_LOW && heapLoad < M_SAFE) {
            newCap = Math.max(MIN_CAP, (int) Math.floor(cap * 0.8));
        }

        if (newCap != cap) {
            capacityState.update(newCap);
            l1.setCapacity(newCap);
            lastAdjustTs.update(now);
            // set cooldown to avoid oscillation (e.g., wait 2 periods)
            cooldownCounterState.update(2);
            System.out.println(String.format("🔄 %s capacity adjust: %d -> %d (smoothedHR=%.3f, heap=%.3f)",
                    getRuntimeContext().getTaskNameWithSubtasks(), cap, newCap, smoothedHr, heapLoad));
        }

        // rotate sliding window: drop oldest slot and zero it for next period
        if (recent == null) {
            recent = new long[SMOOTH_PERIODS * 2];
        }
        // shift array left by 2 (oldest removed), append zeros at end for next slot
        int len = recent.length;
        System.arraycopy(recent, 2, recent, 0, len - 2);
        recent[len - 2] = 0;
        recent[len - 1] = 0;
        recentHitsState.update(recent);

        // re-register next timer
        long next = ctx.timerService().currentProcessingTime() + 5000L;
        ctx.timerService().registerProcessingTimeTimer(next);
    }


    private String buildCacheKey(SeismicRecord rec) {
        return rec.getSensorId(); // 可扩展为 sensorId + 时间分桶
    }

    private FeatureValue computeFeature(SeismicRecord rec) {
        return new FeatureValue(rec.getCollectTimestamp(), rec.getSeismicAmplitude());
    }

    private boolean isDuplicate(SeismicRecord rec, FeatureValue cached) {
        // 简单去重：振幅相同且时间接近（可自定义）
        return Math.abs(rec.getSeismicAmplitude() - cached.getLastAmplitude()) < 0.01
                && Math.abs(rec.getCollectTimestamp() - cached.getLastTs()) < 500;
    }

    private double getHitRate() {
        long h = hits.getCount();
        long m = misses.getCount();
        return (h + m) == 0 ? 0.0 : (double) h / (h + m);
    }

    private double getHeapLoad() {
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mbean.getHeapMemoryUsage();
        return heap.getMax() > 0 ? heap.getUsed() * 1.0 / heap.getMax() : 0.0;
    }
}
