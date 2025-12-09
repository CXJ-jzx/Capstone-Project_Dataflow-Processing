#!/bin/bash
# /data/flink/elastic/elastic_loop.sh

source /data/flink/elastic/conf/elastic.conf

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/elastic_loop.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

log "=== Flink 弹性伸缩服务启动 ==="
log "配置: TM范围=${TASK_MANAGER_MIN_NUM}-${TASK_MANAGER_MAX_NUM}, 并行度范围=${PARALLELISM_MIN}-${PARALLELISM_MAX}"

# 优雅退出处理
cleanup() {
    log "=== 弹性伸缩服务停止 ==="
    exit 0
}
trap cleanup SIGTERM SIGINT

while true; do
    # 采集指标
    if /data/flink/elastic/collect_metrics.sh >> "${LOG_FILE}" 2>&1; then
        # 执行决策
        /data/flink/elastic/elastic_decision.sh >> "${LOG_FILE}" 2>&1 || {
            log "ERROR: 决策脚本执行失败"
        }
    else
        log "ERROR: 指标采集失败，跳过本轮决策"
    fi
    
    sleep 5
done