#!/bin/bash
set -euo pipefail

source /data/flink/elastic/conf/elastic.conf

mkdir -p "${METRICS_DIR}" "${LOG_DIR}"

HISTORY_METRICS_FILE="${METRICS_DIR}/history_metrics.log"
LAST_RECORDS_FILE="${METRICS_DIR}/last_records.txt"
CURRENT_TIME=$(date +%s)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "${LOG_DIR}/collect_metrics.log"
}

check_job_running() {
    [[ -z "${FLINK_JOB_ID}" ]] && { log "ERROR: FLINK_JOB_ID 未配置"; return 1; }
    local STATUS=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.state // "UNKNOWN"')
    [[ "${STATUS}" != "RUNNING" ]] && { log "ERROR: Job 状态为 ${STATUS}"; return 1; }
    return 0
}

get_throughput() {
    local LAST_VERTEX=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.vertices[-1].id' 2>/dev/null)
    
    local TOTAL_RECORDS=0
    if [[ -n "${LAST_VERTEX}" && "${LAST_VERTEX}" != "null" ]]; then
        local RAW=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/vertices/${LAST_VERTEX}/subtasks/metrics?get=numRecordsIn" | jq -r '.[0].sum // 0' 2>/dev/null)
        TOTAL_RECORDS=$(printf "%.0f" "${RAW}" 2>/dev/null || echo "0")
    fi
    
    local LAST_RECORDS=0
    local LAST_TIME=${CURRENT_TIME}
    if [[ -f "${LAST_RECORDS_FILE}" ]]; then
        LAST_RECORDS=$(awk -F',' 'NR==1 {print $1}' "${LAST_RECORDS_FILE}" 2>/dev/null || echo "0")
        LAST_TIME=$(awk -F',' 'NR==1 {print $2}' "${LAST_RECORDS_FILE}" 2>/dev/null || echo "${CURRENT_TIME}")
    fi
    
    echo "${TOTAL_RECORDS},${CURRENT_TIME}" > "${LAST_RECORDS_FILE}"
    
    local TIME_DIFF=$((CURRENT_TIME - LAST_TIME))
    local RECORDS_DIFF=$((TOTAL_RECORDS - LAST_RECORDS))
    
    if [[ ${TIME_DIFF} -gt 0 && ${RECORDS_DIFF} -gt 0 ]]; then
        echo $((RECORDS_DIFF / TIME_DIFF))
    elif [[ ${RECORDS_DIFF} -eq 0 && ${TIME_DIFF} -gt 0 ]]; then
        # ========== 新增：零值平滑处理 ==========
        # 如果记录数没变化，查看历史最后一个非零值
        local LAST_NONZERO=$(grep -v "throughput:0" "${HISTORY_METRICS_FILE}" 2>/dev/null | tail -1 | awk -F',' '{split($2,a,":"); print a[2]}')
        if [[ -n "${LAST_NONZERO}" && ${LAST_NONZERO} -gt 0 ]]; then
            # 返回上次非零值的一半（表示数据流在减少但未完全停止）
            echo $((LAST_NONZERO / 2))
        else
            echo "0"
        fi
    else
        echo "0"
    fi
}

# 获取 CPU 负载
get_cpu_load() {
    local TOTAL_CPU=0
    local TM_COUNT=0
    
    local TM_IDS=$(curl -s "${FLINK_REST_URL}/taskmanagers" | jq -r '.taskmanagers[].id' 2>/dev/null)
    
    for TM_ID in ${TM_IDS}; do
        [[ -z "${TM_ID}" ]] && continue
        local CPU=$(curl -s "${FLINK_REST_URL}/taskmanagers/${TM_ID}/metrics?get=Status.JVM.CPU.Load" | jq -r '.[0].value // 0' 2>/dev/null)
        if [[ -n "${CPU}" && "${CPU}" != "null" ]]; then
            TOTAL_CPU=$(echo "${TOTAL_CPU} + ${CPU}" | bc 2>/dev/null || echo "${TOTAL_CPU}")
            TM_COUNT=$((TM_COUNT + 1))
        fi
    done
    
    if [[ ${TM_COUNT} -gt 0 ]]; then
        local AVG=$(echo "scale=4; ${TOTAL_CPU} / ${TM_COUNT}" | bc 2>/dev/null || echo "0")
        [[ "${AVG}" == .* ]] && AVG="0${AVG}"
        echo "${AVG}"
    else
        echo "0.0"
    fi
}

main() {
    log "开始采集指标..."
    
    if ! check_job_running; then
        echo "Job 未运行"
        exit 1
    fi
    
    THROUGHPUT=$(get_throughput)
    CPU_LOAD=$(get_cpu_load)
    TM_COUNT=$(curl -s "${FLINK_REST_URL}/taskmanagers" | jq '.taskmanagers | length' 2>/dev/null || echo "0")
    
    # 获取当前累计记录数（用于调试）
    local LAST_VERTEX=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.vertices[-1].id' 2>/dev/null)
    local TOTAL_RECORDS=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/vertices/${LAST_VERTEX}/subtasks/metrics?get=numRecordsIn" | jq -r '.[0].sum // 0' 2>/dev/null)
    
    log "采集完成: throughput=${THROUGHPUT}/s, cpu=${CPU_LOAD}, tm=${TM_COUNT}, total_records=${TOTAL_RECORDS}"
    
    # 写入历史
    echo "${CURRENT_TIME},throughput:${THROUGHPUT},cpu:${CPU_LOAD}" >> "${HISTORY_METRICS_FILE}"
    
    # 清理旧数据（保留最近7天）
    CLEANUP_TIME=$((CURRENT_TIME - 7 * 24 * 3600))
    awk -F',' -v t="${CLEANUP_TIME}" '$1 >= t' "${HISTORY_METRICS_FILE}" > "${HISTORY_METRICS_FILE}.tmp" 2>/dev/null && \
        mv "${HISTORY_METRICS_FILE}.tmp" "${HISTORY_METRICS_FILE}"
    
    echo "throughput=${THROUGHPUT}/s | cpu=${CPU_LOAD} | tm=${TM_COUNT} | records=${TOTAL_RECORDS}"
}

main "$@"
