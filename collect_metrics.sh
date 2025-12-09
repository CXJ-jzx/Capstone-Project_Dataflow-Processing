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

# 获取吞吐量（基于 Source 的 numRecordsOut 累计值增量）
get_throughput() {
    # 1. 获取 Source 算子的 ID (通常是 vertices 数组的第 0 个)
    local SOURCE_VERTEX=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.vertices[0].id' 2>/dev/null)
    
    local TOTAL_RECORDS=0
    if [[ -n "${SOURCE_VERTEX}" && "${SOURCE_VERTEX}" != "null" ]]; then
        # 2. Source 算子使用 numRecordsOut (输出记录数)
        local RAW=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/vertices/${SOURCE_VERTEX}/subtasks/metrics?get=numRecordsOut" | jq -r '.[0].sum // 0' 2>/dev/null)
        TOTAL_RECORDS=$(printf "%.0f" "${RAW}" 2>/dev/null || echo "0")
    fi
    
    # 3. 读取上次记录
    local LAST_RECORDS=0
    local LAST_TIME=${CURRENT_TIME}
    if [[ -f "${LAST_RECORDS_FILE}" ]]; then
        LAST_RECORDS=$(awk -F',' 'NR==1 {print $1}' "${LAST_RECORDS_FILE}" 2>/dev/null || echo "0")
        LAST_TIME=$(awk -F',' 'NR==1 {print $2}' "${LAST_RECORDS_FILE}" 2>/dev/null || echo "${CURRENT_TIME}")
    fi
    
    # 4. 保存当前记录 (总数,时间戳)
    echo "${TOTAL_RECORDS},${CURRENT_TIME}" > "${LAST_RECORDS_FILE}"
    
    # 5. 计算吞吐量 ( (当前总数 - 上次总数) / 时间差 )
    local TIME_DIFF=$((CURRENT_TIME - LAST_TIME))
    local RECORDS_DIFF=$((TOTAL_RECORDS - LAST_RECORDS))
    
    # 输出吞吐量结果 (如果时间差为0，输出0，防止除以0错误)
    if [[ ${TIME_DIFF} -gt 0 && ${RECORDS_DIFF} -ge 0 ]]; then
        echo $((RECORDS_DIFF / TIME_DIFF))
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
    
    # 1. 计算 Source 吞吐量
    THROUGHPUT=$(get_throughput)
    CPU_LOAD=$(get_cpu_load)
    TM_COUNT=$(curl -s "${FLINK_REST_URL}/taskmanagers" | jq '.taskmanagers | length' 2>/dev/null || echo "0")
    
    # 2. 获取当前 Source 累计记录数（用于日志展示，保持与吞吐量来源一致）
    # 使用 .vertices[0].id (Source) 和 numRecordsOut
    local SOURCE_VERTEX=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.vertices[0].id' 2>/dev/null)
    local TOTAL_RECORDS=0
    if [[ -n "${SOURCE_VERTEX}" && "${SOURCE_VERTEX}" != "null" ]]; then
         local RAW=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/vertices/${SOURCE_VERTEX}/subtasks/metrics?get=numRecordsOut" | jq -r '.[0].sum // 0' 2>/dev/null)
         TOTAL_RECORDS=$(printf "%.0f" "${RAW}" 2>/dev/null || echo "0")
    fi
    
    log "采集完成: source_throughput=${THROUGHPUT}/s, cpu=${CPU_LOAD}, tm=${TM_COUNT}, source_total_records=${TOTAL_RECORDS}"
    
    # 写入历史
    echo "${CURRENT_TIME},throughput:${THROUGHPUT},cpu:${CPU_LOAD}" >> "${HISTORY_METRICS_FILE}"
    
    # 清理旧数据（保留最近7天）
    CLEANUP_TIME=$((CURRENT_TIME - 7 * 24 * 3600))
    awk -F',' -v t="${CLEANUP_TIME}" '$1 >= t' "${HISTORY_METRICS_FILE}" > "${HISTORY_METRICS_FILE}.tmp" 2>/dev/null && \
        mv "${HISTORY_METRICS_FILE}.tmp" "${HISTORY_METRICS_FILE}"
    
    echo "throughput=${THROUGHPUT}/s | cpu=${CPU_LOAD} | tm=${TM_COUNT} | records=${TOTAL_RECORDS}"
}

main "$@"