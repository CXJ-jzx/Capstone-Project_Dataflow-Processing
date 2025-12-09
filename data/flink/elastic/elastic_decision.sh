#!/bin/bash
# /data/flink/elastic/elastic_decision.sh
set -euo pipefail

source /data/flink/elastic/conf/elastic.conf

LOG_FILE="${LOG_DIR}/elastic_schedule.log"
ALERT_SCRIPT=/data/flink/elastic/alert.sh
HISTORY_METRICS_FILE="${METRICS_DIR}/history_metrics.log"
CURRENT_TIME=$(date +%s)
TRIGGER_TIME=$((CURRENT_TIME - TRIGGER_DURATION))

# 确保目录存在
mkdir -p "${LOG_DIR}"
mkdir -p "${SAVEPOINT_DIR}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "${LOG_FILE}"
}

# ========== 获取当前状态 ==========

# 获取当前并行度（通过 REST API）
get_current_parallelism() {
    local JOB_DETAILS=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}")
    
    # local PARALLELISM=$(echo "${JOB_DETAILS}" | jq -r '.vertices[0].parallelism // 0')
    # 获取 vertex 最大的并行度作为参考
    local PARALLELISM=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq '[.vertices[].parallelism] | max' 2>/dev/null)
    
    if [[ "${PARALLELISM}" == "0" || "${PARALLELISM}" == "null" ]]; then
        echo "${PARALLELISM_MIN}"
    else
        echo "${PARALLELISM}"
    fi
}

# 获取集群总 TM 数量
get_total_tm_count() {
    local TM_COUNT=$(curl -s "${FLINK_REST_URL}/taskmanagers" | jq -r '.taskmanagers | length')
    echo "${TM_COUNT:-0}"
}

# 获取可用 Slot 数量
get_available_slots() {
    local OVERVIEW=$(curl -s "${FLINK_REST_URL}/overview")
    local AVAILABLE=$(echo "${OVERVIEW}" | jq -r '.["slots-available"] // 0')
    echo "${AVAILABLE}"
}

# ========== 防抖检查 ==========

# 检查阈值是否在防抖期间内持续满足
check_threshold() {
    local METRIC_TYPE=$1    # throughput 或 cpu
    local THRESHOLD=$2
    local COMPARE_TYPE=$3   # gt 或 lt

    if [[ ! -f "${HISTORY_METRICS_FILE}" ]] || [[ ! -s "${HISTORY_METRICS_FILE}" ]]; then
        echo 0
        return
    fi

    # 获取防抖期间内的所有记录
    local METRIC_VALUES
    if [[ "${METRIC_TYPE}" == "throughput" ]]; then
        METRIC_VALUES=$(awk -F',' -v t="${TRIGGER_TIME}" '$1 >= t {split($2,a,":"); print a[2]}' "${HISTORY_METRICS_FILE}")
    else
        METRIC_VALUES=$(awk -F',' -v t="${TRIGGER_TIME}" '$1 >= t {split($3,a,":"); print a[2]}' "${HISTORY_METRICS_FILE}")
    fi

    # 无样本则不满足
    if [[ -z "${METRIC_VALUES}" ]]; then
        echo 0
        return
    fi

    # 计算样本数量（至少需要 3 个样本才做决策）
    local SAMPLE_COUNT=$(echo "${METRIC_VALUES}" | wc -l)
    if [[ ${SAMPLE_COUNT} -lt 3 ]]; then
        echo 0
        return
    fi

    # 判断所有样本是否都满足条件
    for v in ${METRIC_VALUES}; do
        local RESULT
        RESULT=$(awk -v val="${v}" -v th="${THRESHOLD}" -v cmp="${COMPARE_TYPE}" 'BEGIN {
            valnum = val + 0
            thnum = th + 0
            if (cmp == "gt") {
                if (valnum > thnum) print 1; else print 0
            } else if (cmp == "lt") {
                if (valnum < thnum) print 1; else print 0
            } else {
                print 0
            }
        }')
        
        if [[ "${RESULT}" != "1" ]]; then
            echo 0
            return
        fi
    done

    echo 1
}

# ========== 核心修改：并行度调整（含汇聚-分发逻辑） ==========

adjust_parallelism() {
    local NEW_PARALLELISM=$1
    local CURRENT_P=$(get_current_parallelism)
    
    log "========== 开始并行度调整 =========="
    log "当前并行度: ${CURRENT_P}, 目标并行度: ${NEW_PARALLELISM}"
    
    local TOTAL_TM=$(get_total_tm_count)
    local TOTAL_SLOTS=$((TOTAL_TM * SLOTS_PER_TM))
    if [[ ${NEW_PARALLELISM} -gt ${TOTAL_SLOTS} ]]; then
        log "Slot 不足，先扩容 TM"
        add_tm
        sleep 10
    fi
    
    log "[步骤1] 创建 Savepoint..."
    local SP_RESPONSE=$(curl -s -X POST "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/savepoints" \
        -H "Content-Type: application/json" \
        -d "{\"cancel-job\": false, \"target-directory\": \"file://${SAVEPOINT_DIR}\"}")
    
    local TRIGGER_ID=$(echo "${SP_RESPONSE}" | jq -r '.["request-id"] // empty')
    
    if [[ -z "${TRIGGER_ID}" ]]; then
        log "ERROR: Savepoint 触发失败"
        return 1
    fi
    
    log "[步骤2] 等待 Savepoint 完成..."
    local SAVEPOINT_PATH=""
    for i in {1..60}; do
        sleep 2
        local STATUS_RESP=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/savepoints/${TRIGGER_ID}")
        local STATUS=$(echo "${STATUS_RESP}" | jq -r '.status.id // "UNKNOWN"')
        
        if [[ "${STATUS}" == "COMPLETED" ]]; then
            SAVEPOINT_PATH=$(echo "${STATUS_RESP}" | jq -r '.operation.location // empty')
            log "Savepoint 完成: ${SAVEPOINT_PATH}"
            break
        elif [[ "${STATUS}" == "FAILED" ]]; then
            log "ERROR: Savepoint 失败"
            return 1
        fi
    done
    
    if [[ -z "${SAVEPOINT_PATH}" ]]; then
        log "ERROR: Savepoint 超时"
        return 1
    fi
    
    # ========== 关键修改：汇聚 & 分发 ==========
    log "[步骤2.5] 汇聚并分发 Savepoint 文件..."
    local LOCAL_PATH=${SAVEPOINT_PATH#file:}
    local DIR_NAME=$(basename "${LOCAL_PATH}")
    
    # 1. 确保本地目录存在
    mkdir -p "${LOCAL_PATH}"
    
    # 2. 汇聚：从其他节点拉取文件到 node01 (忽略重复)
    for NODE in node02 node03; do
        log "正在从 ${NODE} 拉取文件..."
        scp -r root@${NODE}:"${LOCAL_PATH}/*" "${LOCAL_PATH}/" >/dev/null 2>&1 || true
    done
    
    # 3. 分发：将完整的目录推送给所有节点
    for NODE in node02 node03; do
        log "正在分发完整文件到 ${NODE}..."
        ssh root@${NODE} "mkdir -p ${LOCAL_PATH}"
        scp -r "${LOCAL_PATH}/"* root@${NODE}:"${LOCAL_PATH}/"
        
        # 校验
        local COUNT=$(ssh root@${NODE} "ls -1 ${LOCAL_PATH} | wc -l")
        log "  - ${NODE} 文件数: ${COUNT}"
    done
    log "文件同步完成"
    # ============================================
    
    log "[步骤3] 停止原作业..."
    curl -s -X PATCH "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}?mode=cancel" > /dev/null
    
    for i in {1..30}; do
        sleep 2
        local JOB_STATE=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.state // "UNKNOWN"')
        if [[ "${JOB_STATE}" == "CANCELED" || "${JOB_STATE}" == "FINISHED" || "${JOB_STATE}" == "FAILED" ]]; then
            break
        fi
    done
    
    log "[步骤4] 从 Savepoint 重启..."
    local RUN_OUTPUT
    RUN_OUTPUT=$(${FLINK_BIN_PATH}/flink run -d \
        -m ${JOB_MANAGER_IP}:8081 \
        -p ${NEW_PARALLELISM} \
        -s "${SAVEPOINT_PATH}" \
        -c ${JOB_MAIN_CLASS} \
        ${JOB_JAR} ${JOB_ARGS} 2>&1)
    
    if [[ $? -eq 0 ]]; then
        local NEW_JOB_ID=$(echo "${RUN_OUTPUT}" | grep -oE '[a-f0-9]{32}' | tail -1)
        if [[ -n "${NEW_JOB_ID}" ]]; then
            log "重启成功！新 Job ID: ${NEW_JOB_ID}"
            sed -i "s/^FLINK_JOB_ID=.*/FLINK_JOB_ID=${NEW_JOB_ID}/" /data/flink/elastic/conf/elastic.conf
            FLINK_JOB_ID="${NEW_JOB_ID}"
            return 0
        fi
    fi
    
    log "ERROR: 重启失败"
    log "输出: ${RUN_OUTPUT}"
    return 1
}


# ========== TM 扩缩容 ==========

# 扩容 TM
add_tm() {
    local TOTAL_TM=$(get_total_tm_count)
    
    if [[ ${TOTAL_TM} -ge ${TASK_MANAGER_MAX_NUM} ]]; then
        log "TM 数量已达上限 (${TOTAL_TM}/${TASK_MANAGER_MAX_NUM})"
        return
    fi
    
    # 获取各节点 TM 数量
    local TM01=$(ssh -o ConnectTimeout=5 root@node01 "jps 2>/dev/null | grep -c TaskManagerRunner || echo 0")
    local TM02=$(ssh -o ConnectTimeout=5 root@node02 "jps 2>/dev/null | grep -c TaskManagerRunner || echo 0")
    local TM03=$(ssh -o ConnectTimeout=5 root@node03 "jps 2>/dev/null | grep -c TaskManagerRunner || echo 0")
    
    # 优先在 TM 数量少的节点扩容
    if [[ ${TM02} -lt 2 ]]; then
        log "在 node02 启动新 TM..."
        ssh root@node02 "${FLINK_BIN_PATH}/taskmanager.sh start" &
        log "node02 新增 TM，当前集群总数: $((TOTAL_TM + 1))"
    elif [[ ${TM03} -lt 2 ]]; then
        log "在 node03 启动新 TM..."
        ssh root@node03 "${FLINK_BIN_PATH}/taskmanager.sh start" &
        log "node03 新增 TM，当前集群总数: $((TOTAL_TM + 1))"
    elif [[ ${TM01} -lt 2 ]]; then
        log "在 node01 启动新 TM..."
        ssh root@node01 "${FLINK_BIN_PATH}/taskmanager.sh start" &
        log "node01 新增 TM，当前集群总数: $((TOTAL_TM + 1))"
    else
        log "所有节点 TM 数量均已达到 2，无法继续扩容"
    fi
    
    # 等待 TM 注册
    sleep 5
}

# 缩容 TM（优雅停止）
remove_tm() {
    local TOTAL_TM=$(get_total_tm_count)
    
    if [[ ${TOTAL_TM} -le ${TASK_MANAGER_MIN_NUM} ]]; then
        log "TM 数量已达下限 (${TOTAL_TM}/${TASK_MANAGER_MIN_NUM})"
        return
    fi
    
    # 获取一个空闲的 TM（没有运行 task 的）
    local IDLE_TM=$(curl -s "${FLINK_REST_URL}/taskmanagers" | jq -r '
        .taskmanagers[] | 
        select(.slotsNumber == .freeSlots) | 
        .id' | head -1)
    
    if [[ -z "${IDLE_TM}" ]]; then
        log "没有空闲的 TM 可以缩容"
        return
    fi
    
    # 获取该 TM 所在的节点
    local TM_HOST=$(curl -s "${FLINK_REST_URL}/taskmanagers/${IDLE_TM}" | jq -r '.hardware.managedMemory // empty' || echo "")
    
    # 由于无法直接获取 host，我们通过 SSH 查找并停止
    for NODE in node03 node02 node01; do
        local TM_COUNT=$(ssh -o ConnectTimeout=5 root@${NODE} "jps 2>/dev/null | grep -c TaskManagerRunner || echo 0")
        if [[ ${TM_COUNT} -gt 1 ]]; then
            log "在 ${NODE} 停止一个 TM..."
            # 使用 kill 而不是 taskmanager.sh stop（更精确控制）
            ssh root@${NODE} "jps | grep TaskManagerRunner | tail -1 | awk '{print \$1}' | xargs kill -SIGTERM" 2>/dev/null || true
            log "${NODE} 减少 TM，当前集群总数: $((TOTAL_TM - 1))"
            return
        fi
    done
    
    log "未找到可缩容的 TM"
}

# ========== 核心决策逻辑 ==========

main() {
    log "========== 开始决策 =========="
    
    # 检查 Job 是否在运行
    local JOB_STATUS=$(curl -s "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}" | jq -r '.state // "UNKNOWN"')
    if [[ "${JOB_STATUS}" != "RUNNING" ]]; then
        log "Job 状态为 ${JOB_STATUS}，跳过本轮决策"
        return
    fi
    
    local CURRENT_PARALLELISM=$(get_current_parallelism)
    local TOTAL_TM=$(get_total_tm_count)
    
    log "当前状态: 并行度=${CURRENT_PARALLELISM}, TM数量=${TOTAL_TM}"
    
    # 1. 吞吐量触发并行度调整
    ## 扩容检查
    if [[ $(check_threshold "throughput" "${THROUGHPUT_UPPER}" "gt") -eq 1 ]]; then
        if [[ ${CURRENT_PARALLELISM} -lt ${PARALLELISM_MAX} ]]; then
            local NEW_PARALLELISM=$((CURRENT_PARALLELISM + 2))
            [[ ${NEW_PARALLELISM} -gt ${PARALLELISM_MAX} ]] && NEW_PARALLELISM=${PARALLELISM_MAX}
            log "吞吐量超过上限，触发扩容: ${CURRENT_PARALLELISM} -> ${NEW_PARALLELISM}"
            adjust_parallelism ${NEW_PARALLELISM}
            return  # 一次只做一个调整
        fi
    fi
    
    ## 缩容检查
    if [[ $(check_threshold "throughput" "${THROUGHPUT_LOWER}" "lt") -eq 1 ]]; then
        if [[ ${CURRENT_PARALLELISM} -gt ${PARALLELISM_MIN} ]]; then
            local NEW_PARALLELISM=$((CURRENT_PARALLELISM - 1))
            [[ ${NEW_PARALLELISM} -lt ${PARALLELISM_MIN} ]] && NEW_PARALLELISM=${PARALLELISM_MIN}
            log "吞吐量低于下限，触发缩容: ${CURRENT_PARALLELISM} -> ${NEW_PARALLELISM}"
            adjust_parallelism ${NEW_PARALLELISM}
            return
        fi
    fi
    
    # 2. CPU 负载触发 TM 调整
    ## 扩容检查
    if [[ $(check_threshold "cpu" "${CPU_LOAD_UPPER}" "gt") -eq 1 ]]; then
        log "CPU 负载超过上限，触发 TM 扩容"
        add_tm
        return
    fi
    
    ## 缩容检查
    if [[ $(check_threshold "cpu" "${CPU_LOAD_LOWER}" "lt") -eq 1 ]]; then
        log "CPU 负载低于下限，触发 TM 缩容"
        remove_tm
        return
    fi
    
    log "各项指标正常，无需调整"
}

main "$@"
