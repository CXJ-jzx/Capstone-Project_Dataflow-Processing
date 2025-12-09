#!/bin/bash
# /data/flink/elastic/elastic_decision.sh（仅node01）
source /data/flink/elastic/conf/elastic.conf

# 定义变量
LOG_FILE=/data/flink/elastic/log/elastic_schedule.log
ALERT_SCRIPT=/data/flink/elastic/alert.sh
HISTORY_METRICS_FILE=/data/flink/elastic/metrics/history_metrics.log
CURRENT_TIME=$(date +%s)
TRIGGER_TIME=$((CURRENT_TIME - TRIGGER_DURATION))

# ========== 工具函数 ==========
# 1. 获取当前并行度
get_current_parallelism() {
    CURRENT_PARALLELISM=$(${FLINK_BIN_PATH}/flink list -m ${JOB_MANAGER_IP}:8081 | grep ${FLINK_JOB_ID} | awk '{print $NF}')
    [ -z "${CURRENT_PARALLELISM}" ] && CURRENT_PARALLELISM=${PARALLELISM_MIN}
    echo ${CURRENT_PARALLELISM}
}

# 2. 获取集群总TM数量（node01+node02+node03）
get_total_tm_count() {
    # 分别查询各节点TM数量并求和
    TM01=$(ssh root@node01 "jps | grep TaskManagerRunner | wc -l")
    TM02=$(ssh root@node02 "jps | grep TaskManagerRunner | wc -l")
    TM03=$(ssh root@node03 "jps | grep TaskManagerRunner | wc -l")
    TOTAL_TM=$((TM01 + TM02 + TM03))
    echo ${TOTAL_TM}
}

# 3. 触发Checkpoint（数据一致性）
trigger_checkpoint() {
    curl -X POST "${FLINK_REST_URL}/jobs/${FLINK_JOB_ID}/checkpoints" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        ${ALERT_SCRIPT} "Checkpoint触发失败，JobID:${FLINK_JOB_ID}"
        exit 1
    fi
    echo "$(date +'%Y-%m-%d %H:%M:%S') - Checkpoint触发成功" >> ${LOG_FILE}
}

# 4. 调整并行度（仅node01执行）
adjust_parallelism() {
    local NEW_PARALLELISM=$1
    trigger_checkpoint
    # 执行并行度调整
    ${FLINK_BIN_PATH}/flink rescale -m ${JOB_MANAGER_IP}:8081 ${FLINK_JOB_ID} -p ${NEW_PARALLELISM}
    if [ $? -eq 0 ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - 并行度调整成功：$(get_current_parallelism)→${NEW_PARALLELISM}" >> ${LOG_FILE}
    else
        ${ALERT_SCRIPT} "并行度调整失败，回滚至$(get_current_parallelism)"
        ${FLINK_BIN_PATH}/flink rescale -m ${JOB_MANAGER_IP}:8081 ${FLINK_JOB_ID} -p $(get_current_parallelism)
    fi
}

# 5. 扩容TM（优先在node02/node03新增）
add_tm() {
    local TOTAL_TM=$(get_total_tm_count)
    if [ ${TOTAL_TM} -ge ${TASK_MANAGER_MAX_NUM} ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - TM数量已达上限（${TOTAL_TM}）" >> ${LOG_FILE}
        return
    fi
    # 检查node02的TM数量（最多2个）
    TM02=$(ssh root@node02 "jps | grep TaskManagerRunner | wc -l")
    if [ ${TM02} -lt 2 ]; then
        ssh root@node02 "${FLINK_BIN_PATH}/taskmanager.sh start"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - node02新增TM，总数量：$((TOTAL_TM + 1))" >> ${LOG_FILE}
        return
    fi
    # 检查node03的TM数量（最多2个）
    TM03=$(ssh root@node03 "jps | grep TaskManagerRunner | wc -l")
    if [ ${TM03} -lt 2 ]; then
        ssh root@node03 "${FLINK_BIN_PATH}/taskmanager.sh start"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - node03新增TM，总数量：$((TOTAL_TM + 1))" >> ${LOG_FILE}
        return
    fi
    # 最后扩容node01
    ssh root@node01 "${FLINK_BIN_PATH}/taskmanager.sh start"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - node01新增TM，总数量：$((TOTAL_TM + 1))" >> ${LOG_FILE}
}

# 6. 缩容TM（优先停止node02/node03的多余TM）
remove_tm() {
    local TOTAL_TM=$(get_total_tm_count)
    if [ ${TOTAL_TM} -le ${TASK_MANAGER_MIN_NUM} ]; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') - TM数量已达下限（${TOTAL_TM}）" >> ${LOG_FILE}
        return
    fi
    # 先停止node03的多余TM
    TM03=$(ssh root@node03 "jps | grep TaskManagerRunner | wc -l")
    if [ ${TM03} -gt 1 ]; then
        TM_PID=$(ssh root@node03 "jps | grep TaskManagerRunner | tail -n1 | awk '{print $1}'")
        ssh root@node03 "${FLINK_BIN_PATH}/taskmanager.sh stop ${TM_PID}"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - node03停止TM（PID:${TM_PID}），总数量：$((TOTAL_TM - 1))" >> ${LOG_FILE}
        return
    fi
    # 再停止node02的多余TM
    TM02=$(ssh root@node02 "jps | grep TaskManagerRunner | wc -l")
    if [ ${TM02} -gt 1 ]; then
        TM_PID=$(ssh root@node02 "jps | grep TaskManagerRunner | tail -n1 | awk '{print $1}'")
        ssh root@node02 "${FLINK_BIN_PATH}/taskmanager.sh stop ${TM_PID}"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - node02停止TM（PID:${TM_PID}），总数量：$((TOTAL_TM - 1))" >> ${LOG_FILE}
        return
    fi
    # 最后停止node01的多余TM
    TM01=$(ssh root@node01 "jps | grep TaskManagerRunner | wc -l")
    if [ ${TM01} -gt 1 ]; then
        TM_PID=$(ssh root@node01 "jps | grep TaskManagerRunner | tail -n1 | awk '{print $1}'")
        ssh root@node01 "${FLINK_BIN_PATH}/taskmanager.sh stop ${TM_PID}"
        echo "$(date +'%Y-%m-%d %H:%M:%S') - node01停止TM（PID:${TM_PID}），总数量：$((TOTAL_TM - 1))" >> ${LOG_FILE}
    fi
}

# 7. 防抖判断
check_threshold() {
    local METRIC_TYPE=$1
    local THRESHOLD=$2
    local COMPARE_TYPE=$3
    # 筛选指定时间范围内的指标
    grep -E "^(${TRIGGER_TIME}|[0-9]{10}>[${TRIGGER_TIME}])" ${HISTORY_METRICS_FILE} > /tmp/temp_metrics.log
    # 解析指标值
    if [ "${METRIC_TYPE}" == "throughput" ]; then
        METRIC_VALUES=$(cat /tmp/temp_metrics.log | awk -F',' '{print $2}' | awk -F':' '{print $2}')
    else
        METRIC_VALUES=$(cat /tmp/temp_metrics.log | awk -F',' '{print $3}' | awk -F':' '{print $2}')
    fi
    # 判断是否全部满足条件
    local ALL_MATCH=1
    for VALUE in ${METRIC_VALUES}; do
        if [ "${COMPARE_TYPE}" == "gt" ] && [ ${VALUE} -le ${THRESHOLD} ]; then
            ALL_MATCH=0
            break
        elif [ "${COMPARE_TYPE}" == "lt" ] && [ ${VALUE} -ge ${THRESHOLD} ]; then
            ALL_MATCH=0
            break
        fi
    done
    rm -f /tmp/temp_metrics.log
    echo ${ALL_MATCH}
}

# ========== 核心决策逻辑 ==========
CURRENT_PARALLELISM=$(get_current_parallelism)
TOTAL_TM=$(get_total_tm_count)

# 1. 吞吐量触发并行度调整
## 扩容
if [ $(check_threshold "throughput" ${THROUGHPUT_UPPER} "gt") -eq 1 ] && [ ${CURRENT_PARALLELISM} -lt ${PARALLELISM_MAX} ]; then
    NEW_PARALLELISM=$((CURRENT_PARALLELISM + 2))
    [ ${NEW_PARALLELISM} -gt ${PARALLELISM_MAX} ] && NEW_PARALLELISM=${PARALLELISM_MAX}
    adjust_parallelism ${NEW_PARALLELISM}
fi
## 缩容
if [ $(check_threshold "throughput" ${THROUGHPUT_LOWER} "lt") -eq 1 ] && [ ${CURRENT_PARALLELISM} -gt ${PARALLELISM_MIN} ]; then
    NEW_PARALLELISM=$((CURRENT_PARALLELISM - 1))
    adjust_parallelism ${NEW_PARALLELISM}
fi

# 2. CPU负载触发TM调整
## 扩容
if [ $(check_threshold "cpu_load" ${CPU_LOAD_UPPER} "gt") -eq 1 ]; then
    add_tm
fi
## 缩容
if [ $(check_threshold "cpu_load" ${CPU_LOAD_LOWER} "lt") -eq 1 ]; then
    remove_tm
fi