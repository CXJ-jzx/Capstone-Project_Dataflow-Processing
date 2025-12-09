#!/bin/bash
# /data/flink/elastic/alert.sh（仅node01）
ALERT_LOG=/data/flink/elastic/log/alert.log
ALERT_CONTENT=$1
# 写入告警日志（可扩展邮件/短信）
echo "$(date +'%Y-%m-%d %H:%M:%S') - ALERT: ${ALERT_CONTENT}" >> ${ALERT_LOG}
# 示例：邮件告警（需配置sendmail）
# echo "${ALERT_CONTENT}" | mail -s "Flink弹性调度告警" your_email@xxx.com
echo "${ALERT_CONTENT}" | mail -s "Flink弹性调度告警" 1158285666@qq.com