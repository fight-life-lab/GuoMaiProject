#!/bin/bash
# 安装Cronjob定时任务脚本
# 每天9点执行前一天的数据同步

echo "=== 设置Token数据同步定时任务 ==="

# 配置
CRON_TIME="0 9 * * *"  # 每天9点
SCRIPT_DIR="/root/shijingjing/upload_tokens"
PYTHON_SCRIPT="$SCRIPT_DIR/run_sync.py"
LOG_FILE="/root/shijingjing/upload_tokens/logs/cron.log"

# 创建日志目录
mkdir -p /root/shijingjing/upload_tokens/logs

# 构建cron任务（使用Python包装脚本）
# 注意：conda环境需要在cron中正确激活
CRON_JOB="$CRON_TIME cd $SCRIPT_DIR && /root/miniconda3/envs/media_env/bin/python $PYTHON_SCRIPT >> $LOG_FILE 2>&1"

# 检查是否已存在相同的定时任务
if crontab -l 2>/dev/null | grep -q "run_sync.py"; then
    echo "[WARN] 定时任务已存在，先删除旧任务..."
    crontab -l 2>/dev/null | grep -v "run_sync.py" | crontab -
fi

# 添加新的定时任务
echo "[INFO] 添加定时任务: 每天9点执行"
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

# 验证
echo "[INFO] 当前定时任务列表:"
crontab -l

echo ""
echo "=== 定时任务设置完成 ==="
echo "执行时间: 每天9:00"
echo "执行脚本: $PYTHON_SCRIPT"
echo "日志文件: $LOG_FILE"
echo ""
echo "如需修改执行时间，请编辑crontab: crontab -e"
echo "如需查看日志: tail -f $LOG_FILE"
