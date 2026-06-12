#!/bin/bash
# Token数据同步任务脚本 - 带错误处理和邮件通知
# 每天9点执行前一天的数据同步

# 配置
SCRIPT_DIR="/root/shijingjing/upload_tokens"
LOG_DIR="/root/shijingjing/upload_tokens/logs"
EMAIL="shijj9@chinatelecom.cn"
PYTHON_ENV="/root/miniconda3/envs/media_env/bin/python"
SCRIPT_NAME="sync_tokens_api.py"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 获取日期
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/sync_${TODAY}.log"

# 记录开始时间
echo "========================================" >> "$LOG_FILE"
echo "同步任务开始: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "同步日期: $YESTERDAY" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 激活conda环境并执行脚本
export PATH="/root/miniconda3/bin:$PATH"
source /root/miniconda3/etc/profile.d/conda.sh
conda activate media_env

# 执行同步脚本
$PYTHON_ENV "$SCRIPT_DIR/$SCRIPT_NAME" --date "$YESTERDAY" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# 检查执行结果
if [ $EXIT_CODE -ne 0 ]; then
    echo "[ERROR] 同步任务失败，退出码: $EXIT_CODE" >> "$LOG_FILE"
    
    # 发送错误邮件
    SUBJECT="[Token同步失败] ${TODAY} 数据同步异常"
    BODY="Token数据同步任务执行失败。

详细信息：
- 执行时间: $(date '+%Y-%m-%d %H:%M:%S')
- 同步日期: $YESTERDAY
- 退出码: $EXIT_CODE
- 日志文件: $LOG_FILE

请检查日志文件了解详细错误信息。

---
自动发送，请勿回复"

    # 使用mail命令发送邮件（如果可用）
    if command -v mail &> /dev/null; then
        echo "$BODY" | mail -s "$SUBJECT" "$EMAIL"
        echo "[INFO] 错误通知邮件已发送至 $EMAIL" >> "$LOG_FILE"
    else
        echo "[WARN] mail命令不可用，无法发送邮件通知" >> "$LOG_FILE"
        # 尝试使用sendmail
        if command -v sendmail &> /dev/null; then
            {
                echo "Subject: $SUBJECT"
                echo "To: $EMAIL"
                echo "Content-Type: text/plain; charset=UTF-8"
                echo ""
                echo "$BODY"
            } | sendmail "$EMAIL"
            echo "[INFO] 使用sendmail发送错误通知邮件至 $EMAIL" >> "$LOG_FILE"
        fi
    fi
    
    exit 1
else
    echo "[SUCCESS] 同步任务成功完成" >> "$LOG_FILE"
    
    # 检查生成的文件
    DATA_DIR="/root/shijingjing/upload_tokens/data"
    FILE_COUNT=$(find "$DATA_DIR" -name "*.DAT.gz" -newer "$LOG_FILE" 2>/dev/null | wc -l)
    echo "[INFO] 生成数据文件数量: $FILE_COUNT" >> "$LOG_FILE"
fi

echo "同步任务结束: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

exit 0
