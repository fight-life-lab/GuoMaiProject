#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Token数据同步任务脚本 - 带错误处理和邮件通知
每天9点执行前一天的数据同步
"""

import os
import sys
import subprocess
import smtplib
import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# 配置
CONFIG = {
    'script_dir': '/root/shijingjing/upload_tokens',
    'log_dir': '/root/shijingjing/upload_tokens/logs',
    'data_dir': '/root/shijingjing/upload_tokens/data',
    'email': 'shijj9@chinatelecom.cn',
    'python_env': '/root/miniconda3/envs/media_env/bin/python',
    'script_name': 'sync_tokens_api.py',
    'smtp_server': 'smtp.189.cn',  # 电信邮箱SMTP服务器
    'smtp_port': 25,
    'smtp_user': '',  # 发送邮箱账号
    'smtp_password': '',  # 发送邮箱密码
}


def setup_logging(log_dir: str, today: str) -> logging.Logger:
    """配置日志"""
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    log_file = log_path / f'sync_{today}.log'
    
    logger = logging.getLogger('sync_tokens')
    logger.setLevel(logging.INFO)
    
    # 文件处理器
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.INFO)
    
    # 控制台处理器
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    
    # 格式化
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger, log_file


def send_error_email(email: str, subject: str, body: str, log_content: str = ''):
    """发送错误通知邮件"""
    try:
        msg = MIMEMultipart()
        msg['From'] = CONFIG.get('smtp_user', 'noreply@example.com')
        msg['To'] = email
        msg['Subject'] = subject
        
        # 邮件正文
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 附加日志内容
        if log_content:
            msg.attach(MIMEText(f'\n\n--- 日志内容 ---\n{log_content}', 'plain', 'utf-8'))
        
        # 发送邮件
        server = smtplib.SMTP(CONFIG['smtp_server'], CONFIG['smtp_port'])
        server.starttls()
        if CONFIG.get('smtp_user') and CONFIG.get('smtp_password'):
            server.login(CONFIG['smtp_user'], CONFIG['smtp_password'])
        server.send_message(msg)
        server.quit()
        
        return True
    except Exception as e:
        print(f"发送邮件失败: {e}")
        return False


def run_sync():
    """执行同步任务"""
    # 获取日期
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 设置日志
    logger, log_file = setup_logging(CONFIG['log_dir'], today)
    
    logger.info("=" * 50)
    logger.info(f"同步任务开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"同步日期: {yesterday}")
    logger.info("=" * 50)
    
    # 构建命令
    script_path = Path(CONFIG['script_dir']) / CONFIG['script_name']
    cmd = [
        CONFIG['python_env'],
        str(script_path),
        '--date', yesterday
    ]
    
    logger.info(f"执行命令: {' '.join(cmd)}")
    
    try:
        # 执行脚本
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        # 记录输出
        if result.stdout:
            logger.info("脚本输出:\n" + result.stdout)
        if result.stderr:
            logger.error("脚本错误:\n" + result.stderr)
        
        # 检查执行结果
        if result.returncode != 0:
            logger.error(f"同步任务失败，退出码: {result.returncode}")
            
            # 读取日志内容
            log_content = ''
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_content = f.read()
            except:
                pass
            
            # 发送错误邮件
            subject = f"[Token同步失败] {today} 数据同步异常"
            body = f"""Token数据同步任务执行失败。

详细信息：
- 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 同步日期: {yesterday}
- 退出码: {result.returncode}
- 日志文件: {log_file}

请检查日志文件了解详细错误信息。

---
自动发送，请勿回复"""
            
            if send_error_email(CONFIG['email'], subject, body, log_content):
                logger.info(f"错误通知邮件已发送至 {CONFIG['email']}")
            else:
                logger.warning("发送错误通知邮件失败")
            
            sys.exit(1)
        else:
            logger.info("同步任务成功完成")
            
            # 检查生成的文件
            data_path = Path(CONFIG['data_dir'])
            if data_path.exists():
                dat_files = list(data_path.glob('*.DAT.gz'))
                logger.info(f"生成数据文件数量: {len(dat_files)}")
                for f in dat_files:
                    logger.info(f"  - {f.name}")
            
            sys.exit(0)
            
    except subprocess.TimeoutExpired:
        logger.error("同步任务执行超时（超过5分钟）")
        
        subject = f"[Token同步超时] {today} 数据同步异常"
        body = f"""Token数据同步任务执行超时。

详细信息：
- 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 同步日期: {yesterday}
- 超时: 5分钟

请检查脚本是否存在死循环或网络问题。

---
自动发送，请勿回复"""
        
        send_error_email(CONFIG['email'], subject, body)
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"执行同步任务时发生异常: {e}")
        
        subject = f"[Token同步异常] {today} 数据同步异常"
        body = f"""Token数据同步任务执行时发生异常。

详细信息：
- 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 同步日期: {yesterday}
- 异常信息: {str(e)}

请检查系统状态。

---
自动发送，请勿回复"""
        
        send_error_email(CONFIG['email'], subject, body)
        sys.exit(1)
    
    finally:
        logger.info(f"同步任务结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")


if __name__ == '__main__':
    run_sync()
