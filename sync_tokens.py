#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-API Token 数据同步脚本 - 集团稽核系统格式
用于每日定时拉取推理服务Tokens信息并生成符合集团稽核系统要求的文件

文件命名规范：
10800_<数据来源>_<下属系统名称>_AIP_MDL_TOKEN_INFO_<文件处理时间>_<文件数据时间>_D_00_0001.DAT.gz

文件格式规范：
- 编码：UTF-8
- 字段分隔符：0x05 (ENQ控制字符)
- 记录分隔符：0x0D0A (\r\n)
- 压缩格式：gzip
"""

import os
import sys
import json
import gzip
import hashlib
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/var/log/sync_tokens.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 配置参数
CONFIG = {
    'api_base_url': 'http://60.217.65.245:30076',
    'api_token': 'one_api_600640',
    'output_dir': '/data/tokens',
    'retention_days': 30,
    # 集团稽核系统配置
    'org_code': 'SFZXB',  # 数据发展中心编码
    'sub_system': 'ONEAPI',  # 下属系统名称
    'interface_code': 'AIP_MDL_TOKEN_INFO',
    'upload_batch': '00',  # 上传批次，首次为00
    'file_seq': '0001',  # 文件序列号
}

# 字段分隔符 (0x05)
FIELD_SEPARATOR = '\x05'
# 记录分隔符 (\r\n)
RECORD_SEPARATOR = '\r\n'

# 字段映射：One-API字段 -> 稽核系统字段
# 根据协议文档 AIP_MDL_TOKEN_INFO 表结构
FIELD_MAPPING = {
    # One-API字段: (稽核系统字段名, 默认值/处理函数)
    'token_name': ('APP_OWNER_ORG', 'SFZXB'),  # AI应用业务归属单位标识
    'department': ('APP_OWNER_DEPARTMENT', ''),  # 业务归属具体处室
    'model_deploy_org': ('MODEL_DEPLOY_ORG', 'SFZXB'),  # 模型部署单位标识
    'model_deploy_dept': ('MODEL_DEPLOY_DEPARTMENT', ''),  # 模型部署具体处室
    'model_code': ('MODEL_CODE', lambda x: generate_model_code(x)),  # 模型编码
    'model_name': ('MODEL_NAME', ''),  # 模型名称
    'app_id': ('APP_ID', 'SFZXB_ZNYY0001_000001'),  # 应用编码
    'app_name': ('USE_CASE', '通用对话'),  # 应用名称
    'is_internal': ('ITERNAL_APP', '1'),  # 是否内部应用
    'model_brand': ('MODEL_BRAND', lambda x: extract_brand(x)),  # 模型品牌
    'model_size': ('MODEL_SIZE', lambda x: extract_size(x)),  # 模型参数量
    'model_type': ('MODEL_TYPE', 'LLM'),  # 模型类型
    'quant_level': ('QUANT_LEVEL', 'FP16'),  # 模型部署精度
    'hardware_type': ('HARDWARE_TYPE', '910B'),  # 部署硬件类型
    'call_count': ('TOTAL_CALL_COUNT', '1'),  # 模型调用次数
    'call_success_count': ('TOTAL_CALL_SUCCESS_COUNT', '1'),  # 成功调用次数
    'prompt_tokens': ('INPUT_TOKEN_COUNT', '0'),  # 输入Tokens
    'completion_tokens': ('OUTPUT_TOKEN_COUNT', '0'),  # 输出Tokens
    'reasoning_tokens': ('REASONING_TOKEN_COUNT', '0'),  # Thinking Tokens
    'total_tokens': ('TOTAL_TOKEN_COUNT', '0'),  # 总Tokens
    'remark': ('REMARK', ''),  # 备注
}


def generate_model_code(model_name):
    """生成模型编码"""
    if not model_name:
        return 'SFZXB_UNKNOWN_000000'
    # 简化处理：使用模型名称的哈希
    brand = extract_brand(model_name)
    size = extract_size(model_name)
    return f"SFZXB_{brand}_{int(size)}_000001"


def extract_brand(model_name):
    """从模型名称提取品牌"""
    if not model_name:
        return 'UNKNOWN'
    model_upper = model_name.upper()
    if 'QWEN' in model_upper or 'QWEN' in model_name:
        return 'QWEN'
    elif 'DEEPSEEK' in model_upper:
        return 'DEEPSEEK'
    elif 'TELECHAT' in model_upper:
        return 'TELECHAT'
    elif 'GPT' in model_upper:
        return 'OPENAI'
    elif 'KIMI' in model_upper or 'MOONSHOT' in model_upper:
        return 'MOONSHOT'
    elif 'GLM' in model_upper:
        return 'CHATGLM'
    else:
        # 提取第一个单词作为品牌
        return model_name.split('/')[0].split('-')[0].upper()[:10]


def extract_size(model_name):
    """从模型名称提取参数量"""
    if not model_name:
        return 0
    import re
    # 匹配数字+B或数字+ Billion等模式
    patterns = [
        r'(\d+)[-\s]?B',
        r'(\d+)B',
        r'(\d+)\s* Billion',
    ]
    for pattern in patterns:
        match = re.search(pattern, model_name, re.IGNORECASE)
        if match:
            return float(match.group(1))
    # 根据模型名称推断
    if '72' in model_name:
        return 72
    elif '32' in model_name:
        return 32
    elif '14' in model_name:
        return 14
    elif '7' in model_name:
        return 7
    elif '235' in model_name:
        return 235
    return 0


def get_yesterday_date():
    """获取昨天的日期字符串 YYYY-MM-DD"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def get_date_formats(date_str):
    """获取各种日期格式"""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return {
        'YYYYMMDD': dt.strftime('%Y%m%d'),
        'YYYYMMDDHH24MMSS': dt.strftime('%Y%m%d') + '000000',
    }


def fetch_daily_logs(target_date=None):
    """
    拉取指定日期的日志数据
    :param target_date: 日期字符串 YYYY-MM-DD，默认为昨天
    :return: 日志数据列表
    """
    if target_date is None:
        target_date = get_yesterday_date()

    url = f"{CONFIG['api_base_url']}/api/daily_log"
    params = {
        'token': CONFIG['api_token'],
        'date': target_date
    }

    logger.info(f"开始拉取 {target_date} 的日志数据...")

    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if data.get('status') == 'success':
            logs = data.get('data', [])
            count = data.get('count', 0)
            logger.info(f"成功拉取 {count} 条日志记录")
            return logs
        else:
            logger.error(f"API返回错误: {data}")
            return []

    except requests.exceptions.RequestException as e:
        logger.error(f"请求失败: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        return []


def transform_record(log_record):
    """
    将One-API记录转换为稽核系统格式
    :param log_record: One-API原始记录
    :return: 稽核系统格式的记录字典
    """
    model_name = log_record.get('model_name', '')
    prompt_tokens = log_record.get('prompt_tokens', 0)
    completion_tokens = log_record.get('completion_tokens', 0)
    total_tokens = log_record.get('quota', 0)  # One-API使用quota字段存储总token

    record = {
        'APP_OWNER_ORG': 'SFZXB',
        'APP_OWNER_DEPARTMENT': '',
        'MODEL_DEPLOY_ORG': 'SFZXB',
        'MODEL_DEPLOY_DEPARTMENT': '',
        'MODEL_CODE': generate_model_code(model_name),
        'MODEL_NAME': model_name,
        'APP_ID': 'SFZXB_ZNYY0001_000001',
        'USE_CASE': '通用对话',
        'ITERNAL_APP': '1',
        'MODEL_BRAND': extract_brand(model_name),
        'MODEL_SIZE': str(extract_size(model_name)),
        'MODEL_TYPE': 'LLM',
        'QUANT_LEVEL': 'FP16',
        'HARDWARE_TYPE': '910B',
        'TOTAL_CALL_COUNT': '1',
        'TOTAL_CALL_SUCCESS_COUNT': '1',
        'INPUT_TOKEN_COUNT': str(prompt_tokens),
        'OUTPUT_TOKEN_COUNT': str(completion_tokens),
        'REASONING_TOKEN_COUNT': '0',
        'TOTAL_TOKEN_COUNT': str(total_tokens),
        'REMARK': '',
    }
    return record


def generate_dat_content(records):
    """
    生成DAT文件内容
    :param records: 稽核系统格式的记录列表
    :return: 文件内容字符串
    """
    lines = []
    for record in records:
        # 按协议字段顺序排列 (AIP_MDL_TOKEN_INFO 表结构)
        field_order = [
            'APP_OWNER_ORG',           # 1. AI应用业务归属单位标识
            'APP_OWNER_DEPARTMENT',    # 2. AI应用业务归属具体处室/部门
            'MODEL_DEPLOY_ORG',        # 3. 模型部署单位标识
            'MODEL_DEPLOY_DEPARTMENT', # 4. 模型部署具体处室/部门
            'MODEL_CODE',              # 5. 模型编码
            'MODEL_NAME',              # 6. 模型名称
            'APP_ID',                  # 7. 应用编码
            'USE_CASE',                # 8. 应用名称
            'ITERNAL_APP',             # 9. 是否内部应用
            'MODEL_BRAND',             # 10. 模型品牌
            'MODEL_SIZE',              # 11. 模型参数量
            'MODEL_TYPE',              # 12. 模型类型
            'QUANT_LEVEL',             # 13. 模型部署精度
            'HARDWARE_TYPE',           # 14. 部署硬件类型
            'TOTAL_CALL_COUNT',        # 15. 模型调用次数
            'TOTAL_CALL_SUCCESS_COUNT',# 16. 模型成功调用次数
            'INPUT_TOKEN_COUNT',       # 17. 模型输入Tokens消耗
            'OUTPUT_TOKEN_COUNT',      # 18. 模型输出Tokens消耗
            'REASONING_TOKEN_COUNT',   # 19. 模型Thinking Tokens消耗
            'TOTAL_TOKEN_COUNT',       # 20. 模型总Tokens消耗
            'REMARK',                  # 21. 备注
        ]

        # 构建字段值列表
        values = []
        for field in field_order:
            value = record.get(field, '')
            # 转义字段中的特殊字符
            value = str(value).replace(FIELD_SEPARATOR, '^^')
            value = value.replace('\r', '').replace('\n', '')
            values.append(value)

        line = FIELD_SEPARATOR.join(values)
        lines.append(line)

    return RECORD_SEPARATOR.join(lines) + RECORD_SEPARATOR


def generate_filename(data_date, process_time, seq='0001'):
    """
    生成符合规范的DAT文件名
    格式：10800_<数据来源>_<下属系统名称>_AIP_MDL_TOKEN_INFO_<文件处理时间>_<文件数据时间>_D_00_<序列号>.DAT
    """
    date_formats = get_date_formats(data_date)
    process_date_str = datetime.now().strftime('%Y%m%d')
    data_date_str = date_formats['YYYYMMDD']

    filename = (
        f"10800_{CONFIG['org_code']}_{CONFIG['sub_system']}_"
        f"{CONFIG['interface_code']}_{process_date_str}_{data_date_str}_"
        f"D_{CONFIG['upload_batch']}_{seq}.DAT"
    )
    return filename


def generate_val_content(filename, record_count, file_size, md5_hash, data_date):
    """
    生成VAL校验文件内容
    """
    date_formats = get_date_formats(data_date)
    fields = [
        filename.replace('.gz', ''),  # 数据文件名称
        str(record_count),  # 文件记录数
        str(file_size),  # 文件大小（字节数）
        md5_hash,  # MD5字符
        date_formats['YYYYMMDD'],  # 数据日期
        'D',  # 上传周期
        datetime.now().strftime('%Y%m%d%H%M%S'),  # 接口数据文件的生成时间
    ]
    return FIELD_SEPARATOR.join(fields) + RECORD_SEPARATOR


def generate_check_content(dat_filename):
    """
    生成CHECK文件内容
    """
    fields = [
        dat_filename.replace('.gz', ''),  # 待核查的接口数据文件名称
        'D',  # 上传周期
    ]
    return FIELD_SEPARATOR.join(fields) + RECORD_SEPARATOR


def save_audit_files(records, target_date):
    """
    保存稽核系统格式的文件（DAT、VAL、CHECK）
    :param records: 记录列表
    :param target_date: 目标日期
    :return: 生成的文件列表
    """
    output_dir = Path(CONFIG['output_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)

    date_formats = get_date_formats(target_date)
    process_time = datetime.now().strftime('%Y%m%d%H%M%S')

    # 生成DAT文件内容
    dat_content = generate_dat_content(records)
    dat_bytes = dat_content.encode('utf-8')

    # 生成文件名
    dat_filename = generate_filename(target_date, process_time)
    dat_gz_filename = dat_filename + '.gz'

    # 计算MD5（压缩前）
    md5_hash = hashlib.md5(dat_bytes).hexdigest()

    # 保存DAT.gz文件（先写.tmp，完成后重命名）
    dat_gz_path = output_dir / (dat_gz_filename + '.tmp')
    with gzip.open(dat_gz_path, 'wb') as f:
        f.write(dat_bytes)

    # 获取压缩后文件大小
    file_size = dat_gz_path.stat().st_size

    # 重命名去掉.tmp后缀
    final_dat_gz_path = output_dir / dat_gz_filename
    dat_gz_path.rename(final_dat_gz_path)

    # 生成并保存VAL文件
    val_content = generate_val_content(
        dat_gz_filename, len(records), file_size, md5_hash, target_date
    )
    val_filename = dat_filename.replace('.DAT', '.VAL')
    val_path = output_dir / val_filename
    with open(val_path, 'w', encoding='utf-8') as f:
        f.write(val_content)

    # 生成并保存CHECK文件
    check_content = generate_check_content(dat_gz_filename)
    check_filename = dat_filename.replace('.DAT', '.CHECK')
    check_path = output_dir / check_filename
    with open(check_path, 'w', encoding='utf-8') as f:
        f.write(check_content)

    logger.info(f"生成文件：")
    logger.info(f"  - DAT: {final_dat_gz_path}")
    logger.info(f"  - VAL: {val_path}")
    logger.info(f"  - CHECK: {check_path}")

    return [str(final_dat_gz_path), str(val_path), str(check_path)]


def cleanup_old_files():
    """清理过期的历史数据文件"""
    output_dir = Path(CONFIG['output_dir'])
    if not output_dir.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=CONFIG['retention_days'])
    deleted_count = 0

    for file_path in output_dir.glob('10800_*.DAT*'):
        try:
            # 从文件名提取日期
            # 文件名格式：10800_SFZXB_ONEAPI_AIP_MDL_TOKEN_INFO_20260117_20260116_D_00_0001.DAT.gz
            parts = file_path.stem.split('_')
            if len(parts) >= 6:
                data_date_str = parts[5]  # 文件数据时间
                file_date = datetime.strptime(data_date_str, '%Y%m%d')

                if file_date < cutoff_date:
                    file_path.unlink()
                    deleted_count += 1
                    logger.info(f"删除过期文件: {file_path}")
        except (ValueError, OSError) as e:
            logger.warning(f"处理文件 {file_path} 时出错: {e}")

    # 同时清理VAL和CHECK文件
    for pattern in ['*.VAL', '*.CHECK']:
        for file_path in output_dir.glob(pattern):
            try:
                parts = file_path.stem.split('_')
                if len(parts) >= 6:
                    data_date_str = parts[5]
                    file_date = datetime.strptime(data_date_str, '%Y%m%d')
                    if file_date < cutoff_date:
                        file_path.unlink()
                        deleted_count += 1
            except (ValueError, OSError):
                pass

    if deleted_count > 0:
        logger.info(f"共清理 {deleted_count} 个过期文件")


def sync_tokens(target_date=None):
    """
    执行Token数据同步的主函数
    :param target_date: 指定日期，默认为昨天
    :return: 是否成功
    """
    if target_date is None:
        target_date = get_yesterday_date()

    logger.info(f"=== 开始同步 {target_date} 的Token数据 ===")

    # 1. 拉取数据
    logs = fetch_daily_logs(target_date)
    print(type(logs),logs)
    # if not logs:
    #     logger.warning("未获取到任何日志数据，将生成空文件")
    #
    # # 2. 转换数据格式
    # audit_records = [transform_record(log) for log in logs]
    # logger.info(f"转换 {len(audit_records)} 条记录为稽核系统格式")
    #
    # # 3. 生成稽核系统文件
    # generated_files = save_audit_files(audit_records, target_date)
    #
    # # 4. 清理过期数据
    # cleanup_old_files()
    #
    # logger.info(f"=== 同步完成，生成 {len(generated_files)} 个文件 ===")
    # return True


def main():
    """主入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='One-API Token数据同步脚本（集团稽核系统格式）')
    parser.add_argument(
        '--date',
        type=str,
        help='指定日期 (YYYY-MM-DD)，默认为昨天'
    )

    args = parser.parse_args()
    sync_tokens(args.date)


if __name__ == '__main__':
    main()
