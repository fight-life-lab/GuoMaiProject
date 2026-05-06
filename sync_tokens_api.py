#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-API Token 数据同步脚本 - 从SQLite数据库获取数据
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
import gzip
import hashlib
import logging
import sqlite3
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
# import  datetime

# 配置日志
log_dir = Path('/root/shijingjing/upload_tokens')
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / 'sync_tokens.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(log_file), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 配置参数
CONFIG = {
    'db_path': '/root/shijingjing/upload_tokens/one-api.db',  # 数据库路径
    # 'output_dir': '/root/shijingjing/upload_tokens/data',
    'output_dir':'/usr/token/data',
    'retention_days': 30,
    # 集团稽核系统配置
    'num_code': '10800',  # 数据发展中心编码  ???
    'company_code':'GMGS',   # 公司代码
    'sub_system': 'YZPT',  # 系统名称
    'interface_code': 'AIP_MDL_TOKEN_INFO',
    'upload_batch': '01',  # 上传批次，首次为00  批次号
    'file_seq': '0001' # 文件序列号
}

# 字段分隔符 (0x05)
FIELD_SEPARATOR = '0x05'
FIELD_SEPARATOR = '\x05'
# 记录分隔符 (\r\n)
# RECORD_SEPARATOR = '0x0D0A'
RECORD_SEPARATOR = '\r\n'

@dataclass
class TokenLogRecord:
    """Token日志记录数据结构"""
    id: int
    user_id: int
    created_at: int  # Unix时间戳
    type: int
    content: str
    username: str
    token_name: str
    model_name: str
    quota: int  # 总token消耗
    prompt_tokens: int
    completion_tokens: int
    channel_id: int
    request_id: str
    elapsed_time: int
    is_stream: bool
    system_prompt_reset: bool


class DatabaseQuerier:
    """数据库查询器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """连接数据库"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"数据库连接成功: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def get_daily_logs(self, target_date: str) -> List[TokenLogRecord]:
        """
        获取指定日期的日志数据
        :param target_date: 日期字符串 YYYY-MM-DD
        :return: TokenLogRecord列表
        """
        if not self.conn:
            if not self.connect():
                return []
        
        # 转换日期为时间戳范围
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        start_timestamp = int(date_obj.timestamp())
        end_timestamp = int((date_obj + timedelta(days=1)).timestamp())
        
        logger.info(f"查询日期 {target_date} 的数据 (时间戳: {start_timestamp} - {end_timestamp})")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, user_id, created_at, type, content, username, 
                       token_name, model_name, quota, prompt_tokens, 
                       completion_tokens, channel_id, request_id, 
                       elapsed_time, is_stream, system_prompt_reset
                FROM logs
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at
            """, (start_timestamp, end_timestamp))
            
            rows = cursor.fetchall()
            records = []
            
            for row in rows:
                if '测试失败' in row['content']:
                    continue
                record = TokenLogRecord(
                    id=row['id'],
                    user_id=row['user_id'],
                    created_at=row['created_at'],
                    type=row['type'],
                    content=row['content'] or '',
                    username=row['username'] or '',
                    token_name=row['token_name'] or '',
                    model_name=row['model_name'] or '',
                    quota=row['quota'] or 0,
                    prompt_tokens=row['prompt_tokens'] or 0,
                    completion_tokens=row['completion_tokens'] or 0,
                    channel_id=row['channel_id'] or 0,
                    request_id=row['request_id'] or '',
                    elapsed_time=row['elapsed_time'] or 0,
                    is_stream=bool(row['is_stream']),
                    system_prompt_reset=bool(row['system_prompt_reset'])
                )
                records.append(record)
            
            logger.info(f"查询到 {len(records)} 条记录")
            return records
            
        except sqlite3.Error as e:
            logger.error(f"查询数据失败: {e}")
            return []
    
    def get_logs_by_date_range(self, start_date: str, end_date: str) -> List[TokenLogRecord]:
        """
        获取日期范围内的日志数据
        :param start_date: 开始日期 YYYY-MM-DD
        :param end_date: 结束日期 YYYY-MM-DD
        :return: TokenLogRecord列表
        """
        if not self.conn:
            if not self.connect():
                return []
        
        start_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_obj = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        
        start_timestamp = int(start_obj.timestamp())
        end_timestamp = int(end_obj.timestamp())
        
        logger.info(f"查询日期范围 {start_date} 至 {end_date} 的数据")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, user_id, created_at, type, content, username, 
                       token_name, model_name, quota, prompt_tokens, 
                       completion_tokens, channel_id, request_id, 
                       elapsed_time, is_stream, system_prompt_reset
                FROM logs
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at
            """, (start_timestamp, end_timestamp))
            
            rows = cursor.fetchall()
            records = []
            
            for row in rows:
                record = TokenLogRecord(
                    id=row['id'],
                    user_id=row['user_id'],
                    created_at=row['created_at'],
                    type=row['type'],
                    content=row['content'] or '',
                    username=row['username'] or '',
                    token_name=row['token_name'] or '',
                    model_name=row['model_name'] or '',
                    quota=row['quota'] or 0,
                    prompt_tokens=row['prompt_tokens'] or 0,
                    completion_tokens=row['completion_tokens'] or 0,
                    channel_id=row['channel_id'] or 0,
                    request_id=row['request_id'] or '',
                    elapsed_time=row['elapsed_time'] or 0,
                    is_stream=bool(row['is_stream']),
                    system_prompt_reset=bool(row['system_prompt_reset'])
                )
                records.append(record)
            
            logger.info(f"查询到 {len(records)} 条记录")
            return records
            
        except sqlite3.Error as e:
            logger.error(f"查询数据失败: {e}")
            return []


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


def extract_brand(model_name: str) -> str:
    """从模型名称提取品牌"""
    if not model_name:
        return 'UNKNOWN'
    model_upper = model_name.upper()
    if 'QWEN' in model_upper:
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


def extract_size(model_name: str) -> float:
    """从模型名称提取参数量"""
    if not model_name:
        return 0
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
    elif '671' in model_name:
        return 671
    return 0


def generate_model_code(model_name: str) -> str:
    """生成模型编码"""
    if not model_name:
        return 'unknown'
    # print('---',model_name)
    brand = extract_brand(model_name)
    # print(f"brand is {brand}")
    size = extract_size(model_name)
    # print(f"size is {size}")
    return f"GMGS_{brand}_{int(size)}_000001"

def generate_app_id(app_owner_dep_name):
    # knowledge_id=''
    if '人力部'== app_owner_dep_name:
        knowledge_id='BGWX0000'
    else :
        knowledge_id='ZNGL0018'


    return f"GMGS_{knowledge_id}_000001"

def generate_app_name(app_owner_dep_name):
    if '人力部' == app_owner_dep_name:
        knowledge_id = '人力数字员工'
    else:
        knowledge_id = '媒资库'
    return knowledge_id


def transform_record(log_record: TokenLogRecord) -> Dict:
    """
    将数据库记录转换为稽核系统格式
    :param log_record: 数据库原始记录
    :return: 稽核系统格式的记录字典
    """
    model_name = log_record.model_name
    prompt_tokens = log_record.prompt_tokens
    completion_tokens = log_record.completion_tokens
    total_tokens = log_record.quota
    created_at = log_record.created_at
    # print(type(created_at),created_at)
    date_str = datetime.fromtimestamp(created_at).strftime("%Y%m%d")


    # deploy_id = {}

    app_owner_dep_name = generate_model_app_owner_dep(model_name)
    thinking_tokens = int(0)

    record = {
        # 'DAY_ID':date_str, # 账期
        'APP_OWNER_ORG': 'GMGS',
        'APP_OWNER_DEPARTMENT': app_owner_dep_name,
        'MODEL_DEPLOY_ORG': 'GMGS',
        'MODEL_DEPLOY_DEPARTMENT': '数据分中心',
        'MODEL_CODE': generate_model_code(model_name),   #GMGS_QWEN_235_00001   模型部署单位标识_模型品牌_模型参数量_序号
        'MODEL_NAME': generate_model_name_to_id(model_name),  ## 枚举值   qwen-235B-instruct-A22
        'APP_ID': generate_app_id(app_owner_dep_name),   # GMGS_ZNYY0006_000001      AI应用业务归属单位标识_AI应用图谱编码_序号
        'APP_NAME': generate_app_name(app_owner_dep_name),   # 数字员工 ； 内容部-媒资库  ；
        'ITERNAL_APP': '1',
        'MODEL_BRAND': extract_brand(model_name),     # 全部大写：QWEN
        'MODEL_SIZE': int(extract_size(model_name)),  # 模型参数： DECIMAL(20,2)
        'MODEL_TYPE': '1',   # VL的版本的大模型   ---暂时不明确，直接是1  varchar
        'QUANT_LEVEL': '4',  # 是指推理的精度 ----闲杂有假设值FP8
        'HARDWARE_TYPE': 'H100', # H800 -
        'TOTAL_CALL_COUNT': 1,  # 调用次数  DECIMAL(30)
        'TOTAL_CALL_SUCCESS_COUNT': 1,   # 调用成功的次数  DECIMAL(30)
        'INPUT_TOKEN_COUNT': int(prompt_tokens),   # 输入tokens数，    DECIMAL(30)
        'OUTPUT_TOKEN_COUNT':int(completion_tokens),  # 输出tokens数  DECIMAL(30)
        'REASONING_TOKEN_COUNT': thinking_tokens,   ## thinking 的tokens数 DECIMAL(30)
        'TOTAL_TOKEN_COUNT': int(prompt_tokens) + int(completion_tokens) + thinking_tokens,   # 一起的tokens数据
        'REMARK': '',
    }

    return record

def generate_model_app_owner_dep(model_name):
    if 'VL' in model_name:
        dep_name  = '内容部'
    else:
        dep_name = '人力部'
    return dep_name


def generate_model_name_to_id(model_name):
    if '/' in model_name:
        model_name = model_name.split('/')[1]
    model_name_id_config = {'Qwen3-235B-A22B-Instruct-2507':'22'}
    model_id = model_name_id_config.get(model_name,'')
    if model_id:
        return model_name
    else:
        return 'Qwen3-235B-A22B-Instruct-2507'


def generate_dat_content(records: List[Dict]) -> str:
    """
    生成DAT文件内容
    :param records: 稽核系统格式的记录列表
    :return: 文件内容字符串，如果records为空则返回空字符串
    """
    # 如果记录为空，返回空内容（不生成任何数据）
    if not records:
        return ''
    
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
            'APP_NAME',                # 8. 应用名称
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


def generate_filename(data_date: str, process_time: str, seq: str = '0001') -> str:
    """
    生成符合规范的DAT文件名
    格式：10800_<数据来源>_<下属系统名称>_AIP_MDL_TOKEN_INFO_<文件处理时间>_<文件数据时间>_D_00_<序列号>.DAT
    """
    date_formats = get_date_formats(data_date)
    process_date_str = datetime.now().strftime('%Y%m%d')
    data_date_str = date_formats['YYYYMMDD']

    filename = (
        f"10800_{CONFIG['company_code']}_{CONFIG['sub_system']}_"
        f"{CONFIG['interface_code']}_{process_date_str}_{data_date_str}_"
        f"D_{CONFIG['upload_batch']}_{seq}.DAT"
    )
    return filename


def generate_val_content(filename: str, record_count: int, file_size: int, 
                         md5_hash: str, data_date: str) -> str:
    """
    生成VAL校验文件内容
    """
    date_formats = get_date_formats(data_date)
    fields = [
        filename,  # 数据文件名称
        str(record_count),  # 文件记录数
        str(file_size),  # 文件大小（字节数）
        md5_hash,  # MD5字符
        date_formats['YYYYMMDD'],  # 数据日期
        'D',  # 上传周期
        datetime.now().strftime('%Y%m%d%H%M%S'),  # 接口数据文件的生成时间
    ]
    return FIELD_SEPARATOR.join(fields) + RECORD_SEPARATOR


def generate_check_content(dat_filename: str) -> str:
    """
    生成CHECK文件内容
    """
    fields = [
        dat_filename,  # 待核查的接口数据文件名称
        'D',  # 上传周期
    ]
    return FIELD_SEPARATOR.join(fields) + RECORD_SEPARATOR


def save_audit_files(records: List[Dict], target_date: str) -> List[str]:
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

    # print('vl_content',dat_gz_filename)
    #
    # 生成并保存VAL文件
    val_content = generate_val_content(
        dat_gz_filename, len(records), file_size, md5_hash, target_date
    )
    # print(val_content)
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

    return [str(final_dat_gz_path),]


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


def sync_tokens(target_date: Optional[str] = None) -> bool:
    """
    执行Token数据同步的主函数
    :param target_date: 指定日期，默认为昨天
    :return: 是否成功
    """
    if target_date is None:
        target_date = get_yesterday_date()

    logger.info(f"=== 开始同步 {target_date} 的Token数据 ===")

    # 1. 连接数据库并拉取数据
    db = DatabaseQuerier(CONFIG['db_path'])
    logs = db.get_daily_logs(target_date)
    db.close()
    # print(len(logs))
    if not logs:
        logger.warning("未获取到任何日志数据，将生成空文件")
        # audit_records = [transform_record(log) for log in logs]
        # logger.info(f"转换 {len(audit_records)} 条记录为稽核系统格式")
        # generated_files = save_audit_files(audit_records, target_date)
        # logger.info(f"=== 同步完成，生成 {len(generated_files)} 个文件 ===")
    # # else:
    # # print(type(logs),logs)
    # 2. 转换数据格式
    audit_records = [transform_record(log) for log in logs]
    # print(type(audit_records),audit_records)
    # print(audit_records[0])
    logger.info(f"转换 {len(audit_records)} 条记录为稽核系统格式")
    #
    # 3. 生成稽核系统文件
    generated_files = save_audit_files(audit_records, target_date)
    # #
    # # # 4. 清理过期数据
    # # cleanup_old_files()
    # #
    logger.info(f"=== 同步完成，生成 {len(generated_files)} 个文件 ===")
    return True


def main():
    """主入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='One-API Token数据同步脚本（从数据库获取，集团稽核系统格式）')
    parser.add_argument(
        '--date',
        type=str,
        help='指定日期 (YYYY-MM-DD)，默认为昨天'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='开始日期 (YYYY-MM-DD)，用于批量同步'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='结束日期 (YYYY-MM-DD)，用于批量同步'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=CONFIG['db_path'],
        help=f'数据库文件路径 (默认: {CONFIG["db_path"]})'
    )

    args = parser.parse_args()
    
    # 更新数据库路径
    if args.db_path:
        CONFIG['db_path'] = args.db_path
    
    # 批量同步模式
    if args.start_date and args.end_date:
        logger.info(f"批量同步模式: {args.start_date} 至 {args.end_date}")
        current_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        while current_date <= end_date:
            # if date_str in ('2026-04-09','2026-04-10','2026-04-23'):
            #     continue
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str in ('2026-04-09','2026-04-10','2026-04-23'):
                current_date += timedelta(days=1)
                continue
            sync_tokens(date_str)
            current_date += timedelta(days=1)
    else:
        # 单日同步模式
        sync_tokens(args.date)


if __name__ == '__main__':
    main()
