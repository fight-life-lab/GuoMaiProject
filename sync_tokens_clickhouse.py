#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Jingjing Shi
# @Date: 2026/5/26 10:15
# @Filename: sync_tokens_clickhouse.py
# @Software: PyCharm
# @Description: 
#
# !/usr/bin/env python3
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
import requests
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
    'output_dir': '/root/shijingjing/upload_tokens/data',
    # 'output_dir': '/usr/token/data',
    'retention_days': 30,
    # API配置
    'api_base_url': 'http://119.96.25.23:5001',
    'api_token': 'one_api_600640',
    # 集团稽核系统配置
    'num_code': '10800',  # 数据发展中心编码  ???
    'company_code': 'GMGS',  # 公司代码
    'sub_system': 'YZPT',  # 系统名称
    'interface_code': 'AIP_MDL_TOKEN_INFO',
    'upload_batch': '00',  # 上传批次，首次为00  批次号
    'file_seq': '0001',  # 文件序列号
    'quant_level': {"FP32": '1',
                    "FP16": '2',
                    "BF16": '3',
                    "FP8": '4',
                    "INT8": '5',
                    "INT4": '6'},
    'app_knowledge': {
        "智能服务_面向客户_客服机器人": "ZNF0001",
        "智能服务_面向一线_客服助理": "ZNF0002",
        "智能服务_面向一线_客服管理": "ZNF0003",
        "智能服务_面向一线_政支服务": "ZNF0004",
        "智能服务_其他_其他": "ZNFW0000",

        "智能营销_个人及家庭业务_客户经营": "ZNYX0001",
        "智能营销_个人及家庭业务_客户助理": "ZNYX0002",
        "智能营销_个人及家庭业务_数字人直播": "ZNYX0003",
        "智能营销_个人及家庭业务_销售助手": "ZNYX0004",
        "智能营销_个人及家庭业务_门店运营": "ZNYX0008",
        "智能营销_个人及家庭业务_活动策划": "ZNYX0009",
        "智能营销_小微企业业务_销售助手": "ZNYX0005",
        "智能营销_小微企业业务_营销策划": "ZNYX0006",
        "智能营销_政企信息业务_产教解决方案": "ZNYX0007",
        "智能营销_其他_其他": "ZNYX0000",

        "智能运营_云网运营_网络优化": "ZNYY0001",
        "智能运营_云网运营_综合维护": "ZNYY0002",
        "智能运营_云网运营_应急服务": "ZNYY0003",
        "智能运营_云网运营_业务平台": "ZNYY0004",
        "智能运营_云网运营_装维": "ZNYY0005",
        "智能运营_云网运营_运维保障": "ZNYY0006",
        "智能运营_云网运营_信息化平台": "ZNYY0007",

        "智能运营_数据运营_穿透式监管": "ZNYY0008",
        "智能运营_数据运营_数据开发": "ZNYY0009",
        "智能运营_安全运营_云网安全": "ZNYY0010",
        "智能运营_安全运营_反诈": "ZNYY0011",
        "智能运营_安全运营_数据与信息安全管理": "ZNYY0012",  # 修正：原表中序号27为“数据与信息安全”，归入“数据运营”下
        "智能运营_其他_其他": "ZNYN0000",

        "智能研发_科技创新_研发辅助": "ZNYF0001",
        "智能研发_科技创新_专利辅助": "ZNYF0002",
        "智能研发_其他_其他": "ZNYF0000",

        "智能管理_审计_审计管理": "ZNGL0001",
        "智能管理_财务_业务价值管理": "ZNGL0002",
        "智能管理_财务_财务辅助": "ZNGL0003",
        "智能管理_财务_财务风险管理": "ZNGL0004",
        "智能管理_投资者关系_投关管理": "ZNGL0005",
        "智能管理_投资者关系_上市合规": "ZNGL0006",
        "智能管理_党群_党建工作": "ZNGL0007",
        "智能管理_党群_宣传教育": "ZNGL0008",
        "智能管理_云网发展_工程建设": "ZNGL0009",
        "智能管理_云网发展_投资管理": "ZNGL0010",
        "智能管理_共建共享_共享保障": "ZNGL0011",
        "智能管理_共建共享_联合规建": "ZNGL0012",
        "智能管理_法律_合同管理": "ZNGL0013",
        "智能管理_法律_法律辅助": "ZNGL0014",
        "智能管理_工会_员工关爱": "ZNGL0015",
        "智能管理_工会_岗位创新": "ZNGL0034",  # 注意：序号47编码为ZNGL0034
        "智能管理_巡视_巡视整改": "ZNGL0016",
        "智能管理_人力_干部管理": "ZNGL0017",
        "智能管理_人力_劳动管理": "ZNGL0018",
        "智能管理_人力_员工培训": "ZNGL0019",
        "智能管理_人力_薪酬福利": "ZNGL0020",
        "智能管理_企业战略_改革推进": "ZNGL0021",
        "智能管理_企业战略_公司治理": "ZNGL0022",
        "智能管理_企业战略_战略规划": "ZNGL0035",
        "智能管理_采购供应链_采购管理": "ZNGL0023",
        "智能管理_采购供应链_物流管理": "ZNGL0024",
        "智能管理_综合办公_办公助手": "ZNGL0025",
        "智能管理_综合办公_规章制度": "ZNGL0026",
        "智能管理_综合办公_新闻宣传": "ZNGL0027",
        "智能管理_市场经营_渠道管理": "ZNGL0028",
        "智能管理_市场经营_套餐价值管理": "ZNGL0029",
        "智能管理_市场经营_业务经营分析": "ZNGL0030",
        "智能管理_市场经营_业务合规管理": "ZNGL0031",
        "智能管理_纪检_案管与审理": "ZNGL0032",
        "智能管理_资本运营_资产管理": "ZNGL0033",
        "智能管理_其他_其他": "ZNGL0000",

        "农业农村行业_农业农村行业_农业农村行业": "BGNY0000",
        "住建行业_住建行业_住建行业": "BGZJ0000",
        "文宣行业_文宣行业_文宣行业": "BGWX0000",
        "工业制造行业_工业制造行业_工业制造行业": "BGGY0000",
        "交通行业_交通行业_交通行业": "BGJT0000",
        "教育行业_教育行业_教育行业": "BGJY0000",
        "要客行业_要客行业_要客行业": "BGYK0000",
        "金融行业_金融行业_金融行业": "BGJR0000",
        "政务行业_政务行业_政务行业": "BGZW0000",
        "能源化工行业_能源化工行业_能源化工行业": "BGHG0000",
        "车联网行业_车联网行业_车联网行业": "BGCL0000",
        "卫健行业_卫健行业_卫健行业": "BGWJ0000",
        "应急行业_应急行业_应急行业": "BGYJ0000",
        "政法公安行业_政法公安行业_政法公安行业": "BGZF0000"}
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
    app_department: str
    app_name: str
    brand_name: str
    para_type: str
    hardware: str
    app_knowledge: str


class APIQuerier:
    """API接口查询器"""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    def get_daily_logs(self, target_date: str) -> List[TokenLogRecord]:
        """
        通过API获取指定日期的日志数据
        :param target_date: 日期字符串 YYYY-MM-DD
        :return: TokenLogRecord列表
        """
        url = f"{self.base_url}/api/daily_log"
        params = {
            'token': self.token,
            'date': target_date
        }

        logger.info(f"通过API查询日期 {target_date} 的数据")

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if data.get('status') != 'success':
                logger.error(f"API返回错误: {data}")
                return []

            logs = data.get('data', [])
            records = []
            logger.info(f"通过API查询到 {len(logs)} 条记录")
            for log in logs:
                # 过滤测试失败的数据
                content = log.get('content', '')
                type = log.get('type', '')
                if '测试' in content:
                    continue
                if type == 5:
                    continue
                # print(log)
                record = TokenLogRecord(
                    id=log.get('id', 0),
                    user_id=log.get('user_id', 0),
                    created_at=log.get('created_at', 0),
                    type=log.get('type', 0),
                    content=content,
                    username=log.get('username', ''),
                    token_name=log.get('token_name', ''),
                    model_name=log.get('model_name', ''),
                    app_department=log.get('app_department', ''),
                    app_name=log.get('app_name', ''),
                    app_knowledge=log.get('lvl1_lvl2_lvl3', ''),
                    brand_name=log.get('brand_name', ''),
                    para_type=log.get('para_type', ''),
                    hardware=log.get('hardware', ''),
                    quota=log.get('quota', 0),
                    prompt_tokens=log.get('prompt_tokens', 0),
                    completion_tokens=log.get('completion_tokens', 0),
                    channel_id=log.get('channel_id', 0),
                    request_id=log.get('request_id', ''),
                    elapsed_time=log.get('elapsed_time', 0),
                    is_stream=bool(log.get('is_stream', False)),
                    system_prompt_reset=bool(log.get('system_prompt_reset', False))
                )
                # print(record)
                records.append(record)
                # break

            logger.info(f"过滤测试数据后剩余 {len(records)} 条记录")
            return records

        except requests.exceptions.RequestException as e:
            logger.error(f"API请求失败: {e}")
            return []
        except Exception as e:
            logger.error(f"处理API数据失败: {e}")
            return []


class DatabaseQuerier:
    """数据库查询器（保留作为备选）"""

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
        return 'QWEN'
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
        return 'QWEN'


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


# def generate_app_id(app_owner_dep_name):
#     # knowledge_id=''
#     if '人力部'== app_owner_dep_name:
#         knowledge_id='BGWX0000'
#     else :
#         knowledge_id='ZNGL0018'
#
#
#     return f"GMGS_{knowledge_id}_000001"
def generate_app_id(app_knowledge):
    # knowledge_id=''
    if app_knowledge in CONFIG.get('app_knowledge', {}).keys():
        knowledge_id = CONFIG['app_knowledge'][app_knowledge]
    else:
        knowledge_id = 'BGWX0000'
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
    app_department = log_record.app_department
    brand_name = log_record.brand_name
    hardware = log_record.hardware.upper()
    app_name = log_record.app_name
    para_type = log_record.para_type
    app_knowledge = log_record.app_knowledge
    # deploy_id = {}

    # app_owner_dep_name = generate_model_app_owner_dep(model_name)
    thinking_tokens = int(0)

    record = {
        # 'DAY_ID':date_str, # 账期
        'APP_OWNER_ORG': 'GMGS',
        'APP_OWNER_DEPARTMENT': app_department,  # 人力部
        'MODEL_DEPLOY_ORG': 'GMGS',
        'MODEL_DEPLOY_DEPARTMENT': '数据分中心',
        'MODEL_CODE': generate_model_code(model_name),  # GMGS_QWEN_235_00001   模型部署单位标识_模型品牌_模型参数量_序号
        'MODEL_NAME': generate_model_name_to_id(model_name),  ## 枚举值   qwen-235B-instruct-A22
        'APP_ID': generate_app_id(app_knowledge),  # GMGS_ZNYY0006_000001      AI应用业务归属单位标识_AI应用图谱编码_序号
        'APP_NAME': app_name,  # 数字员工 ； 内容部-媒资库  ；
        'ITERNAL_APP': '1',
        'MODEL_BRAND': extract_brand(brand_name),  # 全部大写：QWEN
        'MODEL_SIZE': int(extract_size(model_name)),  # 模型参数： 235
        'MODEL_TYPE': '1',  # VL的版本的大模型   ---暂时不明确，直接是1  varchar
        'QUANT_LEVEL': generate_quant_level(para_type),  # 是指推理的精度 ----闲杂有假设值FP8  int8
        'HARDWARE_TYPE': hardware,  # H800 -
        'TOTAL_CALL_COUNT': 1,  # 调用次数  DECIMAL(30)
        'TOTAL_CALL_SUCCESS_COUNT': 1,  # 调用成功的次数  DECIMAL(30)
        'INPUT_TOKEN_COUNT': int(prompt_tokens),  # 输入tokens数，    DECIMAL(30)
        'OUTPUT_TOKEN_COUNT': int(completion_tokens),  # 输出tokens数  DECIMAL(30)
        'REASONING_TOKEN_COUNT': thinking_tokens,  ## thinking 的tokens数 DECIMAL(30)
        'TOTAL_TOKEN_COUNT': int(prompt_tokens) + int(completion_tokens) + thinking_tokens,  # 一起的tokens数据
        'REMARK': '',
    }

    return record


def generate_model_app_owner_dep(model_name):
    # if 'VL' in model_name:
    #     dep_name  = '内容部'
    # else:
    dep_name = '人力部'
    return dep_name


def generate_quant_level(para_type):
    if para_type in CONFIG.get('quant_level', {}).keys():
        return CONFIG['quant_level'][para_type]
    else:
        return '4'


def generate_model_name_to_id(model_name):

    model_name = model_name.replace('-INT8', '')

    if '/' in model_name:
        model_name = model_name.split('/')[1]
    if model_name in ['Qwen3-VL-235B-A22B-Instruct','Qwen3.5-122B-A10B']:

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
            'APP_OWNER_ORG',  # 1. AI应用业务归属单位标识
            'APP_OWNER_DEPARTMENT',  # 2. AI应用业务归属具体处室/部门
            'MODEL_DEPLOY_ORG',  # 3. 模型部署单位标识
            'MODEL_DEPLOY_DEPARTMENT',  # 4. 模型部署具体处室/部门
            'MODEL_CODE',  # 5. 模型编码
            'MODEL_NAME',  # 6. 模型名称
            'APP_ID',  # 7. 应用编码
            'APP_NAME',  # 8. 应用名称
            'ITERNAL_APP',  # 9. 是否内部应用
            'MODEL_BRAND',  # 10. 模型品牌
            'MODEL_SIZE',  # 11. 模型参数量
            'MODEL_TYPE',  # 12. 模型类型
            'QUANT_LEVEL',  # 13. 模型部署精度
            'HARDWARE_TYPE',  # 14. 部署硬件类型
            'TOTAL_CALL_COUNT',  # 15. 模型调用次数
            'TOTAL_CALL_SUCCESS_COUNT',  # 16. 模型成功调用次数
            'INPUT_TOKEN_COUNT',  # 17. 模型输入Tokens消耗
            'OUTPUT_TOKEN_COUNT',  # 18. 模型输出Tokens消耗
            'REASONING_TOKEN_COUNT',  # 19. 模型Thinking Tokens消耗
            'TOTAL_TOKEN_COUNT',  # 20. 模型总Tokens消耗
            'REMARK',  # 21. 备注
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

    return [str(final_dat_gz_path), ]


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

    # 1. 通过API获取数据
    api = APIQuerier(CONFIG['api_base_url'], CONFIG['api_token'])
    logs = api.get_daily_logs(target_date)
    # print(type(logs),logs[0])

    if not logs:
        logger.warning("未获取到任何日志数据，将生成空文件")

    # 2. 转换数据格式
    audit_records = [transform_record(log) for log in logs]
    logger.info(f"转换 {len(audit_records)} 条记录为稽核系统格式")
    # print(audit_records)
    #
    # # 3. 生成稽核系统文件
    generated_files = save_audit_files(audit_records, target_date)
    #
    logger.info(f"=== 同步完成，生成 {len(generated_files)} 个文件 ===")
    # return True


def main():
    """主入口函数"""
    import argparse

    parser = argparse.ArgumentParser(description='One-API Token数据同步脚本（从API接口获取，集团稽核系统格式）')
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
        '--api-url',
        type=str,
        default=CONFIG['api_base_url'],
        help=f'API基础URL (默认: {CONFIG["api_base_url"]})'
    )
    parser.add_argument(
        '--api-token',
        type=str,
        default=CONFIG['api_token'],
        help='API认证Token'
    )

    args = parser.parse_args()

    # 更新API配置
    if args.api_url:
        CONFIG['api_base_url'] = args.api_url
    if args.api_token:
        CONFIG['api_token'] = args.api_token

    # 批量同步模式
    if args.start_date and args.end_date:
        logger.info(f"批量同步模式: {args.start_date} 至 {args.end_date}")
        current_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            sync_tokens(date_str)
            current_date += timedelta(days=1)
    else:
        # 单日同步模式
        sync_tokens(args.date)


if __name__ == '__main__':
    main()
