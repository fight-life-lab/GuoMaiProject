#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据推送脚本 - 定时采集并通过SFTP推送数据

功能：
1. 每天凌晨0点自动从 Prometheus 采集前一天的数据
2. 生成数据文件（.txt.gz）和稽核文件（.chk）
3. 通过 SFTP 将文件传输到远程服务器

依赖安装：
pip install requests schedule paramiko

使用方式：
python data_push.py                     # 启动定时任务（每天0点执行）
python data_push.py --once              # 立即执行一次
python data_push.py --once --date 20250114   # 采集指定日期
python data_push.py --dry-run           # 模拟运行，不实际传输
python data_push.py --config /path/to/config.json  # 指定配置文件
"""

import os
import sys
import gzip
import json
import logging
import time
import argparse
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_push.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# 配置加载
# ============================================================
def load_config(config_path: str = None) -> Dict:
    """加载配置文件"""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    
    if not os.path.exists(config_path):
        logger.error(f"配置文件不存在: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"配置文件加载成功: {config_path}")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"配置文件格式错误: {e}")
        sys.exit(1)


# ============================================================
# 数据模型
# ============================================================
@dataclass
class TokenStatistics:
    """Token统计数据"""
    day_id: str
    prov_id: str
    model_type: int
    model_name: str
    total_call_count: int
    total_call_success_count: int
    total_token_count: int
    modle_size: Optional[float] = None
    quant_level: Optional[int] = None
    hardware_type: Optional[str] = None
    department: Optional[str] = None
    use_case: Optional[str] = None
    iternal_app: Optional[str] = None


# ============================================================
# 文件格式化
# ============================================================
class FileFormatter:
    """文件格式处理器"""
    FIELD_SEPARATOR = "||"
    ESCAPE_SEPARATOR = "^^"
    LINE_SEPARATOR = "\r\n"
    ENCODING = "utf-8"
    
    @classmethod
    def escape_field(cls, value) -> str:
        if value is None:
            return ""
        str_value = str(value)
        return str_value.replace(cls.FIELD_SEPARATOR, cls.ESCAPE_SEPARATOR)
    
    @classmethod
    def format_record(cls, record: TokenStatistics) -> str:
        fields = [
            record.day_id,
            record.prov_id,
            record.model_type,
            record.model_name,
            record.total_call_count,
            record.total_call_success_count,
            record.total_token_count,
            record.modle_size if record.modle_size is not None else "",
            record.quant_level if record.quant_level is not None else "",
            record.hardware_type if record.hardware_type else "",
            record.department if record.department else "",
            record.use_case if record.use_case else "",
            record.iternal_app if record.iternal_app else ""
        ]
        escaped_fields = [cls.escape_field(f) for f in fields]
        return cls.FIELD_SEPARATOR.join(escaped_fields)
    
    @classmethod
    def format_records(cls, records: List[TokenStatistics]) -> str:
        lines = [cls.format_record(r) for r in records]
        return cls.LINE_SEPARATOR.join(lines) + cls.LINE_SEPARATOR


# ============================================================
# Prometheus 查询
# ============================================================
class PrometheusQuerier:
    """Prometheus 查询器"""
    
    def __init__(self, prometheus_url: str, extra_labels: Dict[str, str] = None):
        self.base_url = prometheus_url.rstrip('/')
        self.query_url = f"{self.base_url}/api/v1/query"
        self.extra_labels = extra_labels or {}
    
    def _build_label_selector(self, model_label: str, model_pattern: str) -> str:
        """构建标签选择器"""
        selectors = [f'{model_label}=~"{model_pattern}"']
        for label, value in self.extra_labels.items():
            selectors.append(f'{label}="{value}"')
        return "{" + ",".join(selectors) + "}"
    
    def query(self, promql: str, time: datetime = None) -> Dict:
        params = {"query": promql}
        if time:
            params["time"] = time.timestamp()
        
        try:
            response = requests.get(self.query_url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") != "success":
                logger.error(f"Prometheus 查询失败: {result.get('error', '未知错误')}")
                return {"status": "error", "data": {"result": []}}
            
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Prometheus 请求异常: {e}")
            return {"status": "error", "data": {"result": []}}
    
    def get_increment(self, metric: str, model_label: str,
                       model_names: List[str], 
                       start_time: datetime, end_time: datetime,
                       optional: bool = False) -> Dict[str, int]:
        """
        获取指定时间范围内多个模型的增量数据
        
        Args:
            metric: 指标名称
            model_label: 模型标签名
            model_names: 模型名称列表
            start_time: 开始时间
            end_time: 结束时间
            optional: 如果为 True，当指标不存在时返回空字典而不报错
        """
        results = {}
        model_pattern = "|".join(model_names)
        
        # 计算时间范围（秒）
        duration_seconds = int((end_time - start_time).total_seconds())
        duration_str = f"{duration_seconds}s"
        
        label_selector = self._build_label_selector(model_label, model_pattern)
        promql = f'sum by ({model_label}) (increase({metric}{label_selector}[{duration_str}]))'
        logger.debug(f"PromQL: {promql}")
        logger.debug(f"时间范围: {start_time} -> {end_time} ({duration_seconds}s)")
        
        result = self.query(promql, end_time)
        
        if result.get("status") == "success":
            for item in result.get("data", {}).get("result", []):
                model = item.get("metric", {}).get(model_label, "unknown")
                value = item.get("value", [None, "0"])
                try:
                    results[model] = int(float(value[1]))
                except (ValueError, TypeError, IndexError):
                    results[model] = 0
        elif not optional:
            logger.warning(f"指标 {metric} 查询失败")
        
        return results
    
    def get_daily_sum_increment(self, metric: str, model_label: str,
                                model_names: List[str], date: datetime,
                                optional: bool = False) -> Dict[str, int]:
        """获取指定日期的日增量（固定日期模式：0点-24点）"""
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        return self.get_increment(metric, model_label, model_names, day_start, day_end, optional)


# ============================================================
# SFTP 上传
# ============================================================
class SFTPUploader:
    """SFTP 上传器"""
    
    def __init__(self, config: Dict):
        self.host = config.get("host", "")
        self.port = config.get("port", 22)
        self.username = config.get("username", "")
        self.password = config.get("password", "")
        self.private_key_path = config.get("private_key_path", "")
        self.remote_dir = config.get("remote_dir", "/")
        self.timeout = config.get("timeout", 30)
        self.enabled = config.get("enabled", True)
    
    def upload_files(self, file_paths: List[str], dry_run: bool = False) -> Dict:
        """
        上传文件到SFTP服务器
        
        Args:
            file_paths: 本地文件路径列表
            dry_run: 如果为True，只模拟上传，不实际执行
        
        Returns:
            上传结果字典
        """
        result = {
            "success": False,
            "uploaded": [],
            "failed": [],
            "errors": []
        }
        
        if not self.enabled:
            logger.info("SFTP 上传已禁用")
            result["success"] = True
            result["errors"].append("SFTP已禁用，跳过上传")
            return result
        
        if dry_run:
            logger.info("[DRY-RUN] 模拟SFTP上传:")
            for fp in file_paths:
                filename = os.path.basename(fp)
                remote_path = f"{self.remote_dir}/{filename}"
                logger.info(f"  [DRY-RUN] {fp} -> {self.host}:{remote_path}")
                result["uploaded"].append({
                    "local": fp,
                    "remote": remote_path,
                    "dry_run": True
                })
            result["success"] = True
            return result
        
        try:
            import paramiko
        except ImportError:
            logger.error("请先安装 paramiko 模块: pip install paramiko")
            result["errors"].append("缺少 paramiko 模块")
            return result
        
        transport = None
        sftp = None
        
        try:
            logger.info(f"连接SFTP服务器: {self.host}:{self.port}")
            
            transport = paramiko.Transport((self.host, self.port))
            
            # 认证方式：密钥或密码
            if self.private_key_path and os.path.exists(self.private_key_path):
                logger.info(f"使用密钥认证: {self.private_key_path}")
                pkey = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                transport.connect(username=self.username, pkey=pkey)
            else:
                logger.info(f"使用密码认证")
                transport.connect(username=self.username, password=self.password)
            
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # 确保远程目录存在
            self._ensure_remote_dir(sftp, self.remote_dir)
            
            # 上传文件
            for local_path in file_paths:
                filename = os.path.basename(local_path)
                remote_path = f"{self.remote_dir}/{filename}"
                
                try:
                    logger.info(f"上传文件: {local_path} -> {remote_path}")
                    sftp.put(local_path, remote_path)
                    result["uploaded"].append({
                        "local": local_path,
                        "remote": remote_path
                    })
                    logger.info(f"上传成功: {filename}")
                except Exception as e:
                    logger.error(f"上传失败 {filename}: {e}")
                    result["failed"].append({
                        "local": local_path,
                        "error": str(e)
                    })
            
            result["success"] = len(result["failed"]) == 0
            
        except Exception as e:
            logger.error(f"SFTP 连接/上传失败: {e}")
            result["errors"].append(str(e))
        
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()
        
        return result
    
    def _ensure_remote_dir(self, sftp, remote_dir: str):
        """确保远程目录存在"""
        dirs = remote_dir.strip('/').split('/')
        current_path = ""
        
        for d in dirs:
            current_path = f"{current_path}/{d}"
            try:
                sftp.stat(current_path)
            except FileNotFoundError:
                logger.info(f"创建远程目录: {current_path}")
                sftp.mkdir(current_path)


# ============================================================
# 数据采集和文件生成
# ============================================================
class DataCollectorAndGenerator:
    """数据采集与文件生成器"""
    
    def __init__(self, config: Dict):
        self.config = config
        prometheus_config = config.get("prometheus", {})
        self.prometheus = PrometheusQuerier(
            prometheus_config.get("url", "http://prometheus:9090"),
            prometheus_config.get("extra_label_filters", {})
        )
        
        output_config = config.get("output", {})
        self.data_type = output_config.get("data_type", "token_info")
        self.protocol = output_config.get("protocol", "0")
        self.prov_id = output_config.get("prov_id", "831")
        self.data_dir = output_config.get("local_data_dir", "./data_files")
        
        # 时间范围模式: "fixed_day" (固定日期0点-24点) 或 "rolling_24h" (当前时间往前24小时)
        self.time_range_mode = config.get("time_range_mode", "fixed_day")
        
        # 创建数据目录
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.metrics_presets = config.get("metrics_presets", {})
        self.default_engine = config.get("default_engine", "vllm")
        self.metrics_optional = config.get("metrics_optional", {})
    
    def get_metrics_config_for_engine(self, engine: str) -> Dict:
        """根据引擎名称获取对应的指标配置"""
        if engine in self.metrics_presets:
            return self.metrics_presets[engine].copy()
        return self.metrics_presets.get("vllm", {}).copy()
    
    def get_model_engine(self, model_config: Dict) -> str:
        """获取模型使用的引擎"""
        return model_config.get("engine", self.default_engine)
    
    def _calculate_time_range(self, date: datetime = None) -> tuple:
        """
        根据配置的时间范围模式计算查询的开始和结束时间
        
        Returns:
            (start_time, end_time, day_id)
        """
        if self.time_range_mode == "rolling_24h":
            # 滚动24小时模式：当前时间往前24小时
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            # day_id 使用结束时间的日期
            day_id = end_time.strftime("%Y%m%d")
            logger.info(f"时间范围模式: rolling_24h (滚动24小时)")
            logger.info(f"  开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            # 固定日期模式：指定日期的0点到24点
            if date is None:
                date = datetime.now() - timedelta(days=1)
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            day_id = date.strftime("%Y%m%d")
            logger.info(f"时间范围模式: fixed_day (固定日期)")
            logger.info(f"  日期: {day_id}")
            logger.info(f"  开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return start_time, end_time, day_id
    
    def collect_daily_stats(self, date: datetime = None) -> List[TokenStatistics]:
        """采集指定时间范围的统计数据"""
        start_time, end_time, day_id = self._calculate_time_range(date)
        
        logger.info(f"开始采集 {day_id} 的统计数据...")
        
        # 按 engine 分组模型
        engine_models = {}
        for model_config in self.config.get("models", []):
            engine = self.get_model_engine(model_config)
            if engine not in engine_models:
                engine_models[engine] = []
            engine_models[engine].append(model_config)
        
        logger.info(f"检测到 {len(engine_models)} 种推理引擎: {list(engine_models.keys())}")
        
        # 按 engine 分组查询
        model_metrics = {}
        
        for engine, models in engine_models.items():
            metrics_config = self.get_metrics_config_for_engine(engine)
            model_label = metrics_config.get("model_label", "model_name")
            
            logger.info(f"--- 查询引擎 [{engine}] 的模型 ({len(models)} 个) ---")
            
            # 收集该引擎的所有 label
            engine_labels = []
            for m in models:
                label_match = m["label_match"]
                if isinstance(label_match, list):
                    engine_labels.extend(label_match)
                else:
                    engine_labels.append(label_match)
            
            # 查询各指标
            logger.info(f"  [{engine}] 查询请求总数...")
            request_totals = self.prometheus.get_increment(
                metrics_config.get("request_total", ""), 
                model_label, 
                engine_labels,
                start_time, end_time,
                optional=self.metrics_optional.get("request_total", False)
            )
            
            logger.info(f"  [{engine}] 查询成功请求数...")
            success_totals = self.prometheus.get_increment(
                metrics_config.get("request_success", ""), 
                model_label, 
                engine_labels,
                start_time, end_time,
                optional=self.metrics_optional.get("request_success", False)
            )
            
            logger.info(f"  [{engine}] 查询 Prompt Token 消耗...")
            prompt_tokens = self.prometheus.get_increment(
                metrics_config.get("tokens_total", ""), 
                model_label, 
                engine_labels,
                start_time, end_time,
                optional=self.metrics_optional.get("tokens_total", False)
            )
            
            logger.info(f"  [{engine}] 查询 Generation Token 消耗...")
            gen_tokens = self.prometheus.get_increment(
                metrics_config.get("generation_tokens", ""), 
                model_label, 
                engine_labels,
                start_time, end_time,
                optional=self.metrics_optional.get("generation_tokens", False)
            )
            
            # 将查询结果存入 model_metrics
            for label in engine_labels:
                model_metrics[label] = {
                    "request_total": request_totals.get(label, 0),
                    "request_success": success_totals.get(label, 0),
                    "tokens_total": prompt_tokens.get(label, 0),
                    "generation_tokens": gen_tokens.get(label, 0)
                }
        
        # 组装记录
        records = []
        for model_config in self.config.get("models", []):
            label_match = model_config["label_match"]
            labels_to_sum = label_match if isinstance(label_match, list) else [label_match]
            engine = self.get_model_engine(model_config)
            
            total_calls = sum(model_metrics.get(lbl, {}).get("request_total", 0) for lbl in labels_to_sum)
            success_calls = sum(model_metrics.get(lbl, {}).get("request_success", 0) for lbl in labels_to_sum)
            prompt_token_sum = sum(model_metrics.get(lbl, {}).get("tokens_total", 0) for lbl in labels_to_sum)
            gen_token_sum = sum(model_metrics.get(lbl, {}).get("generation_tokens", 0) for lbl in labels_to_sum)
            total_tokens = prompt_token_sum + gen_token_sum
            
            if total_calls == 0 and success_calls == 0 and total_tokens == 0:
                logger.warning(f"模型 {model_config['name']} (engine={engine}) 无数据，跳过")
                continue
            
            record = TokenStatistics(
                day_id=day_id,
                prov_id=self.prov_id,
                model_type=model_config["model_type"],
                model_name=model_config["name"],
                total_call_count=total_calls,
                total_call_success_count=success_calls,
                total_token_count=total_tokens,
                modle_size=model_config.get("modle_size"),
                quant_level=model_config.get("quant_level"),
                hardware_type=model_config.get("hardware_type"),
                department=model_config.get("department"),
                use_case=model_config.get("use_case"),
                iternal_app=model_config.get("iternal_app")
            )
            
            records.append(record)
            logger.info(f"  {model_config['name']} [{engine}]: 调用={total_calls}, 成功={success_calls}, Tokens={total_tokens}")
        
        logger.info(f"采集完成，共 {len(records)} 条记录")
        return records
    
    def get_storage_path(self, date: datetime) -> str:
        """获取存储路径"""
        date_dir = os.path.join(self.data_dir, self.data_type, date.strftime("%Y%m%d"))
        os.makedirs(date_dir, exist_ok=True)
        return date_dir
    
    def generate_filename(self, start_time: datetime, end_time: datetime) -> str:
        """生成文件名"""
        gen_time = datetime.now()
        time_format = "%Y%m%d%H%M%S"
        return (
            f"{self.data_type}_{self.protocol}_"
            f"{start_time.strftime(time_format)}_"
            f"{end_time.strftime(time_format)}_"
            f"{gen_time.strftime(time_format)}.txt.gz"
        )
    
    def generate_check_filename(self, start_time: datetime, end_time: datetime) -> str:
        """生成稽核文件名"""
        time_format = "%Y%m%d%H%M%S"
        return (
            f"{self.data_type}_{self.protocol}_"
            f"{start_time.strftime(time_format)}_"
            f"{end_time.strftime(time_format)}.chk"
        )
    
    def generate_files(self, date: datetime = None) -> dict:
        """采集数据并生成文件"""
        result = {
            'success': False,
            'data_file': None,
            'check_file': None,
            'record_count': 0,
            'file_paths': [],
            'time_range': {},
            'errors': []
        }
        
        try:
            # 计算时间范围
            start_time, end_time, day_id = self._calculate_time_range(date)
            result['time_range'] = {
                'mode': self.time_range_mode,
                'start': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'day_id': day_id
            }
            
            # 1. 采集数据
            records = self.collect_daily_stats(date)
            
            if not records:
                result['errors'].append("没有采集到任何数据")
                return result
            
            # 2. 生成文件
            # 文件名使用实际的时间范围
            file_start = start_time
            file_end = end_time - timedelta(seconds=1)
            
            storage_path = self.get_storage_path(start_time)
            filename = self.generate_filename(file_start, file_end)
            temp_filepath = os.path.join(storage_path, filename + ".tmp")
            final_filepath = os.path.join(storage_path, filename)
            
            # 格式化并写入
            content = FileFormatter.format_records(records)
            with gzip.open(temp_filepath, 'wt', encoding=FileFormatter.ENCODING) as f:
                f.write(content)
            
            file_size = os.path.getsize(temp_filepath)
            os.rename(temp_filepath, final_filepath)
            
            result['data_file'] = {
                'filename': filename,
                'filepath': final_filepath,
                'size': file_size
            }
            result['file_paths'].append(final_filepath)
            result['record_count'] = len(records)
            
            logger.info(f"数据文件生成成功: {final_filepath}")
            
            # 3. 生成稽核文件
            check_filename = self.generate_check_filename(file_start, file_end)
            check_filepath = os.path.join(storage_path, check_filename)
            check_content = f"{len(records)} {file_size} {filename}\r\n"
            
            with open(check_filepath, 'w', encoding='utf-8') as f:
                f.write(check_content)
            
            result['check_file'] = {
                'filename': check_filename,
                'filepath': check_filepath
            }
            result['file_paths'].append(check_filepath)
            
            logger.info(f"稽核文件生成成功: {check_filepath}")
            
            result['success'] = True
            
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"文件生成失败: {e}")
        
        return result


# ============================================================
# 定时调度
# ============================================================
def run_scheduled_task(config: Dict, dry_run: bool = False):
    """执行定时任务"""
    logger.info("=" * 60)
    logger.info("定时任务开始执行")
    logger.info("=" * 60)
    
    # 1. 采集数据并生成文件
    collector = DataCollectorAndGenerator(config)
    result = collector.generate_files()
    
    if not result['success']:
        logger.error(f"数据采集/文件生成失败: {result['errors']}")
        return
    
    logger.info(f"数据文件: {result['data_file']['filename']}")
    logger.info(f"稽核文件: {result['check_file']['filename']}")
    logger.info(f"记录数: {result['record_count']}")
    
    # 2. SFTP 上传
    sftp_config = config.get("sftp", {})
    if sftp_config.get("enabled", False):
        uploader = SFTPUploader(sftp_config)
        upload_result = uploader.upload_files(result['file_paths'], dry_run=dry_run)
        
        if upload_result['success']:
            logger.info("SFTP上传成功")
            for item in upload_result['uploaded']:
                logger.info(f"  已上传: {item.get('local')} -> {item.get('remote')}")
        else:
            logger.error(f"SFTP上传失败: {upload_result['errors']}")
            for item in upload_result['failed']:
                logger.error(f"  失败: {item.get('local')} - {item.get('error')}")
    else:
        logger.info("SFTP上传已禁用，跳过上传步骤")
    
    logger.info("=" * 60)
    logger.info("定时任务执行完成")
    logger.info("=" * 60)


def scheduler_thread(config: Dict, dry_run: bool = False):
    """定时调度线程"""
    try:
        import schedule
    except ImportError:
        logger.error("请先安装 schedule 模块: pip install schedule")
        return
    
    schedule_time = config.get("schedule_time", "00:00")
    logger.info(f"定时任务已设置，将在每天 {schedule_time} 执行")
    
    schedule.every().day.at(schedule_time).do(run_scheduled_task, config, dry_run)
    
    while True:
        schedule.run_pending()
        time.sleep(60)


# ============================================================
# 主函数
# ============================================================
def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="数据推送脚本 - 定时采集并通过SFTP推送数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python data_push.py                          # 启动定时任务
  python data_push.py --once                   # 立即执行一次
  python data_push.py --once --date 20250114   # 采集指定日期
  python data_push.py --dry-run                # 模拟运行
  python data_push.py --config /path/to/config.json  # 指定配置文件
        """
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="配置文件路径（默认：当前目录下的 config.json）"
    )
    
    parser.add_argument(
        "--once",
        action="store_true",
        help="立即执行一次，不启动定时调度"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="指定采集日期，格式：YYYYMMDD（默认为昨天）"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="模拟运行，不实际进行SFTP传输"
    )
    
    args = parser.parse_args()
    
    # 加载配置
    config = load_config(args.config)
    
    # 解析日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            logger.error(f"日期格式错误: {args.date}，应为 YYYYMMDD")
            return
    
    if args.once:
        # 单次执行模式
        logger.info("=" * 60)
        logger.info("单次执行模式")
        logger.info("=" * 60)
        
        # 1. 采集数据并生成文件
        collector = DataCollectorAndGenerator(config)
        result = collector.generate_files(target_date)
        
        if not result['success']:
            logger.error(f"执行失败: {result['errors']}")
            return
        
        logger.info(f"数据文件: {result['data_file']['filename']}")
        logger.info(f"稽核文件: {result['check_file']['filename']}")
        logger.info(f"记录数: {result['record_count']}")
        
        # 2. SFTP 上传
        sftp_config = config.get("sftp", {})
        if sftp_config.get("enabled", False):
            uploader = SFTPUploader(sftp_config)
            upload_result = uploader.upload_files(result['file_paths'], dry_run=args.dry_run)
            
            if upload_result['success']:
                logger.info("SFTP上传成功")
                for item in upload_result['uploaded']:
                    logger.info(f"  已上传: {item.get('local')} -> {item.get('remote')}")
            else:
                logger.error(f"SFTP上传失败: {upload_result['errors']}")
        else:
            logger.info("SFTP上传已禁用")
        
        logger.info("=" * 60)
        logger.info("执行完成")
        logger.info("=" * 60)
    
    else:
        # 定时任务模式
        schedule_time = config.get("schedule_time", "00:00")
        sftp_config = config.get("sftp", {})
        
        time_range_mode = config.get("time_range_mode", "fixed_day")
        mode_desc = "滚动24小时" if time_range_mode == "rolling_24h" else "固定日期(前一天)"
        
        print(f"""
╔═══════════════════════════════════════════════════════════════╗
║             数据推送脚本 - 定时采集并通过SFTP推送             ║
╠═══════════════════════════════════════════════════════════════╣
║  定时执行: 每天 {schedule_time} 自动执行                               ║
║  时间范围: {mode_desc:55s} ║
║                                                               ║
║  SFTP配置:                                                    ║
║  ├── 启用状态: {'已启用' if sftp_config.get('enabled') else '已禁用':53s} ║
║  ├── 远程主机: {sftp_config.get('host', 'N/A'):53s} ║
║  ├── 远程端口: {str(sftp_config.get('port', 22)):53s} ║
║  └── 远程目录: {sftp_config.get('remote_dir', 'N/A'):53s} ║
║                                                               ║
║  模式: {'模拟运行' if args.dry_run else '正常运行':58s} ║
║                                                               ║
║  按 Ctrl+C 停止服务                                           ║
╚═══════════════════════════════════════════════════════════════╝
        """)
        
        try:
            scheduler_thread(config, dry_run=args.dry_run)
        except KeyboardInterrupt:
            logger.info("服务已停止")


if __name__ == "__main__":
    main()
