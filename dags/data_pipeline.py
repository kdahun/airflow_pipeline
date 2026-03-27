"""
AIS 데이터 파이프라인 DAG (Spark)
==================================
10분마다 실행:
  1) process_task          — HDFS → Spark 파싱 → Cassandra 저장 + MMSI 집계
  2) report_analysis_task  — Cassandra → Spark 슬롯 검증 (process_task 완료 후)
  3) rssi_analysis_task    — Cassandra → Spark RSSI 분석 + 차트 (process_task 완료 후)

2·3은 병렬 실행. 모든 태스크는 spark-worker-1에서 연산한다.
"""

import logging
import os
from typing import Tuple

import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "dlim-cassandra-1")
HDFS_NAMENODE  = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

SPARK_CONF = {
    "spark.hadoop.fs.defaultFS": HDFS_NAMENODE,
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.host": "airflow-scheduler",
}

SPARK_ENV = {
    "CASSANDRA_HOST": CASSANDRA_HOST,
    "HDFS_NAMENODE": HDFS_NAMENODE,
}


def _hdfs_path() -> str:
    """현재 시각(KST) 기준으로 처리할 HDFS 경로를 계산한다."""
    now_kst = pendulum.now(KST)
    ref_kst = now_kst.subtract(minutes=5)
    floored_minute = (ref_kst.minute // 10) * 10
    target_kst = ref_kst.replace(minute=floored_minute, second=0, microsecond=0)
    return target_kst.strftime("/datalake/ais/%Y/%m/%d/%H/%M")


def _time_range() -> Tuple[str, str]:
    """현재 시각(KST) 기준 최근 10분의 ISO 시각 쌍을 반환한다."""
    now_kst = pendulum.now(KST)
    end_dt = now_kst
    start_dt = now_kst.subtract(minutes=10)
    return (
        start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
    )


with DAG(
    dag_id="ais_data_pipeline",
    schedule="*/10 * * * *",
    start_date=pendulum.today(KST),
    catchup=False,
    tags=["ais", "pipeline", "spark"],
) as dag:

    hdfs_path = _hdfs_path()
    start_iso, end_iso = _time_range()

    process_task = SparkSubmitOperator(
        task_id="process_task",
        application="/opt/airflow/scripts/spark/spark_process.py",
        conn_id="spark_default",
        application_args=[hdfs_path],
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        name="ais_process",
    )

    report_analysis_task = SparkSubmitOperator(
        task_id="report_analysis_task",
        application="/opt/airflow/scripts/spark/spark_report_analysis.py",
        conn_id="spark_default",
        application_args=[start_iso, end_iso],
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        name="ais_report_analysis",
    )

    rssi_analysis_task = SparkSubmitOperator(
        task_id="rssi_analysis_task",
        application="/opt/airflow/scripts/spark/spark_rssi_analysis.py",
        conn_id="spark_default",
        application_args=[start_iso, end_iso],
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        name="ais_rssi_analysis",
    )

    process_task >> [report_analysis_task, rssi_analysis_task]
