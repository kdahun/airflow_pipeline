"""
AIS 데이터 파이프라인 DAG
=========================
10분마다 실행:
  1) process_task  — HDFS에서 AIS 데이터를 읽어 Cassandra에 저장 + MMSI별 집계
  2) report_analysis_task — 슬롯 검증 분석 (process_task 완료 후)
  3) rssi_analysis_task   — RSSI/SNR 분석 (process_task 완료 후)

2·3은 병렬 실행.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import hdfs_reader
import mmsi_summary
import report_analysis
import rssi_analysis

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Task callable 함수
# ──────────────────────────────────────────────────────────────

def run_process(**context) -> None:
    """HDFS에서 data_interval_start 시점의 10분 데이터를 읽어 Cassandra에 저장한다."""
    data_interval_start: datetime = context["data_interval_start"]

    hdfs_path = mmsi_summary.get_hdfs_path(data_interval_start)
    logger.info("HDFS 경로: %s", hdfs_path)

    if not hdfs_reader.check_connection():
        raise RuntimeError("HDFS 연결 실패")

    df = mmsi_summary.build_summary(hdfs_path, save=mmsi_summary.SAVE_TO_CASSANDRA)

    if mmsi_summary.SAVE_TO_CASSANDRA:
        import cassandra_save
        cassandra_save.close()

    if df.empty:
        logger.warning("데이터가 없습니다: %s", hdfs_path)
        return

    logger.info("결과 — 총 %s척 / %s건", f"{len(df):,}", f"{df['count'].sum():,}")
    logger.info("\n%s", df.to_string(index=True))


def run_report(**context) -> None:
    """슬롯 검증 분석을 실행한다."""
    data_interval_start: datetime = context["data_interval_start"]
    data_interval_end: datetime = context["data_interval_end"]

    logger.info("Report 분석: %s ~ %s", data_interval_start, data_interval_end)
    report_analysis.run(start_dt=data_interval_start, end_dt=data_interval_end)


def run_rssi(**context) -> None:
    """RSSI/SNR 분석을 실행한다."""
    data_interval_start: datetime = context["data_interval_start"]
    data_interval_end: datetime = context["data_interval_end"]

    logger.info("RSSI 분석: %s ~ %s", data_interval_start, data_interval_end)
    rssi_analysis.run(start_dt=data_interval_start, end_dt=data_interval_end)


# ──────────────────────────────────────────────────────────────
# DAG 정의
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id="ais_data_pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["ais", "pipeline"],
) as dag:

    process_task = PythonOperator(
        task_id="process_task",
        python_callable=run_process,
    )

    report_analysis_task = PythonOperator(
        task_id="report_analysis_task",
        python_callable=run_report,
    )

    rssi_analysis_task = PythonOperator(
        task_id="rssi_analysis_task",
        python_callable=run_rssi,
    )

    process_task >> [report_analysis_task, rssi_analysis_task]
