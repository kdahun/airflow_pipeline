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

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

import hdfs_reader
import mmsi_summary
import report_analysis
import rssi_analysis

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")


# ──────────────────────────────────────────────────────────────
# Task callable 함수
# ──────────────────────────────────────────────────────────────

def run_process(**context) -> None:
    """HDFS에서 가장 최근에 완성된 10분 버킷의 데이터를 읽어 Cassandra에 저장한다.

    5분 전 시각을 기준으로 10분 단위 내림(floor)을 적용한다.
    - 스케줄 실행(10:20:05) → 10:15 → floor → 10:10 버킷
    - 수동 실행(10:25)     → 10:20 → floor → 10:20 버킷
    """
    now_kst = pendulum.now(KST)
    ref_kst = now_kst.subtract(minutes=5)
    floored_minute = (ref_kst.minute // 10) * 10
    target_kst = ref_kst.replace(minute=floored_minute, second=0, microsecond=0)

    hdfs_path = mmsi_summary.get_hdfs_path(target_kst)
    logger.info("현재 시각(KST): %s", now_kst.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("대상 버킷(KST): %s", target_kst.strftime("%Y-%m-%d %H:%M:%S"))
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
    """슬롯 검증 분석을 실행한다 (현재 시각 KST 기준 최근 10분)."""
    now_kst = pendulum.now(KST)
    end_dt = now_kst
    start_dt = now_kst.subtract(minutes=10)

    logger.info("Report 분석: %s ~ %s (KST)", start_dt, end_dt)
    report_analysis.run(start_dt=start_dt, end_dt=end_dt)


def run_rssi(**context) -> None:
    """RSSI/SNR 분석을 실행한다 (현재 시각 KST 기준 최근 10분)."""
    now_kst = pendulum.now(KST)
    end_dt = now_kst
    start_dt = now_kst.subtract(minutes=10)

    logger.info("RSSI 분석: %s ~ %s (KST)", start_dt, end_dt)
    rssi_analysis.run(start_dt=start_dt, end_dt=end_dt)


# ──────────────────────────────────────────────────────────────
# DAG 정의
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id="ais_data_pipeline",
    schedule="*/10 * * * *",
    start_date=pendulum.today(KST),
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
