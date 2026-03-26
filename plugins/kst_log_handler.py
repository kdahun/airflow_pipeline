"""
KST 기반 태스크 로그 핸들러
============================
Airflow 기본 FileTaskHandler를 상속하여 로그 파일명을 아래 형식으로 변경한다.

  {dag_id}/{task_id}/{run_type}_{YYYY-MM-DD}_{HHMM}_attempt{n}.log

예시:
  ais_data_pipeline/rssi_analysis_task/scheduled_2026-03-26_1430_attempt1.log

설정 방법 (docker-compose.yml 환경변수):
  AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS: kst_log_handler.LOGGING_CONFIG
"""

from __future__ import annotations

import os
from copy import deepcopy

import pendulum
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.utils.log.file_task_handler import FileTaskHandler

KST = pendulum.timezone("Asia/Seoul")


class KSTFileTaskHandler(FileTaskHandler):
    """파일명에 KST 시각을 사용하는 태스크 로그 핸들러."""

    def _render_filename(self, ti, try_number: int) -> str:
        try:
            exec_dt = pendulum.instance(ti.execution_date).in_timezone(KST)
        except Exception:
            exec_dt = pendulum.now(KST)

        # run_id 예: scheduled__2026-03-26T05:30:00+00:00  →  "scheduled"
        run_type = ti.run_id.split("__")[0]
        date_str = exec_dt.strftime("%Y-%m-%d")
        time_str = exec_dt.strftime("%H%M")

        return (
            f"{ti.dag_id}/{ti.task_id}/"
            f"{run_type}_{date_str}_{time_str}_attempt{try_number}.log"
        )


# Airflow 기본 로깅 설정을 복사한 뒤 핸들러 클래스만 교체
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["task"]["class"] = (
    "kst_log_handler.KSTFileTaskHandler"
)
