"""
RSSI / SNR 분석 모듈
====================
rssi_analysis_test.ipynb 을 Airflow 실행용 Python 모듈로 변환.

Cassandra에서 지정 시간 범위의 AIS 데이터를 조회한 뒤
RSSI/SNR 통계를 로그로 기록하고, 시계열 차트를 PNG 파일로 저장한다.

사용:
    from rssi_analysis import run
    run(start_dt, end_dt)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE = "ais_class_a_dynamic"

CHART_DIR = os.getenv("RSSI_CHART_DIR", "/opt/airflow/logs/rssi")

COLUMNS = (
    "mmsi, date_bucket, received_at, msg_type, sog, cog, "
    "nav_status, longitude, latitude, rssi, slot_num, snr"
)


# ──────────────────────────────────────────────────────────────
# Cassandra 조회
# ──────────────────────────────────────────────────────────────

def _load_from_cassandra(
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    limit: int = 100_000,
) -> pd.DataFrame:
    logger.info("[Cassandra] 연결: %s:%s / keyspace=%s", CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE)

    try:
        cluster = Cluster(
            contact_points=[CASSANDRA_HOST],
            port=CASSANDRA_PORT,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            protocol_version=4,
        )
        session = cluster.connect(CASSANDRA_KEYSPACE)
    except NoHostAvailable as exc:
        raise RuntimeError(f"Cassandra 연결 실패: {exc}") from exc

    rows: list[dict] = []
    try:
        cql = f"SELECT {COLUMNS} FROM {CASSANDRA_TABLE}"
        params: list = []
        conditions: list[str] = []

        if start_dt is not None:
            conditions.append("date_bucket >= %s")
            params.append(start_dt)
        if end_dt is not None:
            conditions.append("date_bucket <= %s")
            params.append(end_dt)

        if conditions:
            cql += " WHERE " + " AND ".join(conditions) + " ALLOW FILTERING"
        cql += f" LIMIT {limit}"

        stmt = SimpleStatement(cql, fetch_size=1000)
        result = session.execute(stmt, params)
        rows = [
            {
                "mmsi": row.mmsi,
                "date_bucket": row.date_bucket,
                "received_at": row.received_at,
                "msg_type": row.msg_type,
                "sog": row.sog,
                "cog": row.cog,
                "nav_status": row.nav_status,
                "longitude": row.longitude,
                "latitude": row.latitude,
                "rssi": row.rssi,
                "slot_num": row.slot_num,
                "snr": row.snr,
            }
            for row in result
        ]
    finally:
        cluster.shutdown()

    if not rows:
        logger.warning("[조회] 결과 없음 — 빈 DataFrame 반환")
        return pd.DataFrame(columns=COLUMNS.replace(" ", "").split(","))

    df = pd.DataFrame(rows)
    for col in ("date_bucket", "received_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    logger.info("[조회] %s → %s건 로드 완료", CASSANDRA_TABLE, f"{len(df):,}")
    return df


# ──────────────────────────────────────────────────────────────
# 메인 실행 함수
# ──────────────────────────────────────────────────────────────

def run(start_dt: datetime, end_dt: datetime) -> None:
    """지정 시간 범위의 RSSI/SNR 통계를 로그로 기록하고 차트를 PNG로 저장한다."""
    df = _load_from_cassandra(start_dt=start_dt, end_dt=end_dt)

    if df.empty:
        logger.warning("데이터가 없어 분석을 건너뜁니다.")
        return

    df_sort = df.sort_values("date_bucket").copy()

    # --- RSSI / SNR DataFrame 로그 기록 ---
    rssi_stats = df_sort["rssi"].describe()
    snr_stats = df_sort["snr"].describe()
    logger.info("=== RSSI 통계 ===\n%s", rssi_stats.to_string())
    logger.info("=== SNR 통계 ===\n%s", snr_stats.to_string())

    rssi_snr_df = df_sort[["mmsi", "date_bucket", "rssi", "snr"]].copy()
    logger.info("=== RSSI/SNR DataFrame (상위 50건) ===\n%s", rssi_snr_df.head(50).to_string())

    # --- 시계열 차트 생성 및 PNG 저장 ---
    fig, axes = plt.subplots(2, 1, figsize=(18, 8), sharex=True)
    fig.suptitle("수신 시각별 RSSI / SNR", fontsize=14, fontweight="bold")

    axes[0].plot(df_sort["date_bucket"], df_sort["rssi"], linewidth=0.5, color="steelblue")
    rssi_mean = df_sort["rssi"].mean()
    axes[0].axhline(rssi_mean, color="red", linestyle="--", linewidth=1,
                    label=f"평균: {rssi_mean:.1f} dBm")
    axes[0].set_ylabel("RSSI (dBm)")
    axes[0].legend(fontsize=10)
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(df_sort["date_bucket"], df_sort["snr"], linewidth=0.5, color="darkorange")
    snr_mean = df_sort["snr"].mean()
    axes[1].axhline(snr_mean, color="red", linestyle="--", linewidth=1,
                    label=f"평균: {snr_mean:.1f} dB")
    axes[1].set_ylabel("SNR (dB)")
    axes[1].set_xlabel("수신 시각")
    axes[1].legend(fontsize=10)
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()

    chart_dir = Path(CHART_DIR)
    chart_dir.mkdir(parents=True, exist_ok=True)
    filename = f"rssi_{start_dt.strftime('%Y%m%d_%H%M')}.png"
    chart_path = chart_dir / filename
    fig.savefig(str(chart_path), dpi=150)
    plt.close(fig)
    logger.info("차트 저장 완료: %s", chart_path)
