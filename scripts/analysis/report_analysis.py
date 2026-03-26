"""
AIS 슬롯 검증 분석 모듈
========================
report_analysis_test.ipynb 을 Airflow 실행용 Python 모듈로 변환.

Cassandra에서 지정 시간 범위의 AIS 데이터를 조회한 뒤
슬롯 간격(NI) 기반 이상 여부를 검증하고 결과를 로그로 기록한다.

사용:
    from report_analysis import run
    run(start_dt, end_dt)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE = "ais_class_a_dynamic"

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
            conditions.append("date_bucket >= ?")
            params.append(start_dt.replace(tzinfo=None))
        if end_dt is not None:
            conditions.append("date_bucket <= ?")
            params.append(end_dt.replace(tzinfo=None))

        if conditions:
            cql += " WHERE " + " AND ".join(conditions)
        cql += f" LIMIT {limit}"
        if conditions:
            cql += " ALLOW FILTERING"

        stmt = session.prepare(cql)
        stmt.fetch_size = 1000
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
# 슬롯 검증 로직
# ──────────────────────────────────────────────────────────────

def _get_ni(sog_raw) -> Optional[int]:
    """SOG(×10 저장) → NI(Nominal Increment) 반환."""
    sog = sog_raw / 10.0
    if sog == 0:
        return None
    elif sog < 14:
        return 375
    elif sog < 23:
        return 225
    else:
        return 75


def _slot_diff_wrap(prev, curr, max_slot=2250) -> int:
    diff = int(curr) - int(prev)
    if diff < 0:
        diff += max_slot
    return diff


def _is_slot_valid(row) -> Optional[bool]:
    ni = row["NI"]
    if ni is None:
        return None
    margin = 0.2 * ni
    diff = row["slot_diff"]
    return (ni - margin) <= diff <= (ni + margin)


# ──────────────────────────────────────────────────────────────
# 메인 실행 함수
# ──────────────────────────────────────────────────────────────

def run(start_dt: datetime, end_dt: datetime) -> None:
    """지정 시간 범위의 AIS 데이터에 대해 슬롯 검증을 수행하고 결과를 로그로 기록한다."""
    df = _load_from_cassandra(start_dt=start_dt, end_dt=end_dt)

    if df.empty:
        logger.warning("데이터가 없어 분석을 건너뜁니다.")
        return

    df_sort = df.sort_values(["mmsi", "date_bucket"]).copy()

    df_check = df_sort.copy()
    df_check["prev_slot"] = df_check.groupby("mmsi")["slot_num"].shift(1)
    df_check["prev_sog"] = df_check.groupby("mmsi")["sog"].shift(1)
    df_check = df_check.dropna(subset=["prev_slot", "prev_sog"]).copy()
    df_check["prev_slot"] = df_check["prev_slot"].astype(int)
    df_check["NI"] = df_check["prev_sog"].apply(_get_ni)
    df_check["slot_diff"] = df_check.apply(
        lambda r: _slot_diff_wrap(r["prev_slot"], r["slot_num"]), axis=1
    )
    df_check["slot_valid"] = df_check.apply(_is_slot_valid, axis=1)

    # --- 전체 슬롯 검증 결과 ---
    logger.info("=== 전체 슬롯 검증 결과 ===")
    logger.info("\n%s", df_check["slot_valid"].value_counts(dropna=False).to_string())

    # --- 선박별 요약 ---
    summary = df_check.groupby("mmsi")["slot_valid"].agg(
        total="count",
        valid=lambda x: x.sum(),
        invalid=lambda x: (x == False).sum(),  # noqa: E712
        sog_zero=lambda x: x.isna().sum(),
    )
    summary["invalid_rate(%)"] = (summary["invalid"] / summary["total"] * 100).round(2)
    logger.info("=== 선박별 요약 ===")
    logger.info("\n%s", summary.to_string())

    # --- 이상 레코드 ---
    df_invalid = df_check[df_check["slot_valid"] == False]  # noqa: E712
    logger.info("이상 레코드 수: %d", len(df_invalid))
    if not df_invalid.empty:
        logger.info(
            "\n%s",
            df_invalid[["mmsi", "date_bucket", "prev_slot", "slot_num", "slot_diff", "NI", "prev_sog"]].to_string(),
        )
