"""
AIS Type 5 메시지 무결성 분석 모듈
====================================
두 가지 무결성 검사를 수행한다:

1. 보고 주기 이상 탐지
   - AIS Type 5 표준 보고 주기: 6분 (13500 슬롯)
   - expand=0, 후보 쌍 4개 이상: 최대 +13650 슬롯 (364초)
   - expand=3 최대 혼잡: 최대 +13875 슬롯 (370초, 6분 10초)
   - 유효 범위: 300초 ~ 370초 (벗어나면 이상)

2. 선박 재원 유효성 검증
   - dlim.vessel_info 참조 테이블과 비교
   - imo_number, ship_type : 완전 일치
   - call_sign              : 정규화 일치 (대문자, @ 제거)
   - vessel_name            : 유사도 검사 (SequenceMatcher ≥ 0.85,
                              AIS 20자 절단 고려한 접두어 체크 포함)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Optional

import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

logger = logging.getLogger(__name__)

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "dlim"

TABLE_STATIC      = "ais_static_voyage"
TABLE_VESSEL_INFO = "vessel_info"

# ── 보고 주기 상수 ──────────────────────────────────────────────
SLOTS_PER_FRAME          = 2250
EXPECTED_INTERVAL_SEC    = 360   # 13500 슬롯 = 6분
MAX_INTERVAL_EXPAND0_SEC = 364   # 13650 슬롯, expand=0 4+ 후보 쌍
MAX_INTERVAL_SEC         = 370   # 13875 슬롯, expand=3 최대 혼잡
MIN_INTERVAL_SEC         = 300   # 5분 미만이면 비정상 재전송 의심

# ── 선박명 유사도 임계값 ────────────────────────────────────────
NAME_SIMILARITY_THRESHOLD = 0.85


# ══════════════════════════════════════════════════════════════
# Cassandra 연결
# ══════════════════════════════════════════════════════════════

def _connect():
    try:
        cluster = Cluster(
            contact_points=[CASSANDRA_HOST],
            port=CASSANDRA_PORT,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            protocol_version=4,
        )
        session = cluster.connect(CASSANDRA_KEYSPACE)
        return cluster, session
    except NoHostAvailable as exc:
        raise RuntimeError(f"Cassandra 연결 실패: {exc}") from exc


# ══════════════════════════════════════════════════════════════
# Cassandra 조회
# ══════════════════════════════════════════════════════════════

def _load_static_voyage(
    session,
    start_dt: datetime,
    end_dt: datetime,
    limit: int = 50_000,
) -> pd.DataFrame:
    """ais_static_voyage에서 시간 범위 내 Type 5 레코드를 로드한다."""
    cql = (
        f"SELECT mmsi, received_at, call_sign, vessel_name, ship_type, imo_number "
        f"FROM {TABLE_STATIC} "
        f"WHERE received_at >= ? AND received_at <= ? "
        f"LIMIT {limit} ALLOW FILTERING"
    )
    try:
        stmt = session.prepare(cql)
        stmt.fetch_size = 1000
        rows = [
            {
                "mmsi":        row.mmsi,
                "received_at": row.received_at,
                "call_sign":   row.call_sign,
                "vessel_name": row.vessel_name,
                "ship_type":   row.ship_type,
                "imo_number":  row.imo_number,
            }
            for row in session.execute(stmt, [
                start_dt.astimezone(timezone.utc).replace(tzinfo=None),
                end_dt.astimezone(timezone.utc).replace(tzinfo=None),
            ])
        ]
    except Exception as exc:
        logger.error("[Cassandra] %s 조회 실패: %s", TABLE_STATIC, exc)
        rows = []

    cols = ["mmsi", "received_at", "call_sign", "vessel_name", "ship_type", "imo_number"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)


def _load_vessel_info(session) -> pd.DataFrame:
    """vessel_info 참조 테이블 전체를 로드한다."""
    cql = (
        f"SELECT mmsi, call_sign, vessel_name, ship_type, imo_number, "
        f"dimension_a, dimension_b, dimension_c, dimension_d "
        f"FROM {TABLE_VESSEL_INFO}"
    )
    stmt = SimpleStatement(cql, fetch_size=5000)
    try:
        rows = [
            {
                "mmsi":            row.mmsi,
                "ref_call_sign":   row.call_sign,
                "ref_vessel_name": row.vessel_name,
                "ref_ship_type":   row.ship_type,
                "ref_imo_number":  row.imo_number,
                "ref_dim_a":       row.dimension_a,
                "ref_dim_b":       row.dimension_b,
                "ref_dim_c":       row.dimension_c,
                "ref_dim_d":       row.dimension_d,
            }
            for row in session.execute(stmt)
        ]
    except Exception as exc:
        logger.error("[Cassandra] %s 조회 실패: %s", TABLE_VESSEL_INFO, exc)
        rows = []

    cols = ["mmsi", "ref_call_sign", "ref_vessel_name", "ref_ship_type", "ref_imo_number",
            "ref_dim_a", "ref_dim_b", "ref_dim_c", "ref_dim_d"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)


# ══════════════════════════════════════════════════════════════
# 분석 1: 보고 주기 이상 탐지
# ══════════════════════════════════════════════════════════════

def _classify_interval(sec: float) -> str:
    """
    수신 간격을 4단계로 분류한다.

    NORMAL   : 300 ≤ sec ≤ 364  (expand=0 이내 정상)
    EXPANDED : 364 < sec ≤ 370  (슬롯 혼잡, expand 발생)
    LATE     : sec > 370        (expand=3 초과 → 이상)
    TOO_EARLY: sec < 300        (5분 미만 재전송 → 이상)
    """
    if sec < MIN_INTERVAL_SEC:
        return "TOO_EARLY"
    if sec <= MAX_INTERVAL_EXPAND0_SEC:
        return "NORMAL"
    if sec <= MAX_INTERVAL_SEC:
        return "EXPANDED"
    return "LATE"


def _analyze_interval(df: pd.DataFrame) -> pd.DataFrame:
    """동일 MMSI의 연속 Type 5 수신 간격을 검증한다."""
    if df.empty:
        return df

    df = df.copy()
    df["received_at"] = pd.to_datetime(df["received_at"], utc=True, errors="coerce")
    df = df.sort_values(["mmsi", "received_at"])

    df["prev_received_at"] = df.groupby("mmsi")["received_at"].shift(1)
    df_pairs = df.dropna(subset=["prev_received_at"]).copy()

    df_pairs["interval_sec"] = (
        df_pairs["received_at"] - df_pairs["prev_received_at"]
    ).dt.total_seconds()

    df_pairs["interval_status"] = df_pairs["interval_sec"].apply(_classify_interval)
    return df_pairs


# ══════════════════════════════════════════════════════════════
# 분석 2: 선박 재원 유효성 검증
# ══════════════════════════════════════════════════════════════

def _normalize_callsign(cs: Optional[str]) -> str:
    """콜사인 정규화: 대문자 변환, @ 및 공백 제거."""
    if not cs:
        return ""
    return cs.upper().replace("@", "").strip()


def _normalize_imo(imo: Optional[str]) -> str:
    """IMO 번호 정규화: 숫자만 추출, 선행 0 제거."""
    if not imo:
        return ""
    digits = "".join(c for c in str(imo) if c.isdigit())
    return str(int(digits)) if digits else ""


def _name_similarity(a: Optional[str], b: Optional[str]) -> float:
    """
    두 선박명의 유사도를 0~1로 반환한다.

    AIS는 최대 20자이므로 vessel_info 이름이 더 길 수 있다.
    한쪽이 다른 쪽의 접두어면 완전 일치(1.0)로 처리한다.
    """
    if not a or not b:
        return 0.0
    a_clean = a.upper().strip().replace("@", "")
    b_clean = b.upper().strip().replace("@", "")
    if b_clean.startswith(a_clean) or a_clean.startswith(b_clean):
        return 1.0
    return SequenceMatcher(None, a_clean, b_clean).ratio()


def _classify_vessel_match(row: pd.Series) -> str:
    """vessel_info와의 비교 결과를 분류한다. 여러 이상이면 '|'로 연결."""
    if not row.get("ref_mmsi_found", False):
        return "NOT_FOUND"

    issues: list[str] = []

    imo_ais = _normalize_imo(row.get("imo_number"))
    imo_ref = _normalize_imo(row.get("ref_imo_number"))
    if imo_ais and imo_ref and imo_ais != imo_ref:
        issues.append("IMO_MISMATCH")

    stype_ais = row.get("ship_type")
    stype_ref = row.get("ref_ship_type")
    if stype_ais is not None and stype_ref is not None and int(stype_ais) != int(stype_ref):
        issues.append("SHIPTYPE_MISMATCH")

    cs_ais = _normalize_callsign(row.get("call_sign"))
    cs_ref = _normalize_callsign(row.get("ref_call_sign"))
    if cs_ais and cs_ref and cs_ais != cs_ref:
        issues.append("CALLSIGN_MISMATCH")

    sim = _name_similarity(row.get("vessel_name"), row.get("ref_vessel_name"))
    if sim < NAME_SIMILARITY_THRESHOLD:
        issues.append(f"NAME_MISMATCH(sim={sim:.2f})")
    elif sim < 1.0:
        issues.append(f"NAME_SIMILAR(sim={sim:.2f})")

    return "|".join(issues) if issues else "OK"


def _analyze_vessel_spec(
    df_static: pd.DataFrame,
    df_vessel_info: pd.DataFrame,
) -> pd.DataFrame:
    """각 MMSI의 최신 Type 5 레코드를 vessel_info 참조 테이블과 비교한다."""
    if df_static.empty or df_vessel_info.empty:
        return pd.DataFrame()

    df_latest = (
        df_static.sort_values("received_at")
        .groupby("mmsi")
        .last()
        .reset_index()
    )

    df_merged = df_latest.merge(df_vessel_info, on="mmsi", how="left")
    df_merged["ref_mmsi_found"] = (
        df_merged["ref_call_sign"].notna() | df_merged["ref_vessel_name"].notna()
    )
    df_merged["match_result"] = df_merged.apply(_classify_vessel_match, axis=1)
    return df_merged


# ══════════════════════════════════════════════════════════════
# 메인 실행 함수
# ══════════════════════════════════════════════════════════════

def run(start_dt: datetime, end_dt: datetime) -> None:
    """AIS Type 5 무결성 분석을 수행하고 결과를 로그로 기록한다."""
    logger.info(
        "[Cassandra] 연결: %s:%s / keyspace=%s",
        CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE,
    )

    cluster, session = _connect()
    try:
        df_static      = _load_static_voyage(session, start_dt, end_dt)
        df_vessel_info = _load_vessel_info(session)
    finally:
        cluster.shutdown()

    if df_static.empty:
        logger.warning("[Type5] 데이터가 없어 분석을 건너뜁니다.")
        return

    logger.info(
        "[Type5] 로드 완료: %s건 / %s척",
        f"{len(df_static):,}", df_static["mmsi"].nunique(),
    )

    # ── 분석 1: 보고 주기 ──────────────────────────────────────
    logger.info("=== [분석 1] Type 5 보고 주기 이상 탐지 ===")
    df_interval = _analyze_interval(df_static)

    # MMSI별 전체 메시지 수 (interval과 별도로 df_static 기준)
    msg_count = df_static.groupby("mmsi").size().rename("msg_count")

    if df_interval.empty:
        logger.info("각 MMSI 수신 건수가 1건이라 주기 비교 불가")
        logger.info(
            "MMSI별 수신 메시지 수 (기대: ~9건/시간):\n%s",
            msg_count.to_frame().to_string(),
        )
    else:
        logger.info(
            "유효 범위: %d초(정상) / ~%d초(혼잡) / >%d초(이상)",
            MAX_INTERVAL_EXPAND0_SEC, MAX_INTERVAL_SEC, MAX_INTERVAL_SEC,
        )

        # 전체 상태 분포 (NORMAL 포함)
        logger.info(
            "보고 주기 분류 결과 (전체 interval 기준):\n%s",
            df_interval["interval_status"].value_counts(dropna=False).to_string(),
        )

        # MMSI별 요약: 메시지 수 + interval 통계 + 상태별 카운트
        status_pivot = (
            df_interval.groupby(["mmsi", "interval_status"])
            .size()
            .unstack(fill_value=0)
        )
        all_statuses = ["NORMAL", "EXPANDED", "LATE", "TOO_EARLY"]
        for col in all_statuses:
            if col not in status_pivot.columns:
                status_pivot[col] = 0

        summary = (
            df_interval.groupby("mmsi")["interval_sec"]
            .agg(
                intervals="count",
                mean_sec=lambda x: round(x.mean(), 1),
                min_sec=lambda x: round(x.min(), 1),
                max_sec=lambda x: round(x.max(), 1),
            )
        )
        summary = summary.join(status_pivot[all_statuses], how="left").fillna(0)
        summary = summary.join(msg_count, how="left")
        summary.insert(0, "msg_count", summary.pop("msg_count"))
        summary[["NORMAL", "EXPANDED", "LATE", "TOO_EARLY"]] = (
            summary[["NORMAL", "EXPANDED", "LATE", "TOO_EARLY"]].astype(int)
        )

        logger.info(
            "MMSI별 보고 주기 요약 (msg_count=수신 메시지 수, intervals=비교 쌍 수):\n%s",
            summary.to_string(),
        )

        # MMSI별 수신 시각 목록 (정상/비정상 구분)
        for mmsi, grp in df_interval.groupby("mmsi"):
            lines = []
            for _, row in grp.iterrows():
                flag = "✓" if row["interval_status"] in ("NORMAL", "EXPANDED") else "✗"
                lines.append(
                    f"  {flag} {row['received_at'].strftime('%H:%M:%S')}"
                    f"  ({row['interval_sec']:.0f}s, {row['interval_status']})"
                )
            # 첫 번째 메시지는 비교 쌍이 없으므로 별도 표시
            first_msg = df_static[df_static["mmsi"] == mmsi]["received_at"].min()
            if hasattr(first_msg, "strftime"):
                lines.insert(0, f"  - {first_msg.strftime('%H:%M:%S')}  (첫 수신)")
            logger.info("MMSI %s 수신 시각 상세 (%d건):\n%s", mmsi, msg_count.get(mmsi, 0), "\n".join(lines))

        df_anomaly = df_interval[
            df_interval["interval_status"].isin(["LATE", "TOO_EARLY"])
        ]
        if not df_anomaly.empty:
            logger.warning(
                "이상 보고 주기 %d건:\n%s",
                len(df_anomaly),
                df_anomaly[[
                    "mmsi", "prev_received_at", "received_at",
                    "interval_sec", "interval_status",
                ]].to_string(),
            )
        else:
            logger.info("이상 보고 주기 레코드 없음")

    # ── 분석 2: 선박 재원 ──────────────────────────────────────
    logger.info("=== [분석 2] 선박 재원 유효성 검증 ===")

    if df_vessel_info.empty:
        logger.warning("[vessel_info] 참조 데이터 없음 — 재원 검증 불가")
        return

    df_spec = _analyze_vessel_spec(df_static, df_vessel_info)

    if df_spec.empty:
        logger.warning("재원 검증 결과 없음")
        return

    logger.info(
        "재원 검증 결과 분류:\n%s",
        df_spec["match_result"].value_counts().to_string(),
    )

    df_issues = df_spec[df_spec["match_result"] != "OK"]
    if not df_issues.empty:
        DETAIL_COLS = [
            "mmsi", "vessel_name", "ref_vessel_name",
            "call_sign", "ref_call_sign",
            "imo_number", "ref_imo_number",
            "ship_type", "ref_ship_type",
            "match_result",
        ]
        logger.warning(
            "재원 불일치 %d건:\n%s",
            len(df_issues),
            df_issues[DETAIL_COLS].to_string(),
        )
    else:
        logger.info("모든 선박 재원 검증 통과")
