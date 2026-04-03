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
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy

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

# 기지국 위치
BASE_LAT = 35.080532
BASE_LON = 129.077486

# 거리 구간 정의 (km)
DIST_BINS   = [0, 10, 20, 30, 40, float("inf")]
DIST_LABELS = ["0~10 km", "10~20 km", "20~30 km", "30~40 km", "40 km+"]
DIST_COLORS = ["#e74c3c", "#f1c40f", "#2ecc71", "#3498db", "#9b59b6"]


# ──────────────────────────────────────────────────────────────
# 거리 계산
# ──────────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 위경도 좌표 사이의 거리를 km 단위로 반환한다 (Haversine 공식)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _add_distance(df: pd.DataFrame) -> pd.DataFrame:
    """latitude/longitude 컬럼을 이용해 기지국까지의 거리와 구간 컬럼을 추가한다."""
    valid = df["latitude"].notna() & df["longitude"].notna()
    df["distance_km"] = float("nan")
    df.loc[valid, "distance_km"] = df.loc[valid].apply(
        lambda r: _haversine_km(BASE_LAT, BASE_LON, r["latitude"], r["longitude"]),
        axis=1,
    )
    df["dist_band"] = pd.cut(
        df["distance_km"],
        bins=DIST_BINS,
        labels=DIST_LABELS,
        right=False,
    )
    return df


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
# 메인 실행 함수
# ──────────────────────────────────────────────────────────────

def run(start_dt: datetime, end_dt: datetime) -> None:
    """지정 시간 범위의 RSSI/SNR 통계를 로그로 기록하고 차트를 PNG로 저장한다."""
    df = _load_from_cassandra(start_dt=start_dt, end_dt=end_dt)

    if df.empty:
        logger.warning("데이터가 없어 분석을 건너뜁니다.")
        return

    df_sort = df.sort_values("date_bucket").copy()
    df_sort = _add_distance(df_sort)

    # --- RSSI / SNR / 거리 통계 로그 ---
    rssi_stats = df_sort["rssi"].describe()
    snr_stats  = df_sort["snr"].describe()
    dist_stats = df_sort["distance_km"].describe()
    logger.info("=== RSSI 통계 ===\n%s", rssi_stats.to_string())
    logger.info("=== SNR 통계 ===\n%s", snr_stats.to_string())
    logger.info("=== 거리 통계 (km) ===\n%s", dist_stats.to_string())

    band_counts = df_sort["dist_band"].value_counts().sort_index()
    logger.info("=== 거리 구간별 수신 건수 ===\n%s", band_counts.to_string())

    rssi_snr_df = df_sort[["mmsi", "date_bucket", "distance_km", "dist_band", "rssi", "snr"]].copy()
    logger.info("=== RSSI/SNR/거리 DataFrame (상위 50건) ===\n%s", rssi_snr_df.head(50).to_string())

    band_color_map = dict(zip(DIST_LABELS, DIST_COLORS))
    legend_patches = [
        mpatches.Patch(color=c, label=l)
        for c, l in zip(DIST_COLORS, DIST_LABELS)
    ]

    rssi_mean = df_sort["rssi"].mean()
    snr_mean  = df_sort["snr"].mean()

    # MMSI 집계 (바 차트용)
    mmsi_stats = (
        df_sort.groupby("mmsi")
        .agg(
            mean_rssi=("rssi", "mean"),
            mean_dist=("distance_km", "mean"),
            count=("rssi", "count"),
        )
        .sort_values("mean_rssi", ascending=True)
        .reset_index()
    )
    mmsi_stats["dist_band"] = pd.cut(
        mmsi_stats["mean_dist"],
        bins=DIST_BINS,
        labels=DIST_LABELS,
        right=False,
    )
    bar_colors = mmsi_stats["dist_band"].astype(object).map(band_color_map).fillna("#aaaaaa")
    n_mmsi = len(mmsi_stats)

    # ── 레이아웃: GridSpec 5행 2열 ────────────────────────────────────
    # Row 0: RSSI 시계열 (전체 폭)
    # Row 1: SNR  시계열 (전체 폭)
    # Row 2: 거리 산점도 | 거리 Box plot
    # Row 3: MMSI 바 차트 | RSSI 히스토그램
    # Row 4: MMSI별 RSSI 원시 시계열 | MMSI별 SNR 원시 시계열
    mmsi_row_h = max(4, n_mmsi * 0.28)
    fig_h = 9 + 5 + mmsi_row_h + 4      # 시계열 9 + 거리분석 5 + MMSI행 + 원시 시계열 4
    fig = plt.figure(figsize=(20, fig_h))
    fig.suptitle(
        f"AIS RSSI Analysis  |  {start_dt.strftime('%Y-%m-%d %H:%M')} ~ "
        f"{end_dt.strftime('%H:%M')} KST  |  total {len(df_sort):,} records",
        fontsize=15, fontweight="bold", y=1.002,
    )

    gs = fig.add_gridspec(
        5, 2,
        height_ratios=[1, 1, 1.1, mmsi_row_h / 4.5, 1],
        hspace=0.55, wspace=0.32,
    )

    ax_rssi     = fig.add_subplot(gs[0, :])
    ax_snr      = fig.add_subplot(gs[1, :], sharex=ax_rssi)
    ax_scat     = fig.add_subplot(gs[2, 0])
    ax_box      = fig.add_subplot(gs[2, 1])
    ax_bar      = fig.add_subplot(gs[3, 0])
    ax_hist     = fig.add_subplot(gs[3, 1])
    ax_rssi_raw = fig.add_subplot(gs[4, 0])
    ax_snr_raw  = fig.add_subplot(gs[4, 1])

    # ── [1] RSSI 시계열 ──────────────────────────────────────────────
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        mask = df_sort["dist_band"] == band
        if mask.sum() == 0:
            continue
        ts_rssi = (
            df_sort.loc[mask].set_index("date_bucket")["rssi"]
            .resample("30s").mean().dropna()
        )
        ax_rssi.plot(ts_rssi.index, ts_rssi.values,
                     color=color, linewidth=1.8, alpha=0.9, label=band)

    ax_rssi.axhline(rssi_mean, color="black", linestyle="--", linewidth=1,
                    label=f"overall avg: {rssi_mean:.1f} dBm")
    ax_rssi.set_ylabel("RSSI (dBm)")
    ax_rssi.set_title("RSSI time series  (30s avg per distance band)")
    ax_rssi.legend(fontsize=9, loc="upper right")
    ax_rssi.grid(True, alpha=0.3)

    # ── [2] SNR 시계열 ───────────────────────────────────────────────
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        mask = df_sort["dist_band"] == band
        if mask.sum() == 0:
            continue
        ts_snr = (
            df_sort.loc[mask].set_index("date_bucket")["snr"]
            .resample("30s").mean().dropna()
        )
        ax_snr.plot(ts_snr.index, ts_snr.values,
                    color=color, linewidth=1.8, alpha=0.9, label=band)

    ax_snr.axhline(snr_mean, color="black", linestyle="--", linewidth=1,
                   label=f"overall avg: {snr_mean:.1f} dB")
    ax_snr.set_ylabel("SNR (dB)")
    ax_snr.set_xlabel("received time (UTC)")
    ax_snr.set_title("SNR time series  (30s avg per distance band)")
    ax_snr.legend(fontsize=9, loc="upper right")
    ax_snr.grid(True, alpha=0.3)

    # ── [3] 거리 vs RSSI 산점도 ──────────────────────────────────────
    valid_dist = df_sort["distance_km"].notna()
    sc = ax_scat.scatter(
        df_sort.loc[valid_dist, "distance_km"],
        df_sort.loc[valid_dist, "rssi"],
        c=df_sort.loc[valid_dist, "distance_km"],
        cmap="RdYlBu_r", s=8, alpha=0.6, linewidths=0,
    )
    fig.colorbar(sc, ax=ax_scat, label="distance (km)", pad=0.01)
    ax_scat.set_xlabel("distance from base station (km)")
    ax_scat.set_ylabel("RSSI (dBm)")
    ax_scat.set_title("distance vs RSSI")
    ax_scat.grid(True, alpha=0.3)

    # ── [4] 거리 구간별 Box plot ─────────────────────────────────────
    band_data = [
        df_sort.loc[df_sort["dist_band"] == lbl, "rssi"].dropna().values
        for lbl in DIST_LABELS
    ]
    bp = ax_box.boxplot(
        band_data, patch_artist=True,
        medianprops=dict(color="black", linewidth=1.5),
    )
    for patch, color in zip(bp["boxes"], DIST_COLORS):
        patch.set_facecolor(color)
        patch.set_alpha(0.75)
    ax_box.set_xticklabels(DIST_LABELS, rotation=15, ha="right")
    ax_box.set_xlabel("distance band")
    ax_box.set_ylabel("RSSI (dBm)")
    ax_box.set_title("RSSI box plot per distance band")
    ax_box.grid(True, alpha=0.3, axis="y")
    for i, data in enumerate(band_data):
        if len(data) > 0:
            ax_box.text(i + 1, ax_box.get_ylim()[0],
                        f"n={len(data)}", ha="center", va="bottom", fontsize=8, color="gray")

    # ── [5] MMSI별 평균 RSSI 바 차트 ─────────────────────────────────
    ax_bar.barh(
        mmsi_stats["mmsi"].astype(str),
        mmsi_stats["mean_rssi"],
        color=bar_colors, alpha=0.85, edgecolor="white", linewidth=0.4,
    )
    ax_bar.set_xlim(left=mmsi_stats["mean_rssi"].min() - 2)
    for _, row in mmsi_stats.iterrows():
        ax_bar.text(
            row["mean_rssi"] + 0.3, str(row["mmsi"]),
            f"{row['mean_dist']:.1f}km (n={int(row['count'])})",
            va="center", fontsize=6.5, color="#333333",
        )
    ax_bar.legend(handles=legend_patches, fontsize=8, loc="lower right")
    ax_bar.set_xlabel("mean RSSI (dBm)")
    ax_bar.set_ylabel("MMSI")
    ax_bar.set_title("mean RSSI per vessel  (color=distance band)")
    ax_bar.grid(True, alpha=0.3, axis="x")
    ax_bar.tick_params(axis="y", labelsize=7)

    # ── [6] RSSI 히스토그램 ──────────────────────────────────────────
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        data = df_sort.loc[df_sort["dist_band"] == band, "rssi"].dropna()
        if len(data) == 0:
            continue
        ax_hist.hist(data, bins=25, color=color, alpha=0.6,
                     label=f"{band} (n={len(data)})",
                     edgecolor="white", linewidth=0.4)
    ax_hist.axvline(rssi_mean, color="black", linestyle="--", linewidth=1,
                    label=f"overall avg: {rssi_mean:.1f} dBm")
    ax_hist.set_xlabel("RSSI (dBm)")
    ax_hist.set_ylabel("count")
    ax_hist.set_title("RSSI histogram by distance band")
    ax_hist.legend(fontsize=8)
    ax_hist.grid(True, alpha=0.3, axis="y")

    # ── [7] 전체 RSSI 원시 시계열 (시간순 단일 선) ──────────────────
    raw_sorted = df_sort.sort_values("date_bucket")
    ax_rssi_raw.plot(
        raw_sorted["date_bucket"], raw_sorted["rssi"],
        color="#3498db", linewidth=1.0, alpha=0.8,
        marker="o", markersize=2,
    )
    ax_rssi_raw.axhline(rssi_mean, color="black", linestyle="--", linewidth=1,
                        label=f"overall avg: {rssi_mean:.1f} dBm")
    ax_rssi_raw.set_xlabel("received time (UTC)")
    ax_rssi_raw.set_ylabel("RSSI (dBm)")
    ax_rssi_raw.set_title("RSSI time series (raw, all vessels)")
    ax_rssi_raw.legend(fontsize=8, loc="upper right")
    ax_rssi_raw.grid(True, alpha=0.3)
    ax_rssi_raw.tick_params(axis="x", rotation=30)

    # ── [8] 전체 SNR 원시 시계열 (시간순 단일 선) ───────────────────
    ax_snr_raw.plot(
        raw_sorted["date_bucket"], raw_sorted["snr"],
        color="#e67e22", linewidth=1.0, alpha=0.8,
        marker="o", markersize=2,
    )
    ax_snr_raw.axhline(snr_mean, color="black", linestyle="--", linewidth=1,
                       label=f"overall avg: {snr_mean:.1f} dB")
    ax_snr_raw.set_xlabel("received time (UTC)")
    ax_snr_raw.set_ylabel("SNR (dB)")
    ax_snr_raw.set_title("SNR time series (raw, all vessels)")
    ax_snr_raw.legend(fontsize=8, loc="upper right")
    ax_snr_raw.grid(True, alpha=0.3)
    ax_snr_raw.tick_params(axis="x", rotation=30)

    # ── PNG 저장 (파일 1개) ──────────────────────────────────────────
    chart_dir = Path(CHART_DIR)
    chart_dir.mkdir(parents=True, exist_ok=True)

    ts = start_dt.strftime("%Y%m%d_%H%M")
    chart_path = chart_dir / f"rssi_{ts}.png"

    fig.savefig(str(chart_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    logger.info("차트 저장 완료: %s", chart_path)
