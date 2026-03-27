"""
Spark 기반 RSSI/SNR 분석 모듈
===============================
Cassandra에서 지정 시간 범위의 AIS 데이터를 조회한 뒤
Spark UDF로 기지국 거리 계산 + 집계를 수행하고,
드라이버에서 matplotlib으로 6-panel 차트를 PNG로 저장한다.

실행:
    spark-submit --master spark://spark-master:7077 \
        spark_rssi_analysis.py <start_iso> <end_iso>
    예) spark_rssi_analysis.py 2026-03-26T16:40:00 2026-03-26T16:50:00
"""
from __future__ import annotations

import math
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE    = "ais_class_a_dynamic"

CHART_DIR = os.getenv("RSSI_CHART_DIR", "/opt/airflow/logs/rssi")

BASE_LAT = 35.080532
BASE_LON = 129.077486

DIST_BINS   = [0, 5, 10, 20, 40, float("inf")]
DIST_LABELS = ["0~5 km", "5~10 km", "10~20 km", "20~40 km", "40 km+"]
DIST_COLORS = ["#e74c3c", "#e67e22", "#f1c40f", "#2ecc71", "#3498db"]

COLUMNS = [
    "mmsi", "date_bucket", "received_at", "msg_type",
    "sog", "cog", "nav_status", "longitude", "latitude",
    "rssi", "slot_num", "snr",
]


def _load_from_cassandra(start_dt: datetime, end_dt: datetime) -> list[dict]:
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    cluster = Cluster(
        contact_points=[CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(),
        protocol_version=4,
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)

    cols = ", ".join(COLUMNS)
    cql = (
        f"SELECT {cols} FROM {CASSANDRA_TABLE} "
        f"WHERE date_bucket >= ? AND date_bucket <= ? "
        f"LIMIT 100000 ALLOW FILTERING"
    )
    stmt = session.prepare(cql)
    stmt.fetch_size = 1000
    result = session.execute(stmt, [start_dt, end_dt])

    rows = [{c: getattr(r, c) for c in COLUMNS} for r in result]
    cluster.shutdown()
    return rows


@F.udf(T.DoubleType())
def haversine_km(lat, lon):
    """기지국과의 거리를 km 단위로 반환."""
    if lat is None or lon is None:
        return None
    R = 6371.0
    dlat = math.radians(lat - BASE_LAT)
    dlon = math.radians(lon - BASE_LON)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(BASE_LAT)) * math.cos(math.radians(lat)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


@F.udf(T.StringType())
def dist_band_udf(km):
    """거리(km) -> 구간 라벨."""
    if km is None:
        return None
    for i, (lo, hi) in enumerate(zip(DIST_BINS, DIST_BINS[1:])):
        if lo <= km < hi:
            return DIST_LABELS[i]
    return DIST_LABELS[-1]


def _render_chart(pdf, start_dt: datetime, end_dt: datetime) -> str:
    """pandas DataFrame을 받아 6-panel 차트를 PNG로 저장하고 경로를 반환한다."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import pandas as pd

    df_sort = pdf.sort_values("date_bucket").copy()
    band_color_map = dict(zip(DIST_LABELS, DIST_COLORS))
    legend_patches = [
        mpatches.Patch(color=c, label=l)
        for c, l in zip(DIST_COLORS, DIST_LABELS)
    ]

    rssi_mean = df_sort["rssi"].mean()
    snr_mean  = df_sort["snr"].mean()

    mmsi_stats = (
        df_sort.groupby("mmsi")
        .agg(mean_rssi=("rssi", "mean"), mean_dist=("distance_km", "mean"), count=("rssi", "count"))
        .sort_values("mean_rssi", ascending=True)
        .reset_index()
    )
    mmsi_stats["dist_band"] = pd.cut(
        mmsi_stats["mean_dist"], bins=DIST_BINS, labels=DIST_LABELS, right=False,
    )
    bar_colors = mmsi_stats["dist_band"].astype(object).map(band_color_map).fillna("#aaaaaa")
    n_mmsi = len(mmsi_stats)

    mmsi_row_h = max(4, n_mmsi * 0.28)
    fig_h = 9 + 5 + mmsi_row_h
    fig = plt.figure(figsize=(20, fig_h))
    fig.suptitle(
        f"AIS RSSI Analysis  |  {start_dt.strftime('%Y-%m-%d %H:%M')} ~ "
        f"{end_dt.strftime('%H:%M')} KST  |  total {len(df_sort):,} records",
        fontsize=15, fontweight="bold", y=1.002,
    )

    gs = fig.add_gridspec(
        4, 2, height_ratios=[1, 1, 1.1, mmsi_row_h / 4.5],
        hspace=0.55, wspace=0.32,
    )

    ax_rssi = fig.add_subplot(gs[0, :])
    ax_snr  = fig.add_subplot(gs[1, :], sharex=ax_rssi)
    ax_scat = fig.add_subplot(gs[2, 0])
    ax_box  = fig.add_subplot(gs[2, 1])
    ax_bar  = fig.add_subplot(gs[3, 0])
    ax_hist = fig.add_subplot(gs[3, 1])

    # [1] RSSI time series
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        mask = df_sort["dist_band"] == band
        if mask.sum() == 0:
            continue
        ts = df_sort.loc[mask].set_index("date_bucket")["rssi"].resample("30s").mean().dropna()
        ax_rssi.plot(ts.index, ts.values, color=color, linewidth=1.8, alpha=0.9, label=band)

    ax_rssi.axhline(rssi_mean, color="black", linestyle="--", linewidth=1,
                    label=f"overall avg: {rssi_mean:.1f} dBm")
    ax_rssi.set_ylabel("RSSI (dBm)")
    ax_rssi.set_title("RSSI time series  (30s avg per distance band)")
    ax_rssi.legend(fontsize=9, loc="upper right")
    ax_rssi.grid(True, alpha=0.3)

    # [2] SNR time series
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        mask = df_sort["dist_band"] == band
        if mask.sum() == 0:
            continue
        ts = df_sort.loc[mask].set_index("date_bucket")["snr"].resample("30s").mean().dropna()
        ax_snr.plot(ts.index, ts.values, color=color, linewidth=1.8, alpha=0.9, label=band)

    ax_snr.axhline(snr_mean, color="black", linestyle="--", linewidth=1,
                   label=f"overall avg: {snr_mean:.1f} dB")
    ax_snr.set_ylabel("SNR (dB)")
    ax_snr.set_xlabel("received time (UTC)")
    ax_snr.set_title("SNR time series  (30s avg per distance band)")
    ax_snr.legend(fontsize=9, loc="upper right")
    ax_snr.grid(True, alpha=0.3)

    # [3] distance vs RSSI scatter
    valid = df_sort["distance_km"].notna()
    sc = ax_scat.scatter(
        df_sort.loc[valid, "distance_km"], df_sort.loc[valid, "rssi"],
        c=df_sort.loc[valid, "distance_km"], cmap="RdYlBu_r", s=8, alpha=0.6, linewidths=0,
    )
    fig.colorbar(sc, ax=ax_scat, label="distance (km)", pad=0.01)
    ax_scat.set_xlabel("distance from base station (km)")
    ax_scat.set_ylabel("RSSI (dBm)")
    ax_scat.set_title("distance vs RSSI")
    ax_scat.grid(True, alpha=0.3)

    # [4] Box plot
    band_data = [
        df_sort.loc[df_sort["dist_band"] == lbl, "rssi"].dropna().values
        for lbl in DIST_LABELS
    ]
    bp = ax_box.boxplot(band_data, patch_artist=True, medianprops=dict(color="black", linewidth=1.5))
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

    # [5] MMSI bar chart
    ax_bar.barh(
        mmsi_stats["mmsi"].astype(str), mmsi_stats["mean_rssi"],
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

    # [6] Histogram
    for band, color in zip(DIST_LABELS, DIST_COLORS):
        data = df_sort.loc[df_sort["dist_band"] == band, "rssi"].dropna()
        if len(data) == 0:
            continue
        ax_hist.hist(data, bins=25, color=color, alpha=0.6,
                     label=f"{band} (n={len(data)})", edgecolor="white", linewidth=0.4)
    ax_hist.axvline(rssi_mean, color="black", linestyle="--", linewidth=1,
                    label=f"overall avg: {rssi_mean:.1f} dBm")
    ax_hist.set_xlabel("RSSI (dBm)")
    ax_hist.set_ylabel("count")
    ax_hist.set_title("RSSI histogram by distance band")
    ax_hist.legend(fontsize=8)
    ax_hist.grid(True, alpha=0.3, axis="y")

    chart_dir = Path(CHART_DIR)
    chart_dir.mkdir(parents=True, exist_ok=True)
    ts_str = start_dt.strftime("%Y%m%d_%H%M")
    chart_path = chart_dir / f"rssi_{ts_str}.png"
    fig.savefig(str(chart_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    return str(chart_path)


def run(start_iso: str, end_iso: str) -> None:
    start_dt = datetime.fromisoformat(start_iso)
    end_dt   = datetime.fromisoformat(end_iso)
    print(f"[RSSI 분석] {start_dt} ~ {end_dt}")

    rows = _load_from_cassandra(start_dt, end_dt)
    if not rows:
        print("[RSSI] 데이터 없음 — 종료")
        return

    spark = SparkSession.builder.appName("ais_rssi_analysis").getOrCreate()

    schema = T.StructType([
        T.StructField("mmsi",        T.StringType()),
        T.StructField("date_bucket", T.TimestampType()),
        T.StructField("received_at", T.TimestampType()),
        T.StructField("msg_type",    T.IntegerType()),
        T.StructField("sog",         T.FloatType()),
        T.StructField("cog",         T.FloatType()),
        T.StructField("nav_status",  T.IntegerType()),
        T.StructField("longitude",   T.DoubleType()),
        T.StructField("latitude",    T.DoubleType()),
        T.StructField("rssi",        T.FloatType()),
        T.StructField("slot_num",    T.IntegerType()),
        T.StructField("snr",         T.FloatType()),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    print(f"[조회] {CASSANDRA_TABLE} -> {df.count():,}건 로드 완료")

    # Spark에서 거리 계산 + 구간 분류
    df = (
        df
        .withColumn("distance_km", haversine_km(F.col("latitude"), F.col("longitude")))
        .withColumn("dist_band", dist_band_udf(F.col("distance_km")))
    )

    # 통계 로그
    print("\n=== RSSI 통계 ===")
    df.select(F.mean("rssi"), F.stddev("rssi"), F.min("rssi"), F.max("rssi")).show()

    print("=== SNR 통계 ===")
    df.select(F.mean("snr"), F.stddev("snr"), F.min("snr"), F.max("snr")).show()

    print("=== 거리 통계 (km) ===")
    df.select(F.mean("distance_km"), F.stddev("distance_km"),
              F.min("distance_km"), F.max("distance_km")).show()

    print("=== 거리 구간별 수신 건수 ===")
    df.groupBy("dist_band").count().orderBy("dist_band").show()

    # pandas 변환 후 차트 생성 (드라이버에서 실행)
    pdf = df.toPandas()
    pdf["date_bucket"] = pdf["date_bucket"].dt.tz_localize("UTC") if pdf["date_bucket"].dt.tz is None else pdf["date_bucket"]

    chart_path = _render_chart(pdf, start_dt, end_dt)
    print(f"차트 저장 완료: {chart_path}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_rssi_analysis.py <start_iso> <end_iso>", file=sys.stderr)
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
