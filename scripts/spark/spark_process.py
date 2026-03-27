"""
Spark 기반 AIS 처리 모듈
========================
HDFS에서 AIS JSON 파일을 읽어 필드를 파싱·변환한 뒤
Cassandra에 저장하고 MMSI별 집계 결과를 출력한다.

실행:
    spark-submit --master spark://spark-master:7077 spark_process.py <hdfs_path>
    예) spark_process.py /datalake/ais/2026/03/26/16/40
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE    = "ais_class_a_dynamic"
HDFS_NAMENODE      = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ais_process")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )


def _save_to_cassandra(rows: list[dict]) -> tuple[int, int]:
    """드라이버에서 cassandra-driver로 일괄 INSERT (워커 네트워크 제약 회피)."""
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    stmt = session.prepare(f"""
        INSERT INTO {CASSANDRA_TABLE}
          (mmsi, date_bucket, received_at, coast_mmsi,
           cog, integrity_flag, latitude, longitude,
           msg_type, nav_status, raim_flag, rot,
           sog, timestamp_sec, true_heading,
           rssi, slot_num, snr)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    saved = 0
    failed = 0
    now = datetime.now()

    for r in rows:
        try:
            session.execute(stmt, (
                r["mmsi"],
                r["date_bucket"],
                now,
                None,
                r["cog"],
                r["integrity_flag"],
                r["latitude"],
                r["longitude"],
                r["msg_type"],
                r["nav_status"],
                r["raim_flag"],
                r["rot"],
                r["sog"],
                r["timestamp_sec"],
                r["true_heading"],
                r["rssi"],
                r["slot_num"],
                r["snr"],
            ))
            saved += 1
        except Exception:
            failed += 1

    cluster.shutdown()
    return saved, failed


def _parse_data_bucket_udf(raw: str):
    """dataBucket 문자열 → timestamp."""
    if not raw:
        return None
    from datetime import datetime as dt
    for fmt in ("%Y-%m-%d-%H:%M:%S.%f", "%Y-%m-%d-%H:%M:%S"):
        try:
            return dt.strptime(raw, fmt)
        except ValueError:
            continue
    return None

parse_bucket = F.udf(_parse_data_bucket_udf, T.TimestampType())


def run(hdfs_path: str) -> None:
    spark = _build_spark()

    full_path = f"{HDFS_NAMENODE}{hdfs_path}"
    print(f"[Spark] HDFS 읽기: {full_path}")

    raw_df = spark.read.json(full_path)

    if raw_df.rdd.isEmpty():
        print("[Spark] 데이터 없음 — 종료")
        spark.stop()
        return

    # JSON 구조: { messageId, dataBucket, data: { mmsi, cog, ... }, vsi: { rssi, slotNum, snr } }
    parsed = raw_df.select(
        F.col("data.mmsi").cast("double").cast("long").cast("string").alias("mmsi"),
        parse_bucket(F.col("dataBucket")).alias("date_bucket"),
        (F.coalesce(F.col("messageId"), F.col("data.messageId")).cast("int")).alias("msg_type"),
        F.col("data.cog").cast("float").alias("cog"),
        F.col("data.positionAccuracy").cast("int").alias("integrity_flag"),
        (F.col("data.latitude").cast("double") / 600000).alias("latitude"),
        (F.col("data.longitude").cast("double") / 600000).alias("longitude"),
        F.coalesce(
            F.col("data.navigationalStatus"), F.col("data.navigationStatus")
        ).cast("int").alias("nav_status"),
        F.col("data.raimFlag").cast("boolean").alias("raim_flag"),
        F.col("data.rateOfTurn").cast("float").alias("rot"),
        F.coalesce(F.col("data.sog"), F.col("data.speedOverGround")).cast("float").alias("sog"),
        F.coalesce(F.col("data.timeStamp"), F.col("data.timestamp_sec")).cast("int").alias("timestamp_sec"),
        F.col("data.trueHeading").cast("int").alias("true_heading"),
        F.col("vsi.rssi").cast("float").alias("rssi"),
        F.col("vsi.slotNum").cast("int").alias("slot_num"),
        F.col("vsi.snr").cast("float").alias("snr"),
    ).filter(F.col("mmsi").isNotNull())

    total_count = parsed.count()
    print(f"[Spark] 파싱 완료: {total_count:,}건")

    # Cassandra 저장 (드라이버에서 실행)
    rows = [r.asDict() for r in parsed.collect()]
    saved, failed = _save_to_cassandra(rows)
    print(f"[Cassandra] 저장 완료 {saved:,}건 / 실패 {failed:,}건")

    # MMSI별 집계
    summary = (
        parsed
        .groupBy("mmsi")
        .agg(
            F.min("date_bucket").alias("startTime"),
            F.max("date_bucket").alias("endTime"),
            F.count("*").alias("count"),
        )
        .orderBy(F.desc("count"))
    )

    print(f"\n[결과] 총 {summary.count()}척 / {total_count:,}건")
    summary.show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark_process.py <hdfs_path>", file=sys.stderr)
        sys.exit(1)
    run(sys.argv[1])
