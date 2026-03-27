"""
Spark 기반 AIS 슬롯 검증 분석 모듈
====================================
Cassandra에서 지정 시간 범위의 AIS 데이터를 조회한 뒤
Spark Window 함수로 슬롯 간격(NI) 기반 이상 여부를 검증하고
결과를 로그(stdout)로 기록한다.

실행:
    spark-submit --master spark://spark-master:7077 \
        spark_report_analysis.py <start_iso> <end_iso>
    예) spark_report_analysis.py 2026-03-26T16:40:00 2026-03-26T16:50:00
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE    = "ais_class_a_dynamic"

COLUMNS = [
    "mmsi", "date_bucket", "received_at", "msg_type",
    "sog", "cog", "nav_status", "longitude", "latitude",
    "rssi", "slot_num", "snr",
]


def _load_from_cassandra(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """드라이버에서 cassandra-driver로 시간 범위 조회."""
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


@F.udf(T.IntegerType())
def get_ni(sog_raw):
    """SOG(x10 저장) -> NI(Nominal Increment)."""
    if sog_raw is None:
        return None
    sog = float(sog_raw) / 10.0
    if sog == 0:
        return None
    elif sog < 14:
        return 375
    elif sog < 23:
        return 225
    else:
        return 75


@F.udf(T.IntegerType())
def slot_diff_wrap(prev, curr):
    if prev is None or curr is None:
        return None
    diff = int(curr) - int(prev)
    if diff < 0:
        diff += 2250
    return diff


@F.udf(T.BooleanType())
def is_slot_valid(ni, diff):
    if ni is None or diff is None:
        return None
    margin = 0.2 * ni
    return (ni - margin) <= diff <= (ni + margin)


def run(start_iso: str, end_iso: str) -> None:
    start_dt = datetime.fromisoformat(start_iso)
    end_dt   = datetime.fromisoformat(end_iso)
    print(f"[Report 분석] {start_dt} ~ {end_dt}")

    rows = _load_from_cassandra(start_dt, end_dt)
    if not rows:
        print("[Report] 데이터 없음 — 종료")
        return

    spark = SparkSession.builder.appName("ais_report_analysis").getOrCreate()

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

    w = Window.partitionBy("mmsi").orderBy("date_bucket")

    df_check = (
        df
        .withColumn("prev_slot", F.lag("slot_num").over(w))
        .withColumn("prev_sog",  F.lag("sog").over(w))
        .filter(F.col("prev_slot").isNotNull() & F.col("prev_sog").isNotNull())
        .withColumn("NI", get_ni(F.col("prev_sog")))
        .withColumn("slot_diff", slot_diff_wrap(F.col("prev_slot"), F.col("slot_num")))
        .withColumn("slot_valid", is_slot_valid(F.col("NI"), F.col("slot_diff")))
    )

    # 전체 슬롯 검증 결과
    print("\n=== 전체 슬롯 검증 결과 ===")
    df_check.groupBy("slot_valid").count().show()

    # 선박별 요약
    summary = df_check.groupBy("mmsi").agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("slot_valid") == True, 1).otherwise(0)).alias("valid"),
        F.sum(F.when(F.col("slot_valid") == False, 1).otherwise(0)).alias("invalid"),
        F.sum(F.when(F.col("slot_valid").isNull(), 1).otherwise(0)).alias("sog_zero"),
    )
    summary = summary.withColumn(
        "invalid_rate(%)",
        F.round(F.col("invalid") / F.col("total") * 100, 2),
    )
    print("=== 선박별 요약 ===")
    summary.show(100, truncate=False)

    # 이상 레코드
    df_invalid = df_check.filter(F.col("slot_valid") == False)
    invalid_cnt = df_invalid.count()
    print(f"이상 레코드 수: {invalid_cnt}")
    if invalid_cnt > 0:
        df_invalid.select(
            "mmsi", "date_bucket", "prev_slot", "slot_num",
            "slot_diff", "NI", "prev_sog",
        ).show(200, truncate=False)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_report_analysis.py <start_iso> <end_iso>", file=sys.stderr)
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
