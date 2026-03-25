"""
선박별(MMSI) 수신 현황 요약 DataFrame 출력 + Cassandra 저장
=============================================================
실행:
  python mmsi_summary.py
  python mmsi_summary.py /datalake/ais/2026/03/20/12   # 경로 직접 지정

출력 컬럼:
  mmsi        선박 고유 식별번호
  startTime   최초 수신 시각
  endTime     마지막 수신 시각
  count       수신 건수

Cassandra 저장 여부:
  SAVE_TO_CASSANDRA = True  → 읽으면서 동시에 cassandra_save 모듈로 저장
  SAVE_TO_CASSANDRA = False → DataFrame 출력만 수행

의존 패키지:
  pip install requests pandas cassandra-driver pyasyncore
"""

import sys
from datetime import datetime

import pandas as pd

import cassandra_save
import hdfs_reader

# ──────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────
SAVE_TO_CASSANDRA = True


def get_hdfs_path(dt: datetime) -> str:
    """datetime → HDFS 경로 변환 (분 단위)."""
    return dt.strftime("/datalake/ais/%Y/%m/%d/%H/%M")


# ══════════════════════════════════════════════════════════════
# 집계 로직
# ══════════════════════════════════════════════════════════════

def _parse_data_bucket(raw: str) -> datetime | None:
    """
    data_bucket 문자열 → datetime 변환.
    포맷 예시: "2026-03-20-12:50:21.494"
    """
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d-%H:%M:%S.%f")
    except ValueError:
        try:
            return datetime.strptime(raw, "%Y-%m-%d-%H:%M:%S")
        except ValueError:
            return None


def build_summary(base_path: str, save: bool = False) -> pd.DataFrame:
    """
    HDFS 경로를 읽어 MMSI별 집계 DataFrame을 반환합니다.
    save=True 이면 레코드를 읽는 동시에 cassandra_save.save_record()로 저장합니다.

    반환 컬럼: mmsi, startTime, endTime, count
    """
    agg: dict[str, dict] = {}
    saved_count  = 0
    failed_count = 0

    for record in hdfs_reader.stream_records(base_path):
        # Cassandra 저장 (save 플래그가 True일 때만)
        if save:
            try:
                cassandra_save.save_record(record)
                saved_count += 1
            except Exception:
                failed_count += 1

        # ── MMSI별 집계 ───────────────────────────────────────
        payload  = record.get("data") or record
        raw_mmsi = payload.get("mmsi") or payload.get("userId")
        if raw_mmsi is None:
            continue

        try:
            mmsi = str(int(float(raw_mmsi)))
        except (TypeError, ValueError):
            continue

        # 수신 시각: 최상위 dataBucket 사용 (data.timeStamp는 AIS 슬롯 번호 0~59)
        dt = _parse_data_bucket(record.get("dataBucket"))

        if mmsi not in agg:
            agg[mmsi] = {"min_dt": dt, "max_dt": dt, "count": 0}

        bucket = agg[mmsi]
        bucket["count"] += 1

        if dt is not None:
            if bucket["min_dt"] is None or dt < bucket["min_dt"]:
                bucket["min_dt"] = dt
            if bucket["max_dt"] is None or dt > bucket["max_dt"]:
                bucket["max_dt"] = dt

    if save:
        print(f"  [Cassandra] 저장 완료 {saved_count:,}건 / 실패 {failed_count:,}건")

    rows = [
        {
            "mmsi":      mmsi,
            "startTime": v["min_dt"],
            "endTime":   v["max_dt"],
            "count":     v["count"],
        }
        for mmsi, v in agg.items()
    ]

    df = pd.DataFrame(rows, columns=["mmsi", "startTime", "endTime", "count"])
    df = df.sort_values("count", ascending=False).reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════════
# main
# ══════════════════════════════════════════════════════════════

def main() -> None:
    if len(sys.argv) > 1:
        hdfs_path = sys.argv[1]
    else:
        hdfs_path = get_hdfs_path(datetime.now())

    if not hdfs_reader.check_connection():
        return

    # if SAVE_TO_CASSANDRA:
    #     print(f"\n[2] 데이터 수집 + Cassandra 저장: {hdfs_path}")
    # else:
    #     print(f"\n[2] 데이터 수집 (저장 없음): {hdfs_path}")


    # 파라미터 : HDFS 경로 , 
    df = build_summary(hdfs_path, save=SAVE_TO_CASSANDRA)

    if SAVE_TO_CASSANDRA:
        cassandra_save.close()

    if df.empty:
        print("\n  데이터가 없습니다.")
        return

    print(f"\n[3] 결과 — 총 {len(df):,}척 / {df['count'].sum():,}건\n")

    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", None)
    print(df.to_string(index=True))

    print("\n" + "=" * 60)
    print("  완료!")
    print("=" * 60)


if __name__ == "__main__":
    main()
