"""
Cassandra 연결 및 AIS 레코드 저장 전담 모듈
=============================================
다른 스크립트에서 import 해서 사용합니다.

사용 예시:
  import cassandra_save

  cassandra_save.save_record(record)   # AIS JSON 레코드 1건 저장
  cassandra_save.close()               # 프로그램 종료 전 반드시 호출

저장 대상:
  keyspace : dlim
  table    : ais_class_a_dynamic

JSON 필드 → Cassandra 컬럼 매핑:
  (outer) messageId           → msg_type        int
  (outer) data_bucket         → date_bucket      date
  (outer) data_bucket         → received_at      timestamp
  data.mmsi                   → mmsi             text
  data.cog                    → cog              float
  data.positionAccuracy       → integrity_flag   int
  data.latitude  ÷ 600000     → latitude         double
  data.longitude ÷ 600000     → longitude        double
  data.navigationalStatus     → nav_status       int
  data.raimFlag               → raim_flag        boolean
  data.rateOfTurn             → rot              float
  data.sog                    → sog              float
  data.timeStamp              → timestamp_sec    int
  data.trueHeading            → true_heading     int
  (없음)                      → coast_mmsi       null

의존 패키지:
  pip install cassandra-driver pyasyncore
"""

import os
import sys
from datetime import datetime

from cassandra.cluster import Cluster

# ──────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────
CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT     = 9042
CASSANDRA_KEYSPACE = "dlim"
CASSANDRA_TABLE    = "ais_class_a_dynamic"

# ──────────────────────────────────────────────────────────────
# 싱글턴 — 모듈 전체에서 연결 1회만 생성
# ──────────────────────────────────────────────────────────────
_cluster: object | None = None
_session: object | None = None


# ══════════════════════════════════════════════════════════════
# 내부 헬퍼
# ══════════════════════════════════════════════════════════════

def _to_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_data_bucket(raw: str | None) -> datetime | None:
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


def _get_session() -> tuple:
    """
    Cassandra 세션을 반환합니다. 최초 호출 시에만 연결합니다 (싱글턴).
    """
    global _cluster, _session

    if _session is None:
        _cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        _session = _cluster.connect(CASSANDRA_KEYSPACE)
        print(f"  [Cassandra] 연결 완료 → {CASSANDRA_HOST}:{CASSANDRA_PORT} / {CASSANDRA_KEYSPACE}")

    return _session


# ══════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════

def save_record(record: dict) -> None:
    """
    AIS JSON 레코드 1건을 ais_class_a_dynamic 테이블에 INSERT합니다.
    NULL 값인 컬럼은 INSERT 목록에서 제외해 tombstone 생성을 방지합니다.

    record 구조:
      {
        "messageId": 1.0,
        "dataBucket": "2026-03-20-12:50:21.494",
        "data": {
          "mmsi": 5.38007769E8,
          "cog": 1969.0,
          ...
        }
      }
    """
    try:
        session = _get_session()

        payload = record.get("data") or record
        vsi     = record.get("vsi") or {}

        # mmsi: text
        mmsi_raw = _to_int(payload.get("mmsi") or payload.get("userId"))
        mmsi     = str(mmsi_raw) if mmsi_raw is not None else None

        db_str = record.get("dataBucket")
        db_dt  = _parse_data_bucket(db_str)
        date_bucket = db_dt
        received_at = datetime.now()

        msg_type = _to_int(record.get("messageId") or payload.get("messageId"))

        cog       = _to_float(payload.get("cog"))
        integrity = _to_int(payload.get("positionAccuracy"))

        raw_lat = _to_float(payload.get("latitude"))
        raw_lon = _to_float(payload.get("longitude"))
        lat = round(raw_lat / 600000, 6) if raw_lat is not None else None
        lon = round(raw_lon / 600000, 6) if raw_lon is not None else None

        nav_status = _to_int(
            payload.get("navigationalStatus") or payload.get("navigationStatus")
        )
        raim_flag     = bool(payload.get("raimFlag")) if payload.get("raimFlag") is not None else None
        rot           = _to_float(payload.get("rateOfTurn"))
        sog           = _to_float(payload.get("sog") or payload.get("speedOverGround"))
        timestamp_sec = _to_int(payload.get("timeStamp") or payload.get("timestamp_sec"))
        true_heading  = _to_int(payload.get("trueHeading"))

        rssi     = _to_float(vsi.get("rssi"))
        slot_num = _to_int(vsi.get("slotNum"))
        snr      = _to_float(vsi.get("snr"))

        # NULL 컬럼을 제외한 동적 INSERT — NULL을 넣으면 Cassandra tombstone이 생성됨
        col_val_pairs = [
            ("mmsi",           mmsi),
            ("date_bucket",    date_bucket),
            ("received_at",    received_at),
            ("msg_type",       msg_type),
            ("cog",            cog),
            ("integrity_flag", integrity),
            ("latitude",       lat),
            ("longitude",      lon),
            ("nav_status",     nav_status),
            ("raim_flag",      raim_flag),
            ("rot",            rot),
            ("sog",            sog),
            ("timestamp_sec",  timestamp_sec),
            ("true_heading",   true_heading),
            ("rssi",           rssi),
            ("slot_num",       slot_num),
            ("snr",            snr),
        ]
        non_null = [(col, val) for col, val in col_val_pairs if val is not None]
        if not non_null:
            return

        cols   = ", ".join(col for col, _ in non_null)
        params = [val for _, val in non_null]
        cql    = f"INSERT INTO {CASSANDRA_TABLE} ({cols}) VALUES ({', '.join(['?'] * len(non_null))})"

        stmt = session.prepare(cql)
        session.execute(stmt, params)

    except Exception as e:
        mmsi_val = (record.get("data") or record).get("mmsi", "?")
        print(f"  [Cassandra 오류] mmsi={mmsi_val} → {e}", file=sys.stderr)


def close() -> None:
    """Cassandra 연결을 명시적으로 닫습니다. 프로그램 종료 전에 반드시 호출하세요."""
    global _cluster, _session
    if _session is not None:
        _session.shutdown()
        _session = None
    if _cluster is not None:
        _cluster.shutdown()
        _cluster = None
    print("  [Cassandra] 연결 종료")
