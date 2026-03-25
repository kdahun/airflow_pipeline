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
from cassandra.query import PreparedStatement

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
_stmt:    PreparedStatement | None = None


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
    PreparedStatement를 함께 관리해 INSERT 성능을 최적화합니다.
    """
    global _cluster, _session, _stmt

    if _session is None:
        _cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        _session = _cluster.connect(CASSANDRA_KEYSPACE)

        _stmt = _session.prepare(f"""
            INSERT INTO {CASSANDRA_TABLE}
              (mmsi, date_bucket, received_at, coast_mmsi,
               cog, integrity_flag, latitude, longitude,
               msg_type, nav_status, raim_flag, rot,
               sog, timestamp_sec, true_heading,
               rssi, slot_num, snr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        print(f"  [Cassandra] 연결 완료 → {CASSANDRA_HOST}:{CASSANDRA_PORT} / {CASSANDRA_KEYSPACE}")

    return _session, _stmt


# ══════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════

def save_record(record: dict) -> None:
    """
    AIS JSON 레코드 1건을 ais_class_a_dynamic 테이블에 INSERT합니다.

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
        session, stmt = _get_session()

        payload = record.get("data") or record
        vsi     = record.get("vsi") or {}   # rssi, slot_num, snr 출처

        # mmsi: text
        mmsi_raw = _to_int(payload.get("mmsi") or payload.get("userId"))
        mmsi     = str(mmsi_raw) if mmsi_raw is not None else None

        # dataBucket 문자열 → datetime 파싱 (공통)
        # JSON 예시: "2026-03-20-12:50:21.494"
        db_str = record.get("dataBucket")
        db_dt  = _parse_data_bucket(db_str)

        # date_bucket: timestamp 타입 — dataBucket 문자열의 KST 시각을 그대로 저장
        # timezone 정보 없는 naive datetime → Cassandra가 변환 없이 값 그대로 저장
        # 예) "2026-03-20-11:59:24" → 저장: 2026-03-20 11:59:24 (KST 시각 그대로)
        date_bucket = db_dt  # naive datetime, timezone 변환 없음

        # received_at: Python이 처리한 현재 시각 (Windows 로컬 = KST) → 그대로 저장
        received_at = datetime.now()

        # msg_type: outer messageId 우선, 없으면 data.messageId
        msg_type = _to_int(record.get("messageId") or payload.get("messageId"))

        cog          = _to_float(payload.get("cog"))
        integrity    = _to_int(payload.get("positionAccuracy"))

        # AIS 위도/경도: 1/10000 분 단위 → 도(degree)
        raw_lat = _to_float(payload.get("latitude"))
        raw_lon = _to_float(payload.get("longitude"))
        lat = round(raw_lat / 600000, 6) if raw_lat is not None else None
        lon = round(raw_lon / 600000, 6) if raw_lon is not None else None

        # navigationalStatus(실제 JSON 키) / navigationStatus(구버전 호환)
        nav_status   = _to_int(
            payload.get("navigationalStatus") or payload.get("navigationStatus")
        )
        raim_flag    = bool(payload.get("raimFlag")) if payload.get("raimFlag") is not None else None
        rot          = _to_float(payload.get("rateOfTurn"))
        sog          = _to_float(payload.get("sog") or payload.get("speedOverGround"))
        timestamp_sec = _to_int(payload.get("timeStamp") or payload.get("timestamp_sec"))
        true_heading  = _to_int(payload.get("trueHeading"))

        # vsi 객체에서 추출 (최상위 레벨)
        rssi     = _to_float(vsi.get("rssi"))
        slot_num = _to_int(vsi.get("slotNum"))
        snr      = _to_float(vsi.get("snr"))

        session.execute(stmt, (
            mmsi,           # mmsi             text
            date_bucket,    # date_bucket       timestamp
            received_at,    # received_at       timestamp
            None,           # coast_mmsi        text  (미사용)
            cog,            # cog               float
            integrity,      # integrity_flag    int
            lat,            # latitude          double
            lon,            # longitude         double
            msg_type,       # msg_type          int
            nav_status,     # nav_status        int
            raim_flag,      # raim_flag         boolean
            rot,            # rot               float
            sog,            # sog               float
            timestamp_sec,  # timestamp_sec     int
            true_heading,   # true_heading      int
            rssi,           # rssi              float
            slot_num,       # slot_num          int
            snr,            # snr               float
        ))

    except Exception as e:
        mmsi_val = (record.get("data") or record).get("mmsi", "?")
        print(f"  [Cassandra 오류] mmsi={mmsi_val} → {e}", file=sys.stderr)


def close() -> None:
    """Cassandra 연결을 명시적으로 닫습니다. 프로그램 종료 전에 반드시 호출하세요."""
    global _cluster, _session, _stmt
    if _session is not None:
        _session.shutdown()
        _session = None
    if _cluster is not None:
        _cluster.shutdown()
        _cluster = None
    _stmt = None
    print("  [Cassandra] 연결 종료")
