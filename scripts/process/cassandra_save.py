"""
Cassandra 연결 및 AIS 레코드 저장 전담 모듈
=============================================
다른 스크립트에서 import 해서 사용합니다.

사용 예시:
  import cassandra_save

  cassandra_save.save_record(record)   # AIS JSON 레코드 1건 저장
  cassandra_save.close()               # 프로그램 종료 전 반드시 호출

저장 대상 (messageId / data.messageId 기준 분기):
  keyspace : dlim
  ais_class_a_dynamic — AIS 타입 1, 3 (위치·동적)
  ais_static_voyage   — AIS 타입 5 (정적·항차)
  signal_information  — AIS 타입 1, 3 수신 시 VSI 신호 정보

ais_class_a_dynamic 매핑 (타입 1, 3):
  (outer) messageId           → msg_type        int
  (outer) dataBucket          → date_bucket      date
  (outer) stationMmsi         → station_mmsi     text
  data.mmsi                   → mmsi             text
  data.cog                    → cog              float
  data.positionAccuracy       → integrity_flag   int
  data.latitude               → latitude         double  (소수점 도 단위 그대로)
  data.longitude              → longitude        double  (소수점 도 단위 그대로)
  data.navigationalStatus     → nav_status       int
  data.raimFlag               → raim_flag        boolean
  data.rateOfTurn             → rot              float
  data.sog                    → sog              float
  data.timeStamp              → timestamp_sec    int
  data.trueHeading            → true_heading     int
  data.communicationState     → sub_message      text (JSON 직렬화)
  received_at                 → now()            timestamp

signal_information 매핑 (타입 1, 3, 5):
  data.mmsi                   → mmsi             text  ← partition key
  (outer) dataBucket          → date_bucket      timestamp  ← partition key
  received_at                 → toa              timestamp
  (outer) stationMmsi         → station_mmsi     text
  "AI"                        → talker_id        text
  "VSI"                       → sentence_id      text
  vsi.slotNum                 → slot_num         int
  vsi.rssi                    → rssi             double
  vsi.snr                     → snr              double

ais_static_voyage 매핑 (타입 5):
  data.mmsi                   → mmsi
  data.callSign               → call_sign
  data.destination            → destination
  data.draught                → draught
  data.imoNumber              → imo_number (text)
  data.shipType               → ship_type
  data.shipName               → vessel_name
  data.dte                    → integrity_flag (있을 때만)
  data.etaMonth/Day/Hour/Min  → eta (dataBucket 연도 기준, 보정)
  (outer) stationMmsi         → station_mmsi
  received_at                 → now()
  msg_type                    → 5

그 외 메시지 타입은 INSERT 하지 않습니다.

의존 패키지:
  pip install cassandra-driver pyasyncore
"""

import json
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
CASSANDRA_TABLE_STATIC = "ais_static_voyage"
CASSANDRA_TABLE_SIGNAL = "signal_information"

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


def _compose_eta_msg5(payload: dict, db_dt: datetime | None) -> datetime | None:
    """
    AIS 타입 5의 etaMonth/Day/Hour/Minute → timestamp.
    연도는 dataBucket 시각이 있으면 그 해를 기준으로 하고,
    ETA가 수신 시각보다 이전이면 다음 해를 시도합니다.
    AIS에서 0·24·60 등은 '미정' 값입니다.
    """
    mo = _to_int(payload.get("etaMonth"))
    d = _to_int(payload.get("etaDay"))
    h = _to_int(payload.get("etaHour"))
    mi = _to_int(payload.get("etaMinute"))
    if mo is None or d is None or h is None or mi is None:
        return None
    if mo <= 0 or d <= 0 or h >= 24 or mi >= 60:
        return None

    base = db_dt or datetime.now()
    base_cmp = base.replace(microsecond=0)

    year = base.year
    for _ in range(2):
        try:
            eta = datetime(year, mo, d, h, mi)
        except ValueError:
            return None
        if eta >= base_cmp:
            return eta
        year += 1
    return None


def _save_signal_information(record: dict, received_at: datetime) -> None:
    """VSI 신호 정보 → signal_information 테이블 INSERT."""
    vsi = record.get("vsi") or {}

    payload = record.get("data") or record
    mmsi_raw = _to_int(payload.get("mmsi") or payload.get("userId"))
    mmsi = str(mmsi_raw) if mmsi_raw is not None else None
    if not mmsi:
        return

    station_mmsi = str(record.get("stationMmsi")) if record.get("stationMmsi") is not None else None

    # date_bucket: HDFS dataBucket 값 사용
    date_bucket = _parse_data_bucket(record.get("dataBucket"))

    rssi     = _to_float(vsi.get("rssi"))
    snr      = _to_float(vsi.get("snr"))
    slot_num = _to_int(vsi.get("slotNum"))

    col_val_pairs = [
        ("mmsi",         mmsi),
        ("date_bucket",  date_bucket),
        ("toa",          received_at),
        ("station_mmsi", station_mmsi),
        ("talker_id",    "AI"),
        ("sentence_id",  "VSI"),
        ("slot_num",     slot_num),
        ("rssi",         rssi),
        ("snr",          snr),
    ]
    non_null = [(col, val) for col, val in col_val_pairs if val is not None]

    cols   = ", ".join(col for col, _ in non_null)
    params = [val for _, val in non_null]
    cql = (
        f"INSERT INTO {CASSANDRA_TABLE_SIGNAL} ({cols}) "
        f"VALUES ({', '.join(['?'] * len(non_null))})"
    )

    session = _get_session()
    stmt = session.prepare(cql)
    session.execute(stmt, params)


def _save_static_voyage(record: dict, payload: dict) -> None:
    """AIS 타입 5 → ais_static_voyage 동적 INSERT."""
    session = _get_session()

    mmsi_raw = _to_int(payload.get("mmsi") or payload.get("userId"))
    mmsi = str(mmsi_raw) if mmsi_raw is not None else None

    db_str = record.get("dataBucket")
    db_dt = _parse_data_bucket(db_str)
    received_at = datetime.now()

    call_sign = payload.get("callSign")
    if call_sign is not None and not isinstance(call_sign, str):
        call_sign = str(call_sign)

    destination = payload.get("destination")
    if destination is not None and not isinstance(destination, str):
        destination = str(destination)

    draught = _to_float(payload.get("draught"))
    eta = _compose_eta_msg5(payload, db_dt)

    imo_number = None
    imo_raw = payload.get("imoNumber")
    if imo_raw is not None:
        try:
            imo_number = str(int(float(imo_raw)))
        except (TypeError, ValueError):
            s = str(imo_raw).strip()
            if s:
                imo_number = s

    integrity_flag = _to_int(payload.get("dte"))
    msg_type = 5
    ship_type = _to_int(payload.get("shipType"))

    vessel_name = payload.get("shipName")
    if vessel_name is not None and not isinstance(vessel_name, str):
        vessel_name = str(vessel_name)

    station_mmsi = str(record.get("stationMmsi")) if record.get("stationMmsi") is not None else None

    col_val_pairs = [
        ("mmsi", mmsi),
        ("received_at", received_at),
        ("msg_type", msg_type),
        ("call_sign", call_sign),
        ("destination", destination),
        ("draught", draught),
        ("eta", eta),
        ("imo_number", imo_number),
        ("integrity_flag", integrity_flag),
        ("ship_type", ship_type),
        ("vessel_name", vessel_name),
        ("station_mmsi", station_mmsi),
    ]
    non_null = [(col, val) for col, val in col_val_pairs if val is not None]
    if not non_null:
        return

    cols = ", ".join(col for col, _ in non_null)
    params = [val for _, val in non_null]
    cql = (
        f"INSERT INTO {CASSANDRA_TABLE_STATIC} ({cols}) "
        f"VALUES ({', '.join(['?'] * len(non_null))})"
    )

    stmt = session.prepare(cql)
    session.execute(stmt, params)

    # VSI 신호 정보를 signal_information 테이블에 별도 저장
    _save_signal_information(record, received_at)


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
    AIS JSON 레코드 1건을 메시지 타입에 따라 INSERT합니다.
    타입 1·3 → ais_class_a_dynamic, 타입 5 → ais_static_voyage, 그 외 → 생략.
    NULL 값인 컬럼은 INSERT 목록에서 제외해 tombstone 생성을 방지합니다.

    record 구조 예:
      {
        "messageId": 1.0,
        "dataBucket": "2026-03-20-12:50:21.494",
        "data": { "messageId": 3, "mmsi": ..., ... }
      }
    """
    try:
        payload = record.get("data") or record
        msg_type = _to_int(record.get("messageId") or payload.get("messageId"))

        if msg_type == 5:
            _save_static_voyage(record, payload)
            return
        if msg_type not in (1, 3):
            return

        session = _get_session()

        # mmsi: text
        mmsi_raw = _to_int(payload.get("mmsi") or payload.get("userId"))
        mmsi = str(mmsi_raw) if mmsi_raw is not None else None

        db_str = record.get("dataBucket")
        db_dt = _parse_data_bucket(db_str)
        date_bucket = db_dt
        received_at = datetime.now()

        cog = _to_float(payload.get("cog"))
        integrity = _to_int(payload.get("positionAccuracy"))

        raw_lat = _to_float(payload.get("latitude"))
        raw_lon = _to_float(payload.get("longitude"))
        lat = round(raw_lat, 6) if raw_lat is not None else None
        lon = round(raw_lon, 6) if raw_lon is not None else None

        nav_status = _to_int(
            payload.get("navigationalStatus") or payload.get("navigationStatus")
        )
        raim_flag = bool(payload.get("raimFlag")) if payload.get("raimFlag") is not None else None
        rot = _to_float(payload.get("rateOfTurn"))
        sog = _to_float(payload.get("sog") or payload.get("speedOverGround"))
        timestamp_sec = _to_int(payload.get("timeStamp") or payload.get("timestamp_sec"))
        true_heading = _to_int(payload.get("trueHeading"))

        # communicationState (오타 키도 함께 시도) → JSON 문자열로 sub_message에 저장
        comm_state_raw = payload.get("communicationState") or payload.get("communcation state")
        sub_message = json.dumps(comm_state_raw, ensure_ascii=False) if comm_state_raw is not None else None

        station_mmsi = str(record.get("stationMmsi")) if record.get("stationMmsi") is not None else None

        # NULL 컬럼을 제외한 동적 INSERT — NULL을 넣으면 Cassandra tombstone이 생성됨
        col_val_pairs = [
            ("mmsi", mmsi),
            ("date_bucket", date_bucket),
            ("received_at", received_at),
            ("msg_type", msg_type),
            ("cog", cog),
            ("integrity_flag", integrity),
            ("latitude", lat),
            ("longitude", lon),
            ("nav_status", nav_status),
            ("raim_flag", raim_flag),
            ("rot", rot),
            ("sog", sog),
            ("timestamp_sec", timestamp_sec),
            ("true_heading", true_heading),
            ("sub_message", sub_message),
            ("station_mmsi", station_mmsi),
        ]
        non_null = [(col, val) for col, val in col_val_pairs if val is not None]
        if not non_null:
            return

        cols = ", ".join(col for col, _ in non_null)
        params = [val for _, val in non_null]
        cql = f"INSERT INTO {CASSANDRA_TABLE} ({cols}) VALUES ({', '.join(['?'] * len(non_null))})"

        stmt = session.prepare(cql)
        session.execute(stmt, params)

        # VSI 신호 정보를 signal_information 테이블에 별도 저장
        _save_signal_information(record, received_at)

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
