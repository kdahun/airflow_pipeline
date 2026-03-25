"""
HDFS(WebHDFS) 읽기 전담 모듈
==============================
다른 스크립트에서 import 해서 사용합니다.

사용 예시:
  from hdfs_reader import stream_records, check_connection

  if not check_connection():
      sys.exit(1)

  for record in stream_records("/datalake/ais/2026/03/20/12"):
      process(record)

Spark 전환 시 이 모듈만 교체하면 됩니다.
  rdd = sc.textFile("hdfs:///datalake/ais/...").map(json.loads)

의존 패키지:
  pip install requests
"""

import json
import os
import sys
from collections.abc import Generator

import requests

# ──────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────
WEBHDFS_HOST = os.getenv("WEBHDFS_HOST", "http://localhost:9870")


# ══════════════════════════════════════════════════════════════
# 내부 HTTP 헬퍼
# ══════════════════════════════════════════════════════════════

def _list(path: str) -> list[dict]:
    """WebHDFS LISTSTATUS: 경로 내 항목 목록 반환."""
    url = f"{WEBHDFS_HOST}/webhdfs/v1{path}?op=LISTSTATUS"
    res = requests.get(url, timeout=10)
    res.raise_for_status()
    return res.json()["FileStatuses"]["FileStatus"]


def _open(path: str) -> str:
    """WebHDFS OPEN: 파일 전체 내용을 문자열로 반환. NameNode → DataNode 자동 redirect."""
    url = f"{WEBHDFS_HOST}/webhdfs/v1{path}?op=OPEN"
    res = requests.get(url, timeout=30, allow_redirects=True)
    res.raise_for_status()
    return res.text


# ══════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════

def check_connection() -> bool:
    """
    WebHDFS NameNode 연결 가능 여부를 확인합니다.
    성공 시 True, 실패 시 False를 반환하고 에러 메시지를 출력합니다.
    """
    try:
        requests.get(
            f"{WEBHDFS_HOST}/webhdfs/v1/?op=LISTSTATUS", timeout=5
        ).raise_for_status()
        print(f"  [HDFS] 연결 성공 ({WEBHDFS_HOST})")
        return True
    except Exception as e:
        print(f"  [HDFS] 연결 실패: {e}", file=sys.stderr)
        print("  → HDFS NameNode가 실행 중인지 확인하세요 (docker ps | grep namenode)", file=sys.stderr)
        return False


def list_json_files(path: str) -> Generator[str, None, None]:
    """
    path 아래 JSON 파일 경로를 하나씩 yield합니다.
    하위 디렉터리는 재귀 탐색합니다.
    """
    try:
        items = _list(path)
    except Exception as e:
        print(f"  [HDFS 경고] 탐색 실패: {path} → {e}", file=sys.stderr)
        return

    for item in sorted(items, key=lambda x: x["pathSuffix"]):
        name      = item["pathSuffix"]
        full_path = f"{path}/{name}"
        if item["type"] == "DIRECTORY":
            yield from list_json_files(full_path)
        elif item["type"] == "FILE" and name.endswith(".json"):
            yield full_path


def stream_lines(file_path: str) -> Generator[dict, None, None]:
    """
    JSON 파일 하나를 열어 \\n 구분 레코드를 dict로 하나씩 yield합니다.
    빈 줄이나 JSON 파싱 실패 줄은 경고 후 스킵합니다.
    """
    try:
        content = _open(file_path)
    except Exception as e:
        print(f"  [HDFS 경고] 파일 읽기 실패: {file_path} → {e}", file=sys.stderr)
        return

    normalized = content.replace("\\n", "\n")

    for lineno, raw in enumerate(normalized.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, list):
                yield from obj
            elif isinstance(obj, dict):
                yield obj
        except json.JSONDecodeError as e:
            print(
                f"  [HDFS 경고] JSON 파싱 실패 ({file_path}:{lineno}) → {e}",
                file=sys.stderr,
            )


def stream_records(base_path: str) -> Generator[dict, None, None]:
    """
    base_path 아래 모든 JSON 파일을 순회하며 레코드를 하나씩 yield합니다.
    전체 데이터를 메모리에 올리지 않습니다 (Generator).

    Spark 전환 예시:
        rdd = sc.textFile("hdfs:///datalake/ais/...").map(json.loads)
    """
    for file_path in list_json_files(base_path):
        print(f"  [HDFS 읽기] {file_path}")
        yield from stream_lines(file_path)
