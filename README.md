```
airflow-pipeline/                    # 프로젝트 루트
├── docker-compose.yml               # Airflow 컨테이너 구성
├── .env                             # 환경변수 (DB 비밀번호 등)
│
├── dags/                            # ⭐ DAG 파일 위치 (스케줄 정의)
│   └── data_pipeline.py             # 처리 → 분석 파이프라인
│
├── logs/                            # Airflow 실행 로그 (자동 생성)
│   └── scheduler/
│       └── 2024-01-01/
│           └── data_pipeline.py.log
│
├── plugins/                         # 커스텀 플러그인 (당장 안써도 됨)
│
└── scripts/                         # 실제 실행할 파이썬 스크립트
    ├── data_processing.py           # 데이터 처리 코드
    └── data_analysis.py             # 데이터 분석 코드
```

# 1. 커스텀 이미지 빌드 (최초 1회, requirements 변경 시 재실행)

docker compose build

# 2. 외부 네트워크 생성 (없는 경우)

docker network create all4land_net

# 3. DB 초기화 (최초 1회)

docker compose up airflow-init

# 4. 서비스 실행

docker compose up -d airflow-webserver airflow-scheduler

# Airflow 스케줄러 컨테이너 이름 확인

docker ps --format "table {{.Names}}" | grep airflow

# Airflow 컨테이너 안에서 직접 연결 테스트

docker exec -it airflow_pipeline-airflow-scheduler-1 python -c "
from cassandra.cluster import Cluster
try:
c = Cluster(['dlim-cassandra-1'], port=9042)
s = c.connect('dlim')
print('연결 성공')
s.execute('SELECT mmsi FROM ais_class_a_dynamic LIMIT 1')
print('쿼리 성공')
c.shutdown()
except Exception as e:
print(f'오류: {type(e).**name**}: {e}')
"
