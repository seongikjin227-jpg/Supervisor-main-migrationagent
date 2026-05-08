# Supervisor Multi-Agent Pipeline

Oracle 데이터 이관, MyBatis SQL 변환, SQL 튜닝을 하나의 파이프라인으로 자동화하는 멀티 에이전트 시스템입니다.  
**Supervisor Agent**가 DB 대기열을 주기적으로 폴링하고, 3개의 전문 에이전트를 고정 배치 크기로 실행합니다.

---

## 전체 구조

```
┌─────────────────────────────────────────────────────────────────┐
│                      Supervisor Agent                           │
│        (Deterministic Batch · LangGraph 상태 머신)               │
│                                                                 │
│   DB 폴링 → 최대 20건씩 Tool 실행 → 5초 대기 → 반복              │
└──────────────┬──────────────┬──────────────┬────────────────────┘
               │              │              │
               ▼              ▼              ▼
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
     │  Mig Agent   │ │  SQL Agent   │ │ Tuning Agent │
     │ 데이터 이관  │ │  SQL 변환    │ │  SQL 튜닝    │
     └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
            │                │                │
            ▼                ▼                ▼
     NEXT_MIG_INFO    NEXT_SQL_INFO    NEXT_SQL_INFO
     (STATUS:READY)   (STATUS:PASS)   (TUNED_TEST:PASS)
```

---

## Supervisor Agent

> **역할**: DB의 대기 작업을 주기적으로 폴링하고, 각 에이전트별로 최대 20건씩 바로 실행합니다.

### 동작 방식

Supervisor는 작업 대상 선정을 LLM에 맡기지 않습니다. DB polling 결과를 기준으로 **Data Migration, SQL Conversion, SQL Tuning을 각각 최대 20건씩** 실행합니다. 에이전트 프로세스는 종료 신호를 받기 전까지 계속 돌며, 처리할 작업이 없는 loop에서도 5초 대기 후 다시 polling합니다.

```
1. Poll   — NEXT_MIG_INFO, NEXT_SQL_INFO 전체 대기 작업 목록 조회
2. Select — 각 에이전트별 대기 작업 중 최대 20건을 실행 대상으로 등록
3. Call   — run_data_migration / run_sql_conversion / run_sql_tuning 실행
4. Wait   — cycle 집계 저장 후 5초 대기
5. Loop   — 종료 신호가 없으면 1로 돌아감
```

### 오케스트레이션 규칙

| 규칙 | 내용 |
|------|------|
| 재시도 제한 | `RETRY >= 3`인 Migration 작업은 건너뜀 |
| 우선순위 | `PRIORITY` 컬럼 오름차순으로 처리 |
| 배치 크기 | 한 batch loop에서 Data Migration, SQL Conversion, SQL Tuning을 각각 최대 20건 실행 |
| 독립 실행 | SQL Agent는 Migration 완료 여부와 무관하게 독립 실행 |
| 계속 실행 | 처리할 작업이 없어도 종료하지 않고 5초 후 다시 polling |

### 사용 가능한 Tools

```python
run_data_migration(map_id: str)        # Mig Agent 실행
run_sql_conversion(row_id: str)        # SQL Agent 실행
run_sql_tuning(row_ids: list[str])     # Tuning Agent 실행
```

---

## Mig Agent (데이터 이관)

> **역할**: `NEXT_MIG_INFO` 테이블의 대기 작업을 처리해 Oracle 간 데이터를 이관합니다.

### 처리 흐름

```
NEXT_MIG_INFO (STATUS=READY)
        │
        ▼
1. Fetch DDL      — 소스·타겟 테이블 스키마 조회
        │
        ▼
2. Check Deps     — 선행 작업 완료 여부 확인
        │
        ▼
3. Generate SQL   — LLM이 매핑 규칙 기반으로 이관 SQL 생성
        │
        ▼
4. Execute SQL    — Oracle에서 이관 SQL 실행
        │
        ▼
5. Verify         — 소스·타겟 row count 비교 검증
        │
        ▼
NEXT_SQL_INFO 생성 (STATUS=READY) → SQL Agent로 인계
```

### 핵심 동작

- **재시도**: 실패 시 LLM에 오류 피드백 후 재생성, 최대 3회
- **카운터**: 처리 시마다 `BATCH_CNT` 증가
- **결과 상태**: `STATUS` 컬럼에 `PASS` / `FAIL` / `SKIP` 저장
- **연쇄**: 이관 성공 후 `NEXT_SQL_INFO`에 `STATUS='READY'` 행 자동 생성

---

## SQL Agent (SQL 변환)

> **역할**: 레거시 MyBatis SQL을 Oracle 호환 TO-BE SQL로 변환하고 row count 검증을 수행합니다.

### 처리 흐름

```
NEXT_SQL_INFO (STATUS: READY / PENDING / FAIL / NULL)
        │
        ▼
1. Generate TO-BE SQL  — LLM이 FR_SQL_TEXT 또는 EDIT_FR_SQL 기반으로 변환
        │
        ▼
2. Extract Bind Params — SQL 내 바인드 파라미터(:param) 추출
        │
        ▼
3. Generate Bind SQL   — 실제 바인드 값을 가져올 쿼리 생성
        │
        ▼
4. Build Bind Sets     — 대표 테스트 케이스 3개 수집
        │
        ▼
5. Generate Test SQL   — 바인드 셋을 적용한 검증 쿼리 생성
        │
        ▼
6. Execute & Validate  — 원본 SQL vs TO-BE SQL row count 비교
        │
        ▼
STATUS=PASS, TUNED_TEST=READY → Tuning Agent로 인계
```

### 핵심 동작

- **RAG 미사용**: 기본 변환만 수행 (튜닝 룰 적용 없음)
- **결과 컬럼**: `TO_SQL_TEXT`, `STATUS`, `TUNED_TEST`, `BIND_SQL`, `BIND_SET`
- **검증 기준**: 원본 SQL과 TO-BE SQL의 row count 일치 여부
- **독립 실행**: Migration 결과와 무관하게 `NEXT_SQL_INFO` 상태만 보고 실행

---

## Tuning Agent (SQL 튜닝)

> **역할**: RAG(검색 증강 생성)로 관련 튜닝 룰을 조회해 TO-BE SQL의 성능을 최적화합니다.

### 처리 흐름

```
NEXT_SQL_INFO (TUNED_TEST: READY / FAIL, BATCH_CNT < 30)
        │
        ▼
1. Retrieve Rules  — FAISS 임베딩으로 tobe_rule_catalog에서 관련 룰 Top-K 조회
        │
        ▼
2. Tune SQL        — LLM이 룰을 적용해 TO_SQL_TEXT → 튜닝 SQL 생성
        │
        ▼
3. Generate Test   — 튜닝 SQL 검증용 쿼리 생성
        │
        ▼
4. Execute & Validate — TO_SQL_TEXT vs TUNED_SQL row count 비교
        │
        ▼
TUNED_SQL 저장, TUNED_TEST = PASS / FAIL
```

### 핵심 동작

- **RAG 엔진**: FAISS CPU 인덱스 + `BAAI/bge-m3` 임베딩 모델
- **멀티 이터레이션**: `TOBE_SQL_TUNING_MAX_ITERATIONS >= 2`이면 직전 튜닝 결과를 다음 반복 입력으로 사용
- **재시도 기준**: `BATCH_CNT < 30`인 `FAIL` 작업은 다음 사이클에서 재시도
- **배치 실행**: Supervisor가 한 batch loop에서 최대 20건까지 dispatch
- **결과 컬럼**: `TUNED_SQL`, `TUNED_TEST`, `BLOCK_RAG_CONTENT`

---

## DB 상태 흐름

```
NEXT_MIG_INFO                       NEXT_SQL_INFO
─────────────                       ─────────────
STATUS=READY                        (Mig Agent가 생성)
   │                                STATUS=READY
   │  Mig Agent                        │
   ▼                                   │  SQL Agent
STATUS=PASS  ─────────────────────►    ▼
                                    STATUS=PASS
                                    TUNED_TEST=READY
                                       │
                                       │  Tuning Agent
                                       ▼
                                    TUNED_TEST=PASS
                                    TUNED_SQL 저장
```

### 주요 컬럼

| 컬럼 | 관리 에이전트 | 설명 |
|------|-------------|------|
| `STATUS` | SQL Agent | TO-BE SQL 검증 결과 (`PASS` / `FAIL`) |
| `TO_SQL_TEXT` | SQL Agent | 변환된 TO-BE SQL |
| `BIND_SQL` | SQL Agent | 바인드 값 추출 SQL |
| `BIND_SET` | SQL Agent | 테스트용 바인드 값 JSON |
| `TUNED_SQL` | Tuning Agent | 최종 튜닝 SQL |
| `TUNED_TEST` | Tuning Agent | 튜닝 상태 (`READY` / `PASS` / `FAIL`) |
| `BLOCK_RAG_CONTENT` | Tuning Agent | 프롬프트에 사용된 RAG 룰 JSON |
| `BATCH_CNT` | SQL·Tuning Agent | 처리 횟수 (30 초과 시 재시도 중단) |

---

## Agent 실행 시간 집계

Supervisor는 한 batch loop에서 실제 처리한 작업이 있을 때 `AG_AGENT_RUN_METRICS`에 실행 시간을 집계해 저장합니다. `BATCH_NO`는 Supervisor 프로세스 실행 1회를 묶는 번호이고, `CYCLE_NO`는 해당 batch 안에서 DB polling이 실행된 순번입니다. `SUPERVISOR_CYCLE`은 poll과 tool 실행 시간을 포함하며 다음 poll까지의 5초 대기 시간은 제외합니다. `DB_MIGRATION`, `SQL_MIGRATION`, `SQL_TUNING`은 각 에이전트 tool이 처리한 job 수와 총 소요 시간을 기록합니다.

```sql
CREATE TABLE AG_AGENT_RUN_METRICS (
    RUN_ID              NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    BATCH_NO            NUMBER,
    CYCLE_NO            NUMBER,
    AGENT_NAME          VARCHAR2(50) NOT NULL,
    JOB_COUNT           NUMBER DEFAULT 0 NOT NULL,
    SUCCESS_COUNT       NUMBER DEFAULT 0 NOT NULL,
    FAIL_COUNT          NUMBER DEFAULT 0 NOT NULL,
    SKIP_COUNT          NUMBER DEFAULT 0 NOT NULL,
    STARTED_AT          TIMESTAMP,
    FINISHED_AT         TIMESTAMP,
    ELAPSED_SECONDS     NUMBER,
    CREATED_AT          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IX_AG_AGENT_RUN_METRICS_01
    ON AG_AGENT_RUN_METRICS (BATCH_NO, CYCLE_NO, AGENT_NAME);
```

기존에 `AG_AGENT_RUN_METRICS`를 이미 생성했다면 아래 DDL로 `BATCH_NO`를 추가합니다.

```sql
ALTER TABLE AG_AGENT_RUN_METRICS ADD (BATCH_NO NUMBER);

DROP INDEX IX_AG_AGENT_RUN_METRICS_01;

CREATE INDEX IX_AG_AGENT_RUN_METRICS_01
    ON AG_AGENT_RUN_METRICS (BATCH_NO, CYCLE_NO, AGENT_NAME);
```

---

## SQL 보조 서비스

`server/services/sql/`에는 Supervisor가 직접 호출하는 Agent 외에도, SQL 작업 데이터를 준비하거나 검증을 보조하는 유틸 서비스가 있습니다. 운영 흐름에서는 `main.py`가 `NEXT_SQL_INFO`를 폴링해 변환/튜닝을 수행하지만, MyBatis mapper XML을 처음 적재하거나 튜닝 룰 테이블을 준비할 때는 아래 보조 기능을 별도로 실행해야 합니다.

### MyBatis XML Parser

파일: `server/services/sql/xml_parser_service.py`

MyBatis mapper XML을 읽어 `NEXT_SQL_INFO`에 SQL 작업 후보를 동기화하는 4단계 유틸입니다.

| Stage | 동작 | 결과 |
|------|------|------|
| `stage1` | `MAPPER_XML_SOURCE_DIR` 하위의 mapper XML을 재귀 파싱 | `server/services/sql/DATA/*.json` 또는 지정 출력 경로에 JSON 생성 |
| `stage2` | stage1 JSON을 `NEXT_SQL_INFO`에 MERGE upsert | `TAG_KIND`, `SPACE_NM`, `SQL_ID`, `FR_SQL_TEXT`, `TARGET_TABLE` 저장 |
| `stage3` | `<include refid="...">` SQL fragment를 재귀 확장 | 확장된 SQL을 `EDIT_FR_SQL`에 저장 |
| `stage4` | 활성 SQL ID 기준으로 정리하고 테이블명 보정 | 비활성 SQL 삭제, `TARGET_TABLE` 보정 |
| `all` | `stage1`부터 `stage4`까지 순서대로 실행 | XML 적재부터 정리까지 일괄 수행 |

실행 예시는 다음과 같습니다.

```bash
python -m server.services.sql.xml_parser_service all
python -m server.services.sql.xml_parser_service stage1 --source-dir C:\path\to\mapper --output-dir C:\path\to\xml-json
python -m server.services.sql.xml_parser_service stage2 --output-dir C:\path\to\xml-json
python -m server.services.sql.xml_parser_service stage3
python -m server.services.sql.xml_parser_service stage4
```

필수 환경 변수는 아래 항목입니다.

```env
MAPPER_XML_SOURCE_DIR=C:\path\to\mapper-xml
XML_PARSER_DATA_DIR=server/services/sql/DATA
ACTIVE_SQL_ID_TABLE=ACTIVE_SQL_ID_TABLE_NAME
ACTIVE_SQL_ID_COLUMN=SQL_ID
```

주의할 점은 `ACTIVE_SQL_ID_COLUMN` 값이 반드시 `namespace.sqlId` 형식이어야 한다는 것입니다. 예를 들어 `com.example.UserMapper.selectUser`처럼 namespace와 SQL ID가 함께 있어야 mapper namespace 충돌을 피할 수 있습니다.

### MyBatis Materializer

파일: `server/services/sql/mybatis_materializer_service.py`

MyBatis 동적 SQL을 특정 bind 값 기준의 실행 SQL 문자열로 렌더링하는 내부 유틸입니다. `<if>`, `<choose>`, `<when>`, `<otherwise>`, `<where>`, `<trim>`, `<foreach>`와 `#{param}`, `${param}` 토큰을 처리합니다.

이 파일은 CLI 진입점이 없고, SQL 검증/테스트 SQL 생성 과정에서 import해서 사용하는 보조 함수 성격입니다. 직접 확인하려면 Python에서 `materialize_sql(sql_text, bind_case)`를 호출합니다.

```python
from server.services.sql.mybatis_materializer_service import materialize_sql

sql = """
SELECT *
FROM USERS
<where>
  <if test="userId != null">AND USER_ID = #{userId}</if>
</where>
"""

print(materialize_sql(sql, {"userId": 100}))
```

### Bind/Validation/Tuning 서비스

| 파일 | 역할 | 직접 실행 |
|------|------|----------|
| `binding_service.py` | MyBatis bind 파라미터 추출, bind case 최대 3개 구성, JSON 직렬화 | 없음 |
| `validation_service.py` | LLM이 만든 bind/test SQL 실행, row count 검증 결과 판정 | 없음 |
| `tobe_sql_tuning_service.py` | `NEXT_SQL_RULES` 또는 `tobe_rule_catalog.json`에서 튜닝 룰 로드, FAISS/임베딩 검색, 실패 시 토큰 검색 fallback | 없음 |
| `prompt_service.py` | `server/config/prompts/*.json` 프롬프트 템플릿 로드 | 없음 |
| `llm_service.py` | TO-BE SQL, bind SQL, test SQL, 튜닝 SQL 생성을 위한 LLM 호출 래퍼 | 없음 |
| `batch_scheduler.py` | `NEXT_SQL_INFO`를 1분마다 폴링하는 SQL 변환/튜닝 단독 스케줄러 | 가능 |

`batch_scheduler.py`는 Supervisor 없이 SQL 파이프라인만 돌리고 싶을 때 사용할 수 있습니다.

```bash
python -m server.services.sql.batch_scheduler
```

다만 기본 운영 경로는 `python main.py`입니다. Supervisor는 Migration, SQL Conversion, SQL Tuning 대기열을 함께 polling하고, 각 에이전트별로 최대 20건씩 dispatch하므로 전체 파이프라인 운영에는 Supervisor 실행을 우선 사용합니다.

### 튜닝 룰 테이블과 보조 스크립트

RAG 튜닝 룰은 우선 `NEXT_SQL_RULES` 테이블에서 읽고, 실패하면 `server/services/sql/data/rag/tobe_rule_catalog.json`으로 fallback합니다. JSON 룰을 DB 테이블로 적재하려면 다음 스크립트를 실행합니다.

```bash
python scripts/create_sql_rules_table.py
```

매핑 룰 조회 및 샘플 매핑 룰 적재 스크립트도 제공됩니다.

```bash
python scripts/list_mapping_rules.py --format table
python scripts/list_mapping_rules.py --fr-table EMPLOYEES --format json
python scripts/seed_mig_rules.py
python scripts/init_db.py
```

`scripts/init_db.py`는 Oracle 연결, 필수 테이블 접근, LLM 연결 상태를 점검합니다. 운영 전 가장 먼저 실행해 `.env`와 DB 권한 문제를 확인하는 용도입니다.

---

## 프론트엔드 (Streamlit 대시보드)

Streamlit 기반 웹 UI로 에이전트 실행 현황을 실시간으로 모니터링하고 제어합니다.

### 페이지 구성

| 페이지 | 설명 |
|--------|------|
| **Dashboard** | 전체 현황 요약 + AI 챗봇 (자연어로 DB 조회) |
| **Mig Agent Monitor** | Migration 작업 목록·상태·SQL 로그 조회 |
| **SQL Agent Monitor** | SQL 변환 작업·생성된 TO-BE SQL·바인드 정보 조회 |
| **Tuning Agent Monitor** | 튜닝 진행 현황·적용된 RAG 룰 조회 |
| **Job Detail** | 개별 작업 상세 정보 (SQL 전문·바인드 셋·오류 내역) |
| **RAG Rule Manager** | 튜닝 룰 카탈로그 업로드·편집 |
| **System Health** | DB 연결·LLM 연결·테이블 상태 점검 |
| **Settings** | 환경 변수 관리 및 에이전트 프로세스 재시작 |

### 에이전트 제어 (사이드바)

```
[Start]  — main.py 서브프로세스 실행
[Pause]  — runtime/agent.pause 플래그 생성 (일시 정지)
[Resume] — 플래그 삭제 (재개)
[Stop]   — SIGTERM 전송 (종료)
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 언어 | Python 3.8+ |
| 멀티 에이전트 | LangGraph 0.2+ (상태 그래프) |
| LLM | LangChain + OpenAI / Anthropic (설정 가능) |
| 벡터 DB | FAISS CPU (RAG 임베딩) |
| 데이터베이스 | Oracle DB (oracledb 2.1+, Thick/Thin 모드) |
| 프론트엔드 | Streamlit 1.35+ |
| 데이터 처리 | Pandas 2.0+, Plotly 5.18+ |
| 스케줄링 | APScheduler 3.10+ |
| 설정 관리 | python-dotenv |

---

## 설치 및 실행

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env.example`을 복사해 `.env`를 만든 뒤, 실행 환경에 맞는 값을 채웁니다.

```powershell
Copy-Item .env.example .env
```

macOS/Linux에서는 다음 명령을 사용합니다.

```bash
cp .env.example .env
```

주요 설정값은 다음과 같습니다. 전체 예시는 `.env.example`을 기준으로 확인합니다.

```env
# Oracle DB
DB_USER=
DB_PASS=
DB_HOST=localhost
DB_PORT=1521
DB_SID=xe
ORACLE_CLIENT_PATH=          # 선택: Thick 모드 사용 시 Oracle Client 경로
ORACLE_SCHEMA=               # 선택: 테이블 스키마 prefix

# 대상 테이블
MAPPING_RULE_TABLE=NEXT_MIG_INFO
MAPPING_RULE_DETAIL_TABLE=NEXT_MIG_INFO_DTL
RESULT_TABLE=NEXT_SQL_INFO

# LLM (openai 또는 anthropic)
LLM_PROVIDER=openai
LLM_API_KEY=
LLM_MODEL=gpt-4o-mini
LLM_BASE_URL=                # 선택: 커스텀 엔드포인트 사용 시
LLM_MAX_TOKENS=4096          # 숫자 입력 필요

# RAG 임베딩
RAG_EMBED_BASE_URL=
RAG_EMBED_API_KEY=
RAG_EMBED_MODEL=BAAI/bge-m3
RAG_EMBED_TIMEOUT_SEC=30
TOBE_RULE_CATALOG_PATH=server/services/sql/data/rag/tobe_rule_catalog.json
TOBE_SQL_TUNING_TOP_K=3
TOBE_SQL_TUNING_MAX_ITERATIONS=1

# MyBatis XML 파서
MAPPER_XML_SOURCE_DIR=
XML_PARSER_DATA_DIR=server/services/sql/DATA
ACTIVE_SQL_ID_TABLE=
ACTIVE_SQL_ID_COLUMN=SQL_ID

# Supervisor 설정
PLANNER_ENABLED=true
PLANNER_MAX_MIG_PER_CYCLE=5
SUPERVISOR_RECURSION_LIMIT=10000
MIG_KIND=DB_MIG
```

현재 Supervisor는 작업 대상 선정에 LLM을 사용하지 않으며, 코드 기준 batch loop당 에이전트별 최대 20건을 실행합니다. `PLANNER_MAX_MIG_PER_CYCLE`은 예전 LLM planner/ReAct 모드 호환 설정으로 남아 있으며 현재 deterministic batch 실행 한도에는 사용하지 않습니다.

### 3. 사전 점검

```bash
python scripts/init_db.py
```

### 4. MyBatis XML 적재가 필요한 경우

mapper XML에서 `NEXT_SQL_INFO`를 생성해야 한다면 에이전트 실행 전에 XML 파서를 먼저 실행합니다.

```bash
python -m server.services.sql.xml_parser_service all
```

### 5. 에이전트 실행

```bash
python main.py
```

### 6. 대시보드 실행

```bash
streamlit run app/app.py
```

---

## 프로젝트 구조

```
.
├── main.py                        # 에이전트 시스템 진입점
├── requirements.txt
├── app/                           # Streamlit 대시보드
│   ├── app.py
│   ├── pages/                     # 모니터링·관리 페이지
│   └── utils/                     # DB 조회·에이전트 제어 유틸
├── scripts/                       # 점검·룰 적재·다이어그램 생성 스크립트
│   ├── init_db.py                 # DB/LLM/테이블 health check
│   ├── create_sql_rules_table.py  # 튜닝 룰 JSON → NEXT_SQL_RULES 적재
│   ├── list_mapping_rules.py      # 매핑 룰 조회
│   └── seed_mig_rules.py          # 샘플 매핑 룰 적재
└── server/
    ├── agents/
    │   ├── supervisor/            # Supervisor Agent (LangGraph deterministic batch)
    │   ├── migration/             # Mig Agent
    │   ├── sql_conversion/        # SQL Agent
    │   └── sql_tuning/            # Tuning Agent
    ├── services/
    │   ├── migration/             # 이관 비즈니스 로직
    │   └── sql/                   # SQL 변환·튜닝 파이프라인
    │       ├── xml_parser_service.py
    │       ├── mybatis_materializer_service.py
    │       ├── binding_service.py
    │       ├── validation_service.py
    │       └── data/rag/tobe_rule_catalog.json
    ├── repositories/              # DB 접근 레이어
    ├── core/                      # DB 연결·LLM 클라이언트·로거
    ├── tools/                     # Supervisor Tool 정의
    └── config/
        ├── settings.py
        └── prompts/               # LLM 프롬프트 템플릿 (JSON)
```
