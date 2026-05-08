"""다이어그램 생성 도구.

두 가지 스타일의 다이어그램을 생성합니다:
  1) 배치 흐름도  (flowchart 스타일 - 박스+다이아몬드)
  2) LangGraph 노드 다이어그램 (노드+엣지 스타일)

결과물: diagrams/ 폴더에 .mmd 파일 저장
        → https://mermaid.live 에 붙여넣기하면 PNG/SVG 다운로드 가능

실행:
  python tools/generate_diagrams.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT  = ROOT / "diagrams"
OUT.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# 1. 배치 흐름도 (이미지 1번 스타일)
#    Supervisor 전체 파이프라인 흐름
# ─────────────────────────────────────────────────────────────────────────────
BATCH_FLOW = """\
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#fff","edgeLabelBackground":"#fff","fontSize":"14px"}}}%%
flowchart LR
    classDef term  fill:#FDDAB0,stroke:#e0956a,color:#333,font-weight:bold
    classDef proc  fill:#ffffff,stroke:#555,color:#111
    classDef agent fill:#EEF0FB,stroke:#8888cc,color:#111
    classDef diam  fill:#ffffff,stroke:#555

    START([배치 시작]):::term --> S1

    S1["**1** | 스케줄러
    작업 대상 유무 확인
    ─────────────
    • 작업유무 = Y
    • 사용유무 = Y"]:::proc --> D1

    D1{카운트 > 0?}:::diam
    D1 -->|no| BEND([배치 종료]):::term
    D1 -->|yes| S2

    S2["**2** | 입력값 조회
    Mapping Rule 조회
    ─────────────
    DDL / 매핑 컬럼 / 의존성
    프롬프트 컨텍스트 구성"]:::proc

    S2 --> MIG_BLOCK & SQL_BLOCK & TUNE_BLOCK

    subgraph MIG_BLOCK["Data Migration Agent"]
        direction TB
        M3["**3** | 이관 SQL
        LLM → SQL 생성 및 실행
        ──────────────────
        INSERT INTO target
        SELECT ... FROM source"]:::agent
        M4["**4** | 검증 SQL
        검증 실행
        ──────────────────
        UNION ALL COUNT 비교"]:::agent
        DM{Loop END?}:::diam
        M5["**5** | 결과 입력
        Status / LOG
        DB 저장"]:::agent

        M3 --> M4 --> DM
        DM -->|"no (재시도)
        max 3회"| M3
        DM -->|yes| M5
    end

    subgraph SQL_BLOCK["SQL Conversion Agent"]
        direction TB
        Q3["**3** | TO-BE SQL 생성
        MyBatis → Oracle SQL
        ──────────────────
        LLM + 매핑룰 기반 변환"]:::agent
        Q4["**4** | 건수 검증
        FROM vs TO-BE 비교
        ──────────────────
        UNION ALL SELECT"]:::agent
        DQ{Loop END?}:::diam
        Q5["**5** | 결과 저장
        TO_SQL_TEXT / LOG
        DB 저장"]:::agent

        Q3 --> Q4 --> DQ
        DQ -->|no| Q3
        DQ -->|yes| Q5
    end

    subgraph TUNE_BLOCK["SQL Tuning Agent"]
        direction TB
        T3["**3** | RAG 튜닝
        룰 검색 + LLM 적용
        ──────────────────
        FAISS / Token 검색"]:::agent
        T4["**4** | 최종 검증
        TUNED_TEST 실행
        ──────────────────
        건수 비교"]:::agent
        DT{Loop END?}:::diam
        T5["**5** | 결과 저장
        TUNED_SQL / TUNED_TEST
        DB 저장"]:::agent

        T3 --> T4 --> DT
        DT -->|no| T3
        DT -->|yes| T5
    end

    M5 & Q5 & T5 --> START
"""

# ─────────────────────────────────────────────────────────────────────────────
# 2-A. LangGraph 노드 다이어그램 - Supervisor Graph (이미지 2번 스타일)
# ─────────────────────────────────────────────────────────────────────────────
LANGGRAPH_SUPERVISOR = """\
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#E8E8FB","edgeLabelBackground":"#fff","fontSize":"13px"}}}%%
flowchart TD
    classDef node  fill:#E8E8FB,stroke:#9090cc,color:#222,rx:8
    classDef term  fill:#C8C8F0,stroke:#7070bb,color:#222,rx:20,font-weight:bold
    classDef wait  fill:#F0F0FF,stroke:#aaaadd,color:#555,stroke-dasharray:4

    ST([__start__]):::term --> SV

    SV["supervisor_node
    ────────────────
    DB 폴링 (5초 주기)
    대기 작업 수집
    종료 신호 감지"]:::node

    SV -->|stop_requested| EN([__end__]):::term
    SV -->|"작업 없음"| WT
    SV -->|"Mig 작업 Send"| DM
    SV -->|"SQL 작업 Send"| SC
    SV -->|"Tuning 작업 Send"| TA

    DM["data_migration_agent
    ────────────────
    MigrationOrchestrator
    1건씩 처리"]:::node

    SC["sql_conversion_agent
    ────────────────
    TobeMultiAgentCoordinator
    SQL 변환 + 검증"]:::node

    TA["sql_tuning_agent
    ────────────────
    SqlTuningAgent
    RAG 기반 튜닝"]:::node

    DM & SC & TA --> WT

    WT["wait_node
    ────────────────
    5초 대기
    pause flag 감지
    stop 신호 감지"]:::wait

    WT -->|continue| SV
    WT -->|stop| EN
"""

# ─────────────────────────────────────────────────────────────────────────────
# 2-B. LangGraph 노드 다이어그램 - Data Migration Graph (이미지 2번 스타일)
# ─────────────────────────────────────────────────────────────────────────────
LANGGRAPH_MIGRATION = """\
%%{init: {"theme":"base","themeVariables":{"primaryColor":"#E8E8FB","edgeLabelBackground":"#fff","fontSize":"13px"}}}%%
flowchart TD
    classDef node  fill:#E8E8FB,stroke:#9090cc,color:#222
    classDef term  fill:#C8C8F0,stroke:#7070bb,color:#222,font-weight:bold
    classDef retry fill:#FFF0E0,stroke:#ccaa66,color:#333,stroke-dasharray:4

    ST([__start__]):::term --> FD

    FD["fetch_ddl
    ────────────────
    소스 DDL 조회
    타겟 DDL 조회
    (ALL_TAB_COLUMNS)"]:::node --> CD

    CD["check_dependency
    ────────────────
    선행 작업 상태 확인
    PRIORITY 기준"]:::node

    CD -->|READY| GN
    CD -->|DEPENDENCY_FAIL| FN

    GN["generate
    ────────────────
    LLM 호출
    migration_sql 생성
    verification_sql 생성"]:::node

    GN -->|SQL 생성 성공| EX
    GN -->|LLM 오류| LW
    GN -->|재시도 초과| FN

    LW["llm_retry_wait
    ────────────────
    1초 대기
    retry_count +1"]:::retry --> GN

    EX["execute
    ────────────────
    INSERT 실행
    COMMIT"]:::node

    EX -->|실행 성공| VF
    EX -->|DBSqlError| BR

    VF["verify
    ────────────────
    UNION ALL COUNT 비교
    DIFF = 0 확인"]:::node

    VF -->|"DIFF = 0 (PASS)"| FN
    VF -->|"DIFF ≠ 0 (FAIL)"| BR

    BR["biz_retry_prepare
    ────────────────
    로그 기록
    TRUNCATE target
    db_attempts +1
    1초 대기"]:::retry --> GN

    FN["finalize
    ────────────────
    STATUS 업데이트
    (PASS/FAIL/SKIP)
    elapsed 기록"]:::node --> EN

    EN([__end__]):::term
"""

# ─────────────────────────────────────────────────────────────────────────────
# 저장 + LangGraph 실제 코드에서 추출 시도
# ─────────────────────────────────────────────────────────────────────────────
def save(filename: str, content: str):
    path = OUT / filename
    path.write_text(content, encoding="utf-8")
    print(f"  OK {path}")


def try_extract_from_langgraph():
    """LangGraph 그래프에서 실제 Mermaid 코드 추출."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from server.agents.migration.graph import migration_graph
        code = migration_graph.get_graph().draw_mermaid()
        save("langgraph_migration_ACTUAL.mmd", code)
        print("  OK LangGraph 실제 그래프 추출 완료")
    except Exception as e:
        print(f"  ⚠ LangGraph 실제 추출 실패 (수동 작성 버전 사용): {e}")


def main():
    print(f"\n다이어그램 생성 → {OUT}\n")

    save("1_batch_flow.mmd",          BATCH_FLOW)
    save("2a_langgraph_supervisor.mmd", LANGGRAPH_SUPERVISOR)
    save("2b_langgraph_migration.mmd",  LANGGRAPH_MIGRATION)

    print("\n[LangGraph 실제 코드 추출 시도]")
    try_extract_from_langgraph()

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 PNG/SVG 변환 방법
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 1) 온라인: https://mermaid.live
    → .mmd 파일 내용 복사 후 붙여넣기 → PNG 다운로드

 2) CLI: npm install -g @mermaid-js/mermaid-cli
    mmdc -i diagrams/1_batch_flow.mmd -o diagrams/1_batch_flow.png
    mmdc -i diagrams/2a_langgraph_supervisor.mmd -o diagrams/2a.png
    mmdc -i diagrams/2b_langgraph_migration.mmd -o diagrams/2b.png

 3) VS Code: "Markdown Preview Mermaid Support" 확장 설치
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    main()
