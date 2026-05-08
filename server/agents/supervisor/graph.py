"""Supervisor LangGraph — LLM tool-calling ReAct 패턴.

그래프 흐름:
  poll  →  supervisor(LLM)  →  tools  →  supervisor(LLM)  →  ...
                               (ReAct 루프)
                          ↓ tool call 없음
                         wait  →  poll (다음 사이클)
                               →  END  (종료 신호)

Supervisor LLM은 3개의 tool을 사용해 작업을 직접 실행합니다:
  - run_data_migration : 이관 작업 1건
  - run_sql_conversion : SQL 변환 작업 1건
  - run_sql_tuning     : SQL 튜닝 작업 묶음
"""

import threading
import time
from pathlib import Path
from typing import Literal

from langchain_core.messages import HumanMessage, RemoveMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from server.agents.supervisor.state import SupervisorState
import server.tools as supervisor_tools
from server.config.settings import (
    LLM_API_KEY,
    LLM_BASE_URL,
    LLM_MODEL,
    PLANNER_MAX_MIG_PER_CYCLE,
)

_RUNTIME_DIR = Path(__file__).resolve().parent.parent.parent.parent / "runtime"
PAUSE_FLAG = _RUNTIME_DIR / "agent.pause"
POLL_INTERVAL_SEC = 5

_stop_event = threading.Event()


def request_stop() -> None:
    _stop_event.set()


_SYSTEM_PROMPT = f"""당신은 데이터 마이그레이션 파이프라인의 수퍼바이저 에이전트입니다.
DB에서 폴링된 대기 작업 목록을 분석하여 적절한 도구를 호출해 모든 작업을 처리하세요.

[판단 기준]
1. RETRY 횟수가 3회 이상인 이관 작업은 건너뛰세요.
2. PRIORITY 숫자가 낮을수록 먼저 처리합니다.
3. 이관(run_data_migration) 작업은 한 사이클에 최대 {{max_mig_per_cycle}}건으로 제한합니다.
4. SQL 변환(run_sql_conversion) 작업은 이관 완료 여부와 무관하게 독립적으로 실행 가능합니다.
5. SQL 튜닝(run_sql_tuning) 작업은 변환 완료(PASS) 건에 대해 실행합니다.
6. 처리할 작업이 없거나 모든 작업을 처리했으면 도구를 호출하지 말고 완료하세요.

도구를 사용해 모든 적절한 작업을 처리한 뒤 완료하세요.""".format(
    max_mig_per_cycle=PLANNER_MAX_MIG_PER_CYCLE
)


def build_supervisor_graph(
    get_migration_jobs,
    get_sql_jobs,
    get_tuning_jobs,
    mig_increment_batch,
    mig_process_job,
    sql_increment_batch,
    sql_process_job,
    tune_process_job,
    logger,
):
    # ── 사이클마다 갱신되는 job 레지스트리 ────────────────────────────────────
    _mig_registry: dict = {}
    _sql_registry: dict = {}
    _tuning_registry: dict = {}

    # ── Tool 초기화 ───────────────────────────────────────────────────────────
    supervisor_tools.init_callbacks(
        mig_inc=mig_increment_batch,
        mig_proc=mig_process_job,
        sql_inc=sql_increment_batch,
        sql_proc=sql_process_job,
        tune_proc=tune_process_job,
        logger=logger,
    )
    _mig_registry, _sql_registry, _tuning_registry = supervisor_tools.get_registries()

    tools = [
        supervisor_tools.run_data_migration,
        supervisor_tools.run_sql_conversion,
        supervisor_tools.run_sql_tuning,
    ]
    tool_node = ToolNode(tools)

    llm = ChatOpenAI(
        api_key=LLM_API_KEY or "EMPTY",
        base_url=LLM_BASE_URL or None,
        model=LLM_MODEL,
        temperature=0,
    )
    llm_with_tools = llm.bind_tools(tools)

    # ── 노드 정의 ─────────────────────────────────────────────────────────────

    def poll_node(state: SupervisorState) -> dict:
        """DB를 폴링하고 LLM에게 전달할 컨텍스트 메시지를 구성한다."""
        if _stop_event.is_set():
            return {"stop_requested": True, "cycle": state.get("cycle", 0) + 1}

        cycle = state.get("cycle", 0) + 1
        logger.info(f"\n{'='*50}")
        logger.info(f"[Supervisor] Cycle {cycle} 시작")
        supervisor_tools.start_cycle_metrics(cycle)

        mig_jobs, sql_jobs, tuning_jobs = [], [], []
        try:
            mig_jobs = get_migration_jobs()
        except Exception as exc:
            logger.error(f"[Supervisor] DataMigration 폴링 오류: {exc}")
        try:
            sql_jobs = get_sql_jobs()
            tuning_jobs = get_tuning_jobs()
        except Exception as exc:
            logger.error(f"[Supervisor] SQL/Tuning 폴링 오류: {exc}")

        # job 레지스트리 갱신
        _mig_registry.clear()
        _sql_registry.clear()
        _tuning_registry.clear()
        for job in mig_jobs:
            _mig_registry[job.map_id] = job
        for job in sql_jobs:
            _sql_registry[job.row_id] = job
        for job in tuning_jobs:
            _tuning_registry[job.row_id] = job

        if mig_jobs:
            logger.info(f"[Supervisor] DataMigration 대기: {len(mig_jobs)}건")
        if sql_jobs:
            logger.info(f"[Supervisor] SqlConversion 대기: {len(sql_jobs)}건")
        if tuning_jobs:
            logger.info(f"[Supervisor] SqlTuning 대기: {len(tuning_jobs)}건")
        if not mig_jobs and not sql_jobs and not tuning_jobs:
            logger.info("[Supervisor] 대기 중인 작업 없음")

        # 이전 사이클 메시지 초기화
        old_messages = state.get("messages", [])
        clear = [RemoveMessage(id=m.id) for m in old_messages if getattr(m, "id", None)]

        # 현재 사이클 컨텍스트 구성
        lines = []
        if mig_jobs:
            lines.append(f"[이관 작업 — {len(mig_jobs)}건]")
            for j in mig_jobs:
                retry = getattr(j, "retry_count", 0) or 0
                lines.append(
                    f"  map_id={j.map_id} | {j.fr_table} → {j.to_table}"
                    f" | PRIORITY={j.priority} | RETRY={retry}회"
                )
        if sql_jobs:
            lines.append(f"\n[SQL 변환 작업 — {len(sql_jobs)}건]")
            for j in sql_jobs[:20]:
                lines.append(f"  row_id={j.row_id} | {j.space_nm}.{j.sql_id}")
            if len(sql_jobs) > 20:
                lines.append(f"  ... 외 {len(sql_jobs) - 20}건")
        if tuning_jobs:
            lines.append(f"\n[SQL 튜닝 작업 — {len(tuning_jobs)}건]")
            for j in tuning_jobs[:10]:
                lines.append(f"  row_id={j.row_id} | {j.space_nm}.{j.sql_id}")
            if len(tuning_jobs) > 10:
                lines.append(f"  ... 외 {len(tuning_jobs) - 10}건")
        if not lines:
            lines = ["현재 처리할 작업이 없습니다."]

        new_messages = [
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content="\n".join(lines)),
        ]

        return {
            "messages": clear + new_messages,
            "cycle": cycle,
            "stop_requested": False,
        }

    def supervisor_node(state: SupervisorState) -> dict:
        """LLM이 상황을 판단하고 tool 호출 여부를 결정한다."""
        response = llm_with_tools.invoke(state["messages"])
        return {"messages": [response]}

    def wait_node(_state: SupervisorState) -> dict:
        """폴링 주기만큼 대기. PAUSE_FLAG 파일이 있으면 재개 신호까지 무한 대기."""
        supervisor_tools.finish_cycle_metrics(logger=logger)
        paused_logged = False
        while PAUSE_FLAG.exists():
            if _stop_event.is_set():
                return {"stop_requested": True}
            if not paused_logged:
                logger.info("[Supervisor] ⏸  일시정지 중... (runtime/agent.pause 감지)")
                paused_logged = True
            time.sleep(0.5)
        if paused_logged:
            logger.info("[Supervisor] ▶  일시정지 해제, 재개합니다.")

        elapsed = 0.0
        step = 0.2
        while elapsed < POLL_INTERVAL_SEC:
            if _stop_event.is_set() or PAUSE_FLAG.exists():
                break
            time.sleep(step)
            elapsed += step
        return {"stop_requested": _stop_event.is_set()}

    # ── 라우팅 함수 ────────────────────────────────────────────────────────────

    def route_after_supervisor(state: SupervisorState) -> Literal["tools", "wait"]:
        """LLM이 tool을 호출하면 tools로, 아니면 wait으로."""
        last = state["messages"][-1]
        if getattr(last, "tool_calls", None):
            return "tools"
        return "wait"

    def route_after_wait(state: SupervisorState) -> Literal["poll", "__end__"]:
        if _stop_event.is_set() or state.get("stop_requested"):
            return END
        return "poll"

    # ── 그래프 조립 ────────────────────────────────────────────────────────────
    #
    #  poll → supervisor → tools → supervisor (ReAct 루프)
    #                    ↓ (tool call 없음)
    #                   wait → poll (다음 사이클) or END
    #
    workflow = StateGraph(SupervisorState)

    workflow.add_node("poll", poll_node)
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("tools", tool_node)
    workflow.add_node("wait", wait_node)

    workflow.set_entry_point("poll")
    workflow.add_edge("poll", "supervisor")
    workflow.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {"tools": "tools", "wait": "wait"},
    )
    workflow.add_edge("tools", "supervisor")  # ReAct 루프
    workflow.add_conditional_edges(
        "wait",
        route_after_wait,
        {"poll": "poll", END: END},
    )

    return workflow.compile()
