"""Supervisor Agent.

LLM이 3개의 에이전트를 tool로 직접 호출하는 ReAct 패턴으로 동작합니다.

  Tool 1: run_data_migration  — 데이터 이관 (DataMigrationAgent)
  Tool 2: run_sql_conversion  — SQL 변환   (SqlConversionAgent)
  Tool 3: run_sql_tuning      — SQL 튜닝   (SqlTuningAgent)
"""

import logging
import os
import signal

from server.agents.supervisor.graph import build_supervisor_graph, request_stop
from server.agents.supervisor.state import SupervisorState

logger = logging.getLogger("migration_agent")


class SupervisorAgent:
    """멀티 에이전트 시스템의 최상위 오케스트레이터."""

    def __init__(self) -> None:
        from server.repositories.migration.repository import (
            get_pending_jobs as get_mig_jobs,
            increment_batch_count as mig_inc,
        )
        from server.agents.migration.orchestrator import MigrationOrchestrator
        from server.repositories.sql.result_repository import (
            get_pending_jobs as get_sql_jobs,
            get_tuning_jobs as get_tuning_jobs_func,
            increment_batch_count as sql_inc,
        )
        from server.agents.sql_conversion.agent import SqlConversionAgent
        from server.agents.sql_tuning.agent import SqlTuningAgent

        dm = MigrationOrchestrator()
        sql_conversion = SqlConversionAgent()
        sql_tuning = SqlTuningAgent()

        self._graph = build_supervisor_graph(
            get_migration_jobs=get_mig_jobs,
            get_sql_jobs=get_sql_jobs,
            get_tuning_jobs=get_tuning_jobs_func,
            mig_increment_batch=mig_inc,
            mig_process_job=dm.process_job,
            sql_increment_batch=sql_inc,
            sql_process_job=sql_conversion.process_job,
            tune_process_job=sql_tuning.process_job,
            logger=logger,
        )

    def run(self) -> None:
        """SIGINT/SIGTERM 을 등록하고 Supervisor 그래프를 실행한다."""
        logger.info("============================================================")
        logger.info(" Multi-Agent Supervisor 시작 (LLM Tool-Calling Mode)")
        logger.info("  ├─ Tool 1: run_data_migration  — 데이터 이관")
        logger.info("  ├─ Tool 2: run_sql_conversion  — SQL 변환")
        logger.info("  └─ Tool 3: run_sql_tuning      — SQL 튜닝")
        logger.info("============================================================")

        self._register_signal_handlers()

        initial_state: SupervisorState = {
            "messages": [],
            "cycle": 0,
            "stop_requested": False,
        }

        try:
            self._graph.invoke(initial_state)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            logger.info("[Supervisor] 모든 에이전트가 종료되었습니다.")

    @staticmethod
    def _register_signal_handlers() -> None:
        signal_count = {"n": 0}

        def _handle(_signum, _frame):
            signal_count["n"] += 1
            request_stop()
            if signal_count["n"] == 1:
                try:
                    msg = "[Supervisor] Stop signal received. Finishing current job...\n"
                    os.write(2, msg.encode("utf-8", errors="ignore"))
                except OSError:
                    pass
            else:
                os._exit(130)

        signal.signal(signal.SIGINT, _handle)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _handle)
