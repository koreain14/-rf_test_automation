# application/run_service_step.py
from __future__ import annotations
from typing import Callable, List, Optional

from application.scheduler import reorder_cases_channel_centric, ChannelCentricPolicy
from application.procedures import ProcedureRegistry
from application.runner_step import StepRunner
from application.step_sink_sqlite import StepResultSinkSQLite
from application.instruments_dummy import DummyInstrument
from infrastructure.run_repo_sqlite import RunRepositorySQLite
from domain.overrides import apply_overrides
from domain.expand import expand_recipe
import traceback
import logging

log = logging.getLogger(__name__)

class RunServiceStep:
    def __init__(self, run_repo: RunRepositorySQLite):
        self.run_repo = run_repo

    def run(
        self,
        project_id: str,
        preset_id: str,
        run_id: str,
        ruleset,
        recipe,
        overrides,
        should_stop: Callable[[], bool],
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> str:
        try:
            # 1) 케이스 생성 + override 적용
            cases_it = apply_overrides(expand_recipe(ruleset, recipe), overrides)

            # 2) execution_policy 적용 (없으면 기본값)
            pol = (recipe.meta or {}).get("execution_policy") or {}
            order = pol.get("test_order") or ["PSD", "OBW", "SP", "RX"]
            include_bw = bool(pol.get("include_bw_in_group", True))
            policy = ChannelCentricPolicy(test_order=order, include_bw_in_group=include_bw)

            cases = reorder_cases_channel_centric(cases_it, policy)

            # 3) Runner/Instrument/Sink 준비
            inst = DummyInstrument()
            sink = StepResultSinkSQLite(self.run_repo, project_id)
            runner = StepRunner(ProcedureRegistry(), sink)

            count = 0
            for case in cases:
                if should_stop():
                    return "ABORTED"

                # 4) result stub 생성
                result_id = self.run_repo.create_result_stub(
                    project_id=project_id,
                    run_id=run_id,
                    row={
                        "test_key": case.key,
                        "tech": ruleset.tech,
                        "regulation": ruleset.regulation,
                        "band": case.band,
                        "standard": case.standard,
                        "test_type": case.test_type,
                        "channel": case.channel,
                        "bw_mhz": case.bw_mhz,
                        "instrument_snapshot": case.instrument,
                        "tags": case.tags,
                    }
                )

                # 5) steps 실행 → ctx.values 리턴
                values = runner.run_case(result_id, case, inst)

                verdict = values.get("verdict", "ERROR")
                self.run_repo.update_result_final(
                    result_id=result_id,
                    status=verdict if verdict in ("PASS", "FAIL", "SKIP", "ERROR") else "ERROR",
                    margin_db=values.get("margin_db"),
                    measured_value=values.get("measured_value"),
                    limit_value=values.get("limit_value"),
                )

                count += 1
                if on_progress and (count % 20 == 0):
                    on_progress(count, verdict)

            return "DONE"
        except Exception:
                
            log.exception("Run failed")            
            raise