from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from application.plan_models import PlanFilter, PlanGroupSummary, PlanQuery
from application.plan_query_engine import PlanQueryEngine


class PlanQueryService:
    """Compatibility wrapper around the single PlanQueryEngine."""

    def __init__(self, plan_service: Any):
        self.plan_service = plan_service
        self._engine = PlanQueryEngine(plan_service=plan_service)

    # ---------- New single-query API ----------
    def execute_query(self, *, ctx: Any, query: PlanQuery):
        return self._engine.execute_query(ctx=ctx, query=query)

    def query_group_summary(self, *, ctx: Any, query: PlanQuery) -> List[PlanGroupSummary]:
        return self._engine.query_group_summary(ctx=ctx, query=query)

    def query_count(self, *, ctx: Any, query: PlanQuery) -> int:
        return self._engine.query_count(ctx=ctx, query=query)

    def query_page(self, *, ctx: Any, query: PlanQuery) -> Dict[str, Any]:
        return self._engine.query_page(ctx=ctx, query=query)

    def query_runnable_case_keys(self, *, ctx: Any, query: PlanQuery) -> List[str]:
        return self._engine.query_runnable_case_keys(ctx=ctx, query=query)

    # ---------- Legacy/service-backed query helpers ----------
    def count_filtered(self, plan_id: Any, plan_filter: Optional[PlanFilter] = None) -> int:
        return sum(1 for _ in self._iter_filtered_cases(plan_id=plan_id, plan_filter=plan_filter))

    def get_group_summary(self, plan_id: Any, plan_filter: Optional[PlanFilter] = None) -> List[PlanGroupSummary]:
        return self._summaries_from_iterable(self._iter_filtered_cases(plan_id=plan_id, plan_filter=plan_filter))

    def get_detail_page(self, plan_id: Any, plan_filter: Optional[PlanFilter] = None, page: int = 1, page_size: int = 200) -> Dict[str, Any]:
        return self._page_from_iterable(self._iter_filtered_cases(plan_id=plan_id, plan_filter=plan_filter), page=page, page_size=page_size)

    def build_filtered_case_keys(self, plan_id: Any, plan_filter: Optional[PlanFilter] = None, enabled_only: bool = True) -> List[Any]:
        keys: List[Any] = []
        for case in self._iter_filtered_cases(plan_id=plan_id, plan_filter=plan_filter):
            if enabled_only and not bool(case.get("enabled", True)):
                continue
            keys.append(case.get("case_key", case.get("id")))
        return keys

    # ---------- In-memory PlanContext query helpers ----------
    def context_rows(self, ctx: Any, plan_filter: Optional[PlanFilter] = None, include_deleted: bool = False) -> List[Dict[str, Any]]:
        return self._engine.context_rows(ctx=ctx, plan_filter=plan_filter, include_deleted=include_deleted)

    def context_group_summary(self, ctx: Any, plan_filter: Optional[PlanFilter] = None) -> List[PlanGroupSummary]:
        return self.query_group_summary(ctx=ctx, query=PlanQuery(filters=plan_filter or PlanFilter(), page=1, page_size=1))

    def context_detail_page(self, ctx: Any, plan_filter: Optional[PlanFilter] = None, page: int = 1, page_size: int = 200) -> Dict[str, Any]:
        return self.query_page(ctx=ctx, query=PlanQuery(filters=plan_filter or PlanFilter(), page=page, page_size=page_size))

    def context_runnable_case_keys(self, ctx: Any, plan_filter: Optional[PlanFilter] = None) -> List[str]:
        return self.query_runnable_case_keys(ctx=ctx, query=PlanQuery(filters=plan_filter or PlanFilter(), page=1, page_size=1))

    def context_test_counts(self, *, ctx: Any, plan_filter: Optional[PlanFilter] = None) -> Dict[str, int]:
        """
        Compatibility wrapper for tree/test count UI.

        Uses the same Query Engine path as summary/detail/runnable queries so
        tree counts stay aligned with the query model without full-loading
        detail rows in the controller.
        """
        summary = self.query_group_summary(
            ctx=ctx,
            query=PlanQuery(filters=plan_filter or PlanFilter(), page=1, page_size=1),
        )
        counts: Dict[str, int] = {}
        for row in summary:
            test_type = str(getattr(row, "test_type", "") or "")
            if not test_type:
                continue
            counts[test_type] = counts.get(test_type, 0) + int(getattr(row, "total_count", 0) or 0)
        return counts


    def invalidate_context_cache(self, *, ctx: Any, reset_case_order: bool = False, clear_hydrated_cases: bool = True) -> None:
        self._engine.invalidate_context_cache(
            ctx=ctx,
            reset_case_order=reset_case_order,
            clear_hydrated_cases=clear_hydrated_cases,
        )

    def _ensure_context_cases(self, ctx: Any) -> None:
        """Compatibility bridge for existing controller call sites."""
        self._engine._ensure_context_cases(ctx)

    # ---------- Legacy internals ----------
    def _summaries_from_iterable(self, iterable: Iterable[Dict[str, Any]]) -> List[PlanGroupSummary]:
        return self._engine._summaries_from_iterable(list(iterable))

    def _page_from_iterable(self, iterable: Iterable[Dict[str, Any]], page: int, page_size: int) -> Dict[str, Any]:
        rows = list(iterable)
        total = len(rows)
        start = max(0, (max(1, int(page or 1)) - 1) * max(1, int(page_size or 200)))
        selected_rows = rows[start:start + max(1, int(page_size or 200))]
        return {
            "page": max(1, int(page or 1)),
            "page_size": max(1, min(int(page_size or 200), 5000)),
            "total": total,
            "rows": selected_rows,
            "start_index": 0 if total == 0 else start + 1,
            "end_index": min(start + len(selected_rows), total),
        }

    def _iter_filtered_cases(self, *, plan_id: Any, plan_filter: Optional[PlanFilter]) -> Iterable[Dict[str, Any]]:
        for case in self._iter_plan_cases(plan_id=plan_id):
            if self._engine._matches_filter(case=case, plan_filter=plan_filter):
                yield case

    def _iter_plan_cases(self, *, plan_id: Any) -> Iterable[Dict[str, Any]]:
        if hasattr(self.plan_service, "iter_cases"):
            yield from self.plan_service.iter_cases(plan_id)
            return
        if hasattr(self.plan_service, "list_cases"):
            for case in self.plan_service.list_cases(plan_id):
                yield case
            return
        if hasattr(self.plan_service, "get_cases"):
            for case in self.plan_service.get_cases(plan_id):
                yield case
            return
        raise AttributeError("Plan service does not expose iter_cases/list_cases/get_cases")
