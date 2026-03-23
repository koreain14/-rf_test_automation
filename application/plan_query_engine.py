from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import hashlib
import logging

from application.execution_policy import apply_execution_policy, execution_policy_from_meta, ordering_policy_from_meta, sort_rows
from application.plan_models import PlanFilter, PlanGroupSummary, PlanQuery, PlanQueryResult, PlanSortSpec

log = logging.getLogger(__name__)


class PlanQueryEngine:
    """Single query entry point for plan detail/summary/count/runnable queries."""

    def __init__(self, plan_service: Any):
        self.plan_service = plan_service

    def execute_query(self, *, ctx: Any, query: PlanQuery) -> PlanQueryResult:
        repo = getattr(self.plan_service, "repo", None)
        cache_key = self._sync_context_cache(ctx=ctx)
        if repo is not None and cache_key:
            result = self._execute_repo_query(ctx=ctx, cache_key=cache_key, query=query)
            log.info(
                "plan_query_engine.execute_query | source=repo cache_key=%s page=%s page_size=%s total=%s rows=%s summary=%s runnable=%s",
                cache_key,
                query.page,
                query.page_size,
                result.total,
                len(result.rows),
                len(result.summary),
                len(result.runnable_case_keys),
            )
            return result
        result = self._execute_fallback_query(ctx=ctx, query=query)
        log.info(
            "plan_query_engine.execute_query | source=fallback page=%s page_size=%s total=%s rows=%s summary=%s runnable=%s",
            query.page,
            query.page_size,
            result.total,
            len(result.rows),
            len(result.summary),
            len(result.runnable_case_keys),
        )
        return result

    def query_group_summary(self, *, ctx: Any, query: PlanQuery) -> List[PlanGroupSummary]:
        return self.execute_query(ctx=ctx, query=query).summary

    def query_count(self, *, ctx: Any, query: PlanQuery) -> int:
        return self.execute_query(ctx=ctx, query=query).total

    def query_page(self, *, ctx: Any, query: PlanQuery) -> Dict[str, Any]:
        return self.execute_query(ctx=ctx, query=query).to_page_dict()

    def query_runnable_case_keys(self, *, ctx: Any, query: PlanQuery) -> List[str]:
        return self.execute_query(ctx=ctx, query=query).runnable_case_keys

    def context_rows(self, *, ctx: Any, plan_filter: Optional[PlanFilter] = None, include_deleted: bool = False) -> List[Dict[str, Any]]:
        rows = self._build_context_rows(ctx=ctx, include_deleted=include_deleted)
        rows = [r for r in rows if self._matches_filter(case=r, plan_filter=plan_filter)]
        execution_policy = execution_policy_from_meta(getattr(ctx.recipe, "meta", {}) if ctx else {})
        ordering_policy = ordering_policy_from_meta(getattr(ctx.recipe, "meta", {}) if ctx else {})
        return sort_rows(apply_execution_policy(rows, execution_policy), ordering_policy)

    def _execute_repo_query(self, *, ctx: Any, cache_key: str, query: PlanQuery) -> PlanQueryResult:
        repo = getattr(self.plan_service, "repo", None)
        plan_filter = self._query_filter_dict(query=query)
        order_by = self._query_order_sql(ctx=ctx, query=query)
        total = repo.query_plan_case_count_by_query(cache_key=cache_key, query=query, plan_filter=plan_filter)
        rows = repo.query_plan_case_page_by_query(cache_key=cache_key, query=query, plan_filter=plan_filter, order_by=order_by)
        summary_rows = repo.query_plan_case_group_summary_by_query(cache_key=cache_key, query=query, plan_filter=plan_filter)
        runnable_case_keys = repo.query_plan_case_runnable_keys_by_query(cache_key=cache_key, query=query, plan_filter=plan_filter, order_by=order_by)
        start, end = self._page_range(page=query.page, page_size=query.page_size, total=total, returned=len(rows))
        return PlanQueryResult(
            query=query,
            rows=rows,
            total=total,
            summary=[PlanGroupSummary(**row) for row in summary_rows],
            runnable_case_keys=list(runnable_case_keys),
            start_index=start,
            end_index=end,
        )

    def _execute_fallback_query(self, *, ctx: Any, query: PlanQuery) -> PlanQueryResult:
        rows = self.context_rows(ctx=ctx, plan_filter=query.filters)
        total = len(rows)
        start_index, end_index = self._page_range(page=query.page, page_size=query.page_size, total=total, returned=min(query.page_size, max(0, total - (max(1, query.page) - 1) * max(1, query.page_size))))
        start_offset = max(0, (max(1, int(query.page or 1)) - 1) * max(1, int(query.page_size or 200)))
        page_rows = rows[start_offset:start_offset + max(1, int(query.page_size or 200))]
        runnable_case_keys = [str(row.get("case_key") or row.get("id") or row.get("key")) for row in rows if bool(row.get("enabled", True)) and not bool(row.get("excluded", False))]
        return PlanQueryResult(
            query=query,
            rows=page_rows,
            total=total,
            summary=self._summaries_from_iterable(rows),
            runnable_case_keys=runnable_case_keys,
            start_index=0 if total == 0 else start_offset + 1,
            end_index=min(start_offset + len(page_rows), total),
        )

    def _page_range(self, *, page: int, page_size: int, total: int, returned: int) -> tuple[int, int]:
        page = max(1, int(page or 1))
        page_size = max(1, min(int(page_size or 200), 5000))
        if total <= 0 or returned <= 0:
            return 0, 0
        start = (page - 1) * page_size + 1
        end = min(total, start - 1 + returned)
        return start, end

    def _query_filter_dict(self, *, query: PlanQuery) -> Dict[str, Any]:
        return query.filters.to_filter_dict() if query.filters else {}

    def _query_order_sql(self, *, ctx: Any, query: PlanQuery) -> str:
        if query.sort:
            return self._sort_specs_to_sql(sort_specs=list(query.sort), ctx=ctx)
        return self._ordering_policy_to_sql(policy=ordering_policy_from_meta(getattr(ctx.recipe, "meta", {}) if ctx else {}))

    def _sort_specs_to_sql(self, *, sort_specs: List[PlanSortSpec], ctx: Any) -> str:
        priority = list(ordering_policy_from_meta(getattr(ctx.recipe, "meta", {}) if ctx else {}).test_priority or [])
        fields: List[str] = []
        for spec in sort_specs:
            field = str(spec.field or "").strip()
            direction = "DESC" if spec.normalized_direction() == "desc" else "ASC"
            sql_field = {
                "band": "band",
                "standard": "standard",
                "phy_mode": "phy_mode",
                "bandwidth_mhz": "bandwidth_mhz",
                "channel": "channel",
                "frequency_mhz": "frequency_mhz",
            }.get(field)
            if sql_field:
                fields.append(f"{sql_field} {direction}")
                continue
            if field == "test_type" and priority:
                cases = " ".join([f"WHEN '{name}' THEN {idx}" for idx, name in enumerate(priority)])
                fields.append(f"CASE test_type {cases} ELSE 999 END {direction}")
        if not any("sort_index" in part for part in fields):
            fields.append("sort_index ASC")
        return ", ".join(fields)

    def _sync_context_cache(self, *, ctx: Any) -> Optional[str]:
        repo = getattr(self.plan_service, "repo", None)
        if ctx is None or repo is None:
            return None
        cache_key = getattr(ctx, "_cache_key", None)
        if not cache_key:
            cache_key = f"{ctx.project_id}:{ctx.preset_id}:{id(ctx)}"
            setattr(ctx, "_cache_key", cache_key)
        self._ensure_context_cache_seeded(ctx=ctx, cache_key=cache_key)
        state_token = self._build_context_state_token(ctx=ctx)
        if getattr(ctx, "_cache_state_token", None) == state_token:
            return cache_key
        rows = self._build_context_rows(ctx=ctx, include_deleted=True)
        repo.sync_plan_case_cache(cache_key=cache_key, project_id=ctx.project_id, preset_id=ctx.preset_id, rows=rows)
        setattr(ctx, "_cache_state_token", state_token)
        return cache_key


    def invalidate_context_cache(self, *, ctx: Any, reset_case_order: bool = False, clear_hydrated_cases: bool = True) -> None:
        """
        Invalidate the repo-backed cache for a context without forcing full TestCase hydration.

        Intended for reload/refresh flows where we want the next query to reseed
        ``plan_case_cache`` from ``iter_cases(...)`` and refresh the UI via the
        normal query path. This keeps reload cache/query-driven instead of
        forcing ``ctx.all_cases`` as the default source.
        """
        if ctx is None:
            return
        repo = getattr(self.plan_service, "repo", None)
        cache_key = getattr(ctx, "_cache_key", None)
        if repo is not None and cache_key:
            try:
                repo.clear_plan_case_cache(cache_key=cache_key)
            except Exception:
                log.exception("plan_query_engine.invalidate_context_cache | clear_cache_failed cache_key=%s", cache_key)
        if hasattr(ctx, "_cache_state_token"):
            setattr(ctx, "_cache_state_token", None)
        if reset_case_order:
            try:
                ctx.case_order = []
            except Exception:
                pass
        if clear_hydrated_cases:
            try:
                ctx.all_cases = []
            except Exception:
                pass

    def _build_context_state_token(self, *, ctx: Any) -> tuple[Any, ...]:
        """
        Build a cache synchronization token without depending on ctx.all_cases.

        Query/state synchronization should be driven by cache-row overlays
        (order/enabled/excluded/deleted/priority/meta), not by whether a full
        in-memory TestCase list has been hydrated for a compatibility path.
        """
        case_order = list(getattr(ctx, "case_order", []) or [])
        case_enabled = dict(getattr(ctx, "case_enabled", {}) or {})
        deleted_case_keys = set(getattr(ctx, "deleted_case_keys", set()) or set())
        excluded_case_keys = set(getattr(ctx, "case_excluded", set()) or set())
        priority_tags = dict(getattr(ctx, "case_priority_tags", {}) or {})
        return (
            len(case_order),
            self._sequence_digest(case_order),
            len(case_enabled),
            self._mapping_digest(case_enabled),
            len(deleted_case_keys),
            self._set_digest(deleted_case_keys),
            len(excluded_case_keys),
            self._set_digest(excluded_case_keys),
            len(priority_tags),
            self._mapping_digest(priority_tags),
            str(getattr(getattr(ctx, "recipe", None), "meta", {}) or {}),
        )

    def _build_context_rows(self, *, ctx: Any, include_deleted: bool = False) -> List[Dict[str, Any]]:
        """
        Build query rows with a cache-row-first policy.

        If repository cache is available, query/state synchronization should not
        depend on ctx.all_cases or full TestCase hydration. Full object fallback
        is retained only for compatibility when repo/cache is unavailable.
        """
        repo = getattr(self.plan_service, "repo", None)
        cache_key = getattr(ctx, "_cache_key", None) if ctx else None
        if repo is not None and cache_key:
            rows = self._build_context_rows_from_cache(ctx=ctx, cache_key=cache_key, include_deleted=include_deleted)
            if rows:
                return rows

        self._ensure_context_cases(ctx)
        by_key = {c.key: c for c in getattr(ctx, "all_cases", [])}
        ordered_keys = list(getattr(ctx, "case_order", []) or [c.key for c in getattr(ctx, "all_cases", [])])
        deleted_case_keys = set(getattr(ctx, "deleted_case_keys", set()) or set())
        excluded_case_keys = set(getattr(ctx, "case_excluded", set()) or set())
        priority_tags = dict(getattr(ctx, "case_priority_tags", {}) or {})
        case_enabled = dict(getattr(ctx, "case_enabled", {}) or {})
        rows: List[Dict[str, Any]] = []
        for idx, key in enumerate(ordered_keys):
            case = by_key.get(key)
            if case is None:
                continue
            deleted = key in deleted_case_keys
            if deleted and not include_deleted:
                continue
            rows.append({
                "case_key": case.key,
                "id": case.key,
                "key": case.key,
                "test_type": case.test_type,
                "band": case.band,
                "standard": case.standard,
                "channel": case.channel,
                "frequency_mhz": case.center_freq_mhz,
                "center_freq_mhz": case.center_freq_mhz,
                "bandwidth_mhz": case.bw_mhz,
                "bw_mhz": case.bw_mhz,
                "phy_mode": str(case.tags.get("phy_mode", "")),
                "mode": str(case.tags.get("phy_mode", "")),
                "tech": str(getattr(getattr(ctx, "recipe", None), "tech", "") or ""),
                "regulation": str(getattr(getattr(ctx, "recipe", None), "regulation", "") or ""),
                "group_name": str(case.tags.get("group", "") or ""),
                "enabled": bool(case_enabled.get(case.key, True)),
                "excluded": case.key in excluded_case_keys,
                "deleted": deleted,
                "priority_tag": priority_tags.get(case.key, ""),
                "sort_index": idx,
            })
        return rows

    def _build_context_rows_from_cache(self, *, ctx: Any, cache_key: str, include_deleted: bool = False) -> List[Dict[str, Any]]:
        repo = getattr(self.plan_service, "repo", None)
        if repo is None:
            return []
        base_rows = repo.list_plan_case_cache_rows(cache_key=cache_key)
        if not base_rows:
            return []
        ordered_keys = list(getattr(ctx, "case_order", []) or [str(r.get("case_key") or "") for r in base_rows])
        deleted_case_keys = set(getattr(ctx, "deleted_case_keys", set()) or set())
        excluded_case_keys = set(getattr(ctx, "case_excluded", set()) or set())
        priority_tags = dict(getattr(ctx, "case_priority_tags", {}) or {})
        case_enabled = dict(getattr(ctx, "case_enabled", {}) or {})
        by_key = {str(r.get("case_key") or ""): dict(r) for r in base_rows}
        rows: List[Dict[str, Any]] = []
        for idx, key in enumerate(ordered_keys):
            row = by_key.get(str(key))
            if row is None:
                continue
            deleted = str(key) in deleted_case_keys or bool(row.get("deleted", False))
            if deleted and not include_deleted:
                continue
            rows.append({
                "case_key": str(row.get("case_key") or key),
                "id": str(row.get("case_key") or key),
                "key": str(row.get("case_key") or key),
                "test_type": str(row.get("test_type", "") or ""),
                "band": str(row.get("band", "") or ""),
                "standard": str(row.get("standard", "") or ""),
                "channel": int(row.get("channel", 0) or 0),
                "frequency_mhz": float(row.get("frequency_mhz", row.get("center_freq_mhz", 0)) or 0),
                "center_freq_mhz": float(row.get("center_freq_mhz", row.get("frequency_mhz", 0)) or 0),
                "bandwidth_mhz": int(row.get("bandwidth_mhz", row.get("bw_mhz", 0)) or 0),
                "bw_mhz": int(row.get("bw_mhz", row.get("bandwidth_mhz", 0)) or 0),
                "phy_mode": str(row.get("phy_mode", row.get("mode", "")) or ""),
                "mode": str(row.get("mode", row.get("phy_mode", "")) or ""),
                "tech": str(row.get("tech", "") or ""),
                "regulation": str(row.get("regulation", "") or ""),
                "group_name": str(row.get("group_name", "") or ""),
                "enabled": bool(case_enabled.get(str(key), bool(row.get("enabled", True)))),
                "excluded": str(key) in excluded_case_keys or bool(row.get("excluded", False)),
                "deleted": deleted,
                "priority_tag": priority_tags.get(str(key), str(row.get("priority_tag", "") or "")),
                "sort_index": idx,
            })
        return rows

    def _ensure_context_cache_seeded(self, *, ctx: Any, cache_key: str) -> None:
        repo = getattr(self.plan_service, "repo", None)
        if ctx is None or repo is None:
            return
        existing_count = repo.count_plan_case_cache_rows(cache_key=cache_key)
        if existing_count > 0:
            if not getattr(ctx, "case_order", None):
                cached_rows = repo.list_plan_case_cache_rows(cache_key=cache_key)
                ctx.case_order = [str(r.get("case_key") or "") for r in cached_rows]
                case_enabled = getattr(ctx, "case_enabled", None)
                if case_enabled is not None:
                    for row in cached_rows:
                        key = str(row.get("case_key") or "")
                        case_enabled.setdefault(key, bool(row.get("enabled", True)))
            return

        case_order: List[str] = []
        case_enabled = getattr(ctx, "case_enabled", None)

        def _iter_rows() -> Iterable[Dict[str, Any]]:
            for idx, case in enumerate(self.plan_service.iter_cases(ctx.ruleset, ctx.recipe, ctx.overrides)):
                case_order.append(case.key)
                if case_enabled is not None:
                    case_enabled.setdefault(case.key, True)
                yield {
                    "case_key": case.key,
                    "id": case.key,
                    "key": case.key,
                    "test_type": case.test_type,
                    "band": case.band,
                    "standard": case.standard,
                    "channel": case.channel,
                    "frequency_mhz": case.center_freq_mhz,
                    "center_freq_mhz": case.center_freq_mhz,
                    "bandwidth_mhz": case.bw_mhz,
                    "bw_mhz": case.bw_mhz,
                    "phy_mode": str(case.tags.get("phy_mode", "")),
                    "mode": str(case.tags.get("phy_mode", "")),
                    "tech": str(getattr(getattr(ctx, "recipe", None), "tech", "") or ""),
                    "regulation": str(getattr(getattr(ctx, "recipe", None), "regulation", "") or ""),
                    "group_name": str(case.tags.get("group", "") or ""),
                    "enabled": True,
                    "excluded": False,
                    "deleted": False,
                    "priority_tag": "",
                    "sort_index": idx,
                }

        repo.rebuild_plan_case_cache_from_iterable(
            cache_key=cache_key,
            project_id=ctx.project_id,
            preset_id=ctx.preset_id,
            rows=_iter_rows(),
        )
        if not getattr(ctx, "case_order", None):
            ctx.case_order = list(case_order)

    def _ensure_context_cases(self, ctx: Any) -> None:
        """
        Compatibility fallback only.

        Query paths should prefer repo/cache rows and avoid hydrating ctx.all_cases.
        This method remains for legacy/debug/export paths when repository cache is
        unavailable or an explicit full-object path is still required.
        """
        if not ctx or getattr(ctx, "all_cases", None):
            return
        log.warning("plan_query_engine._ensure_context_cases | compatibility_fallback=hydrate_all_cases")
        ctx.all_cases = list(self.plan_service.iter_cases(ctx.ruleset, ctx.recipe, ctx.overrides))
        if not getattr(ctx, "case_order", None):
            ctx.case_order = [c.key for c in ctx.all_cases]
        case_enabled = getattr(ctx, "case_enabled", None)
        if case_enabled is not None:
            for c in ctx.all_cases:
                case_enabled.setdefault(c.key, True)

    def _sequence_digest(self, values: List[Any]) -> str:
        h = hashlib.sha1()
        for value in values:
            h.update(str(value).encode("utf-8", errors="ignore"))
            h.update(b"\0")
        return h.hexdigest()

    def _mapping_digest(self, values: Dict[Any, Any]) -> str:
        h = hashlib.sha1()
        for key in sorted(values.keys(), key=lambda v: str(v)):
            h.update(str(key).encode("utf-8", errors="ignore"))
            h.update(b"=")
            h.update(str(values[key]).encode("utf-8", errors="ignore"))
            h.update(b"\0")
        return h.hexdigest()

    def _set_digest(self, values: set[Any]) -> str:
        h = hashlib.sha1()
        for value in sorted(values, key=lambda v: str(v)):
            h.update(str(value).encode("utf-8", errors="ignore"))
            h.update(b"\0")
        return h.hexdigest()

    def _summaries_from_iterable(self, iterable: List[Dict[str, Any]]) -> List[PlanGroupSummary]:
        buckets: Dict[tuple[str, str, int, str], Dict[str, int]] = {}
        for case in iterable:
            key = (str(case.get("band", "") or ""), str(case.get("standard", "") or ""), int(case.get("bandwidth_mhz", 0) or 0), str(case.get("test_type", "") or ""))
            state = buckets.setdefault(key, {"total": 0, "enabled": 0, "disabled": 0})
            state["total"] += 1
            if bool(case.get("enabled", True)):
                state["enabled"] += 1
            else:
                state["disabled"] += 1
        summaries = [
            PlanGroupSummary(
                band=k[0],
                standard=k[1],
                bandwidth_mhz=k[2],
                test_type=k[3],
                total_count=v["total"],
                enabled_count=v["enabled"],
                disabled_count=v["disabled"],
            )
            for k, v in buckets.items()
        ]
        summaries.sort(key=lambda s: (s.band, s.standard, s.bandwidth_mhz, s.test_type))
        return summaries

    def _matches_filter(self, *, case: Dict[str, Any], plan_filter: Optional[PlanFilter]) -> bool:
        if not plan_filter:
            return True
        def _text(v: Any) -> str:
            return str(v or "").strip()
        ruleset_id = getattr(plan_filter, "ruleset_id", "")
        if ruleset_id and _text(case.get("ruleset_id")) != _text(ruleset_id):
            return False
        if plan_filter.band and _text(case.get("band")) != _text(plan_filter.band):
            return False
        if plan_filter.standard and _text(case.get("standard")) != _text(plan_filter.standard):
            return False
        if plan_filter.phy_mode and _text(case.get("phy_mode")) != _text(plan_filter.phy_mode):
            return False
        if plan_filter.test_type and _text(case.get("test_type")).upper() != _text(plan_filter.test_type).upper():
            return False
        if plan_filter.bandwidth_mhz not in (None, ""):
            if int(case.get("bandwidth_mhz", 0) or 0) != int(plan_filter.bandwidth_mhz):
                return False
        if plan_filter.channel_from not in (None, "") and int(case.get("channel", 0) or 0) < int(plan_filter.channel_from):
            return False
        if plan_filter.channel_to not in (None, "") and int(case.get("channel", 0) or 0) > int(plan_filter.channel_to):
            return False
        enabled_state = str(getattr(plan_filter, "enabled_state", "ALL") or "ALL").upper()
        enabled = bool(case.get("enabled", True))
        if enabled_state == "ENABLED" and not enabled:
            return False
        if enabled_state == "DISABLED" and enabled:
            return False
        search = _text(getattr(plan_filter, "search_text", "")).lower()
        if search:
            hay = " ".join(_text(case.get(k)) for k in ("ruleset_id", "band", "standard", "phy_mode", "test_type", "channel", "frequency_mhz", "bandwidth_mhz", "case_key")).lower()
            if search not in hay:
                return False
        return True

    def _ordering_policy_to_sql(self, *, policy: Any) -> str:
        fields = []
        for field in getattr(policy, "order_by", ()):  # pragma: no branch
            if field == "test_type":
                continue
            sql_field = {
                "bandwidth_mhz": "bandwidth_mhz",
                "channel": "channel",
                "band": "band",
                "standard": "standard",
                "phy_mode": "phy_mode",
                "frequency_mhz": "frequency_mhz",
            }.get(field)
            if sql_field:
                fields.append(f"{sql_field} ASC")
        test_priority = list(getattr(policy, "test_priority", ()) or [])
        if test_priority:
            cases = " ".join([f"WHEN '{name}' THEN {idx}" for idx, name in enumerate(test_priority)])
            fields.append(f"CASE test_type {cases} ELSE 999 END ASC")
        fields.append("sort_index ASC")
        return ", ".join(fields)
