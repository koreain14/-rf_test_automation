from __future__ import annotations

import uuid
import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem
from PySide6.QtWidgets import QMessageBox

from application.plan_control_service import PlanControlService
from application.plan_models import PlanFilter, PlanQuery, PlanSortSpec
from application.plan_query_service import PlanQueryService
from ui.execution_order_dialog import ExecutionOrderDialog
from ui.motion_settings_dialog import MotionSettingsDialog
from ui.plan_context import PlanContext
from ui.power_settings_dialog import PowerSettingsDialog
from ui.rf_path_dialog import RFPathDialog


log = logging.getLogger(__name__)


class PlanController:
    def __init__(self, window):
        self.window = window
        self._current_filter = PlanFilter()
        self._group_drill_filter: Optional[PlanFilter] = None
        self._current_page = 1
        self._page_size = 200
        self._visible_rows: List[Dict[str, Any]] = []
        self._current_sort: tuple[PlanSortSpec, ...] = tuple()
        self._query_service = PlanQueryService(window.svc)
        self._control_service = PlanControlService()

        self._ui_bound = False
        self.bind_ui()

    def bind_ui(self) -> None:
        if self._ui_bound:
            return
        pw = getattr(self.window, "plan_widget", None)
        if pw is None:
            return
        pw.btn_prev_page.clicked.connect(self.prev_page)
        pw.btn_next_page.clicked.connect(self.next_page)
        pw.page_size_combo.currentTextChanged.connect(self.set_page_size)
        try:
            pw.page_size_combo.currentIndexChanged.connect(lambda *_: self.set_page_size(pw.page_size_combo.currentText()))
        except Exception:
            pass
        self._ui_bound = True
        log.info("PlanController UI bound | page_size=%s", getattr(self, "_page_size", None))

    # ---------- Plan lifecycle ----------
    def add_plan(self) -> None:
        w = self.window
        if not w.project_id:
            QMessageBox.warning(w, "No project", "Select a project.")
            return
        if not w.preset_id:
            QMessageBox.warning(w, "No preset", "Select a preset first.")
            return
        try:
            ruleset, preset, recipe, overrides = w.svc.build_recipe_from_preset(w.preset_id)
        except Exception as e:
            QMessageBox.critical(w, "Build Plan Failed", str(e))
            return

        plan_id = f"PLAN::{uuid.uuid4().hex[:12]}"
        ctx = PlanContext(
            project_id=w.project_id,
            preset_id=w.preset_id,
            ruleset=ruleset,
            preset=preset,
            recipe=recipe,
            overrides=overrides,
            all_cases=[],
            case_enabled={},
            case_order=[],
            deleted_case_keys=set(),
        )
        w._plans[plan_id] = ctx
        item = self.append_plan_to_tree(plan_id, ctx)
        self.select_tree_node(item)
        total_cases = self._query_service.query_count(
            ctx=ctx,
            query=PlanQuery(filters=PlanFilter(), page=1, page_size=1),
        )
        w.statusBar().showMessage(f"Plan added: {preset.name} ({total_cases} cases)", 5000)

    def reload_plan(self) -> None:
        ctx = self._current_context()
        if not ctx:
            self.clear_cases_view()
            return

        # Reload should remain cache/query-driven. Do not force full hydration of
        # ctx.all_cases here; instead invalidate the repo-backed cache so the next
        # query reseeds from iter_cases(...) and refreshes the UI through the
        # normal query path.
        self._query_service.invalidate_context_cache(
            ctx=ctx,
            reset_case_order=True,
            clear_hydrated_cases=True,
        )
        self._current_page = 1
        self.refresh_plan_tree_order_only(self.current_plan_id())
        self.load_group_summary()
        self.load_detail_page(page=1)

    def _ensure_cases_loaded(self, ctx: Optional[PlanContext] = None, force: bool = False) -> None:
        ctx = ctx or self._current_context()
        if not ctx:
            return
        if ctx.all_cases and not force:
            return
        self._query_service._ensure_context_cases(ctx)

    # ---------- Tree ----------
    def effective_test_order(self, ctx: PlanContext) -> list[str]:
        order = (((ctx.recipe.meta or {}).get("execution_policy") or {}).get("test_order") or [])
        if order:
            return list(order)
        return ["CHANNEL_POWER", "PSD", "OBW", "TX_SPURIOUS", "FE", "RX_SPURIOUS", "RX"]

    def append_plan_to_tree(self, plan_id: str, ctx: PlanContext) -> QStandardItem:
        root = self.window.tree_model.invisibleRootItem()
        parent = QStandardItem(ctx.preset.name)
        parent.setEditable(False)
        parent.setData(plan_id, Qt.UserRole)
        parent.setData({"kind": "plan", "plan_id": plan_id}, Qt.UserRole + 1)
        counts = self._test_counts_for_tree(ctx=ctx)
        for test_type in self.effective_test_order(ctx):
            if test_type not in counts:
                continue
            child = QStandardItem(f"{test_type} ({counts[test_type]})")
            child.setEditable(False)
            child.setData(plan_id, Qt.UserRole)
            child.setData({"kind": "test", "plan_id": plan_id, "test_type": test_type}, Qt.UserRole + 1)
            parent.appendRow(child)
        for test_type, cnt in sorted(counts.items()):
            if test_type in self.effective_test_order(ctx):
                continue
            child = QStandardItem(f"{test_type} ({cnt})")
            child.setEditable(False)
            child.setData(plan_id, Qt.UserRole)
            child.setData({"kind": "test", "plan_id": plan_id, "test_type": test_type}, Qt.UserRole + 1)
            parent.appendRow(child)
        root.appendRow(parent)
        self.window.tree.expand(parent.index())
        return parent

    def refresh_plan_tree_order_only(self, plan_id: str, selected_test_type: str | None = None) -> bool:
        item = self.find_plan_item(plan_id)
        ctx = self.window._plans.get(plan_id)
        if item is None or ctx is None:
            return False
        while item.rowCount() > 0:
            item.removeRow(0)
        counts = self._test_counts_for_tree(ctx=ctx)
        order = self.effective_test_order(ctx)
        for test_type in order:
            if test_type not in counts:
                continue
            child = QStandardItem(f"{test_type} ({counts[test_type]})")
            child.setEditable(False)
            child.setData(plan_id, Qt.UserRole)
            child.setData({"kind": "test", "plan_id": plan_id, "test_type": test_type}, Qt.UserRole + 1)
            item.appendRow(child)
        return True

    def current_plan_id(self) -> str | None:
        return self.window._current_plan_node_id

    def find_plan_item(self, plan_id: str):
        root = self.window.tree_model.invisibleRootItem()
        for row in range(root.rowCount()):
            item = root.child(row)
            if item and item.data(Qt.UserRole) == plan_id:
                return item
        return None

    def remove_plan_item_from_tree(self, plan_id: str) -> bool:
        root = self.window.tree_model.invisibleRootItem()
        for row in range(root.rowCount()):
            item = root.child(row)
            if item and item.data(Qt.UserRole) == plan_id:
                root.removeRow(row)
                return True
        return False

    def remove_plan_from_scenario(self) -> None:
        plan_id = self.current_plan_id()
        if not plan_id:
            return
        self.window._plans.pop(plan_id, None)
        self.remove_plan_item_from_tree(plan_id)
        self.window._current_plan_node_id = None
        self.clear_cases_view()

    def tree_clicked(self, index) -> None:
        item = self.window.tree_model.itemFromIndex(index)
        if item is not None:
            self.select_tree_node(item)

    def select_tree_node(self, item: QStandardItem) -> None:
        meta = item.data(Qt.UserRole + 1) or {}
        plan_id = meta.get("plan_id") or item.data(Qt.UserRole)
        self.window._current_plan_node_id = plan_id
        test_type = meta.get("test_type") if meta.get("kind") == "test" else None
        self.window._tree_filter = {"test_type": test_type} if test_type else None
        self._current_page = 1
        self.load_group_summary()
        self.load_detail_page(page=1)
        self.window.tree.setCurrentIndex(item.index())

    # ---------- Filtering / paging ----------
    def apply_filter(self) -> None:
        self._current_filter = self._read_filter_from_ui()
        self._group_drill_filter = None
        self._current_page = 1
        self.load_group_summary()
        self.load_detail_page(page=1)

    def clear_filter(self) -> None:
        self._clear_filter_ui()
        self._current_filter = PlanFilter()
        self._group_drill_filter = None
        self._current_page = 1
        self.load_group_summary()
        self.load_detail_page(page=1)

    def load_group_summary(self) -> None:
        ctx = self._current_context()
        if not ctx:
            self.window.group_model.clear()
            return
        rows = self._query_service.query_group_summary(ctx=ctx, query=self._build_query(page=1, page_size=1))
        self.window.group_model.set_rows(rows)

    def drill_down_selected_group(self) -> None:
        view = self.window.plan_widget.group_table
        model = self.window.group_model
        idxs = view.selectionModel().selectedRows() if view.selectionModel() else []
        if not idxs:
            return
        row = model.row_at(idxs[0].row())
        if not row:
            return
        self._group_drill_filter = PlanFilter(
            band=row.band,
            standard=row.standard,
            bandwidth_mhz=row.bandwidth_mhz,
            test_type=row.test_type,
            enabled_state=self._current_filter.enabled_state,
            search_text=self._current_filter.search_text,
        )
        self._current_page = 1
        self.load_detail_page(page=1)

    def load_page(self) -> None:
        self.load_detail_page(page=1)

    def load_more(self) -> None:
        self.load_detail_page(page=self._current_page + 1, append=True)

    def load_detail_page(self, page: Optional[int] = None, append: bool = False) -> None:
        log.info("PlanController.load_detail_page | requested_page=%s current_page=%s page_size=%s append=%s", page, self._current_page, self._page_size, append)
        ctx = self._current_context()
        if not ctx:
            self.clear_cases_view()
            return
        if page is not None:
            self._current_page = max(1, int(page))
        result = self._query_service.query_page(ctx=ctx, query=self._build_query(page=self._current_page, page_size=self._page_size))
        log.info("PlanController.load_detail_page | result total=%s returned=%s", result.get("total"), len(result.get("rows") or []))
        rows = result["rows"]
        if append:
            current = self.window.case_model.rows()
            self.window.case_model.set_rows(current + rows)
        else:
            self.window.case_model.set_rows(rows)
        self._visible_rows = self.window.case_model.rows()
        self._set_page_label(result["start_index"], result["end_index"], result["total"])

    def next_page(self) -> None:
        self.load_detail_page(page=self._current_page + 1)

    def prev_page(self) -> None:
        self.load_detail_page(page=max(1, self._current_page - 1))

    def set_page_size(self, value: Any) -> None:
        old = self._page_size
        try:
            parsed = int(value)
        except Exception:
            parsed = 200
        self._page_size = max(1, min(parsed, 5000))
        self._current_page = 1
        log.info("PlanController.set_page_size | old=%s new=%s raw=%r reset_page=1", old, self._page_size, value)
        self.load_detail_page(page=1)

    def clear_cases_view(self) -> None:
        self.window.case_model.clear()
        self.window.group_model.clear()
        self._visible_rows = []
        self._set_page_label(0, 0, 0)

    # ---------- Selection / run ----------
    def selected_case_keys(self) -> List[str]:
        view = self.window.table
        sel = view.selectionModel()
        if sel is None:
            return []
        rows = sorted({idx.row() for idx in sel.selectedRows()})
        keys: List[str] = []
        for row in rows:
            item = self.window.case_model.row_at(row)
            if item:
                keys.append(str(item.get("case_key") or item.get("id") or item.get("key")))
        return keys

    def runnable_case_keys(self, selected_only: bool = False) -> List[str]:
        if selected_only:
            return self.selected_case_keys()
        return self.execution_target_keys(scope="filtered")

    def current_execution_query(self, scope: str = "filtered") -> PlanQuery:
        """
        Public compatibility wrapper exposing the current execution query.

        Scopes:
        - filtered: current UI/tree/group-drill filter across the full filtered set
        - all: whole selected plan, ignoring UI filter / tree node / page state

        Page state must not define execution scope, so execution queries always
        use page=1/page_size=1 and rely on QueryEngine.query_runnable_case_keys().
        """
        normalized = str(scope or "filtered").strip().lower()
        if normalized == "all":
            return PlanQuery(
                filters=PlanFilter(),
                sort=tuple(self._current_sort or ()),
                page=1,
                page_size=1,
                policy={},
            )
        return PlanQuery(
            filters=self._effective_filter(),
            sort=tuple(self._current_sort or ()),
            page=1,
            page_size=1,
            policy={},
        )

    def execution_target_keys(self, scope: str = "filtered") -> List[str]:
        """
        Resolve execution target case keys for the currently selected plan.

        - selected: selected visible rows only
        - filtered: current filtered set across all pages
        - all: entire plan across all pages
        """
        ctx = self._current_context()
        if not ctx:
            return []
        return self.execution_target_keys_for_plan(plan_id=self.current_plan_id() or "", scope=scope)

    def execution_target_keys_for_plan(self, *, plan_id: str, scope: str = "all") -> List[str]:
        """
        Resolve execution target case keys for an arbitrary plan without forcing
        the plan to become the currently selected UI node.

        This compatibility wrapper is used by scenario execution so the run path
        stays query-driven and does not call _ensure_cases_loaded()/ctx.all_cases.
        """
        normalized = str(scope or "all").strip().lower()
        if normalized == "selected":
            current_plan_id = self.current_plan_id()
            if current_plan_id == str(plan_id or ""):
                return self.selected_case_keys()
            return []

        ctx = self.window._plans.get(str(plan_id or ""))
        if not ctx:
            return []

        if normalized == "all":
            query = PlanQuery(
                filters=PlanFilter(),
                sort=tuple(self._current_sort or ()),
                page=1,
                page_size=1,
                policy={},
            )
        else:
            if self.current_plan_id() == str(plan_id or ""):
                query = self.current_execution_query(scope=normalized)
            else:
                query = PlanQuery(
                    filters=PlanFilter(),
                    sort=tuple(self._current_sort or ()),
                    page=1,
                    page_size=1,
                    policy={},
                )

        return self._query_service.query_runnable_case_keys(
            ctx=ctx,
            query=query,
        )

    def skip_selected(self) -> None:
        QMessageBox.information(
            self.window,
            "Filter-Driven Execution",
            "Row-level skip/enable is not used in this version. Use filters, group drill-down, and Run Filtered instead.",
        )

    def run_filtered(self) -> List[str]:
        keys = self.execution_target_keys(scope="filtered")
        if not keys:
            QMessageBox.information(self.window, "No runnable cases", "No enabled cases match the current filter.")
            return []
        self.window._run_controller.start_run_filtered()
        return keys

    # ---------- Plan control dialogs ----------
    def edit_execution_order(self) -> None:
        ctx = self._current_context()
        if not ctx:
            return
        current = self.effective_test_order(ctx)
        dlg = ExecutionOrderDialog(initial_order=current, parent=self.window)
        if dlg.exec():
            order = dlg.get_order()
            try:
                self.window.svc.save_execution_order(ctx.preset_id, order)
                ruleset, preset, recipe, overrides = self.window.svc.build_recipe_from_preset(ctx.preset_id)
                ctx.ruleset = ruleset  # type: ignore[misc]
                ctx.preset = preset  # type: ignore[misc]
                ctx.recipe = recipe  # type: ignore[misc]
                ctx.overrides = overrides  # type: ignore[misc]
            except Exception as e:
                QMessageBox.warning(self.window, "Save failed", str(e))
                return
            self.refresh_plan_tree_order_only(self.current_plan_id())

    def current_switch_path(self) -> str | None:
        ctx = self._current_context()
        return self._control_service.current_switch_path(ctx.recipe) if ctx else None

    def current_antenna(self) -> str | None:
        ctx = self._current_context()
        return self._control_service.current_antenna(ctx.recipe) if ctx else None

    def edit_rf_path(self) -> None:
        ctx = self._current_context()
        if not ctx:
            return
        profile_name = None
        if hasattr(self.window, "_current_equipment_profile_name"):
            try:
                profile_name = self.window._current_equipment_profile_name()
            except Exception:
                profile_name = None
        try:
            im = self.window.run_service.instrument_manager
            path_names = list(im.get_switch_path_names(profile_name))
            antenna_names = list(im.get_switch_port_names(profile_name))
        except Exception:
            path_names = []
            antenna_names = []
        dlg = RFPathDialog(
            path_names,
            antenna_names=antenna_names,
            current_path=self.current_switch_path(),
            current_antenna=self.current_antenna(),
            parent=self.window,
        )
        if dlg.exec():
            ctx.recipe = self._control_service.update_rf_path(ctx.recipe, dlg.selected_path(), dlg.selected_antenna())  # type: ignore[misc]

    def current_power_settings(self) -> dict:
        ctx = self._current_context()
        return self._control_service.current_power(ctx.recipe) if ctx else {}

    def edit_power_settings(self) -> None:
        ctx = self._current_context()
        if not ctx:
            return
        dlg = PowerSettingsDialog(initial=self.current_power_settings(), parent=self.window)
        if dlg.exec():
            ctx.recipe = self._control_service.update_power(ctx.recipe, dlg.settings())  # type: ignore[misc]

    def current_motion_settings(self) -> dict:
        ctx = self._current_context()
        return self._control_service.current_motion(ctx.recipe) if ctx else {}

    def edit_motion_settings(self) -> None:
        ctx = self._current_context()
        if not ctx:
            return
        dlg = MotionSettingsDialog(initial=self.current_motion_settings(), parent=self.window)
        if dlg.exec():
            ctx.recipe = self._control_service.update_motion(ctx.recipe, dlg.settings())  # type: ignore[misc]

    def build_plan_control_summary(self) -> str:
        ctx = self._current_context()
        if not ctx:
            return "No plan selected."
        return self._control_service.build_summary(ctx.preset.name, ctx.recipe, self.effective_test_order(ctx))

    def show_plan_summary(self) -> None:
        QMessageBox.information(self.window, "Plan Summary", self.build_plan_control_summary())

    # ---------- Internal ----------
    def _current_context(self) -> Optional[PlanContext]:
        plan_id = self.current_plan_id()
        return self.window._plans.get(plan_id) if plan_id else None

    def _read_filter_from_ui(self) -> PlanFilter:
        pw = self.window.plan_widget

        def _txt(obj):
            if hasattr(obj, "currentText"):
                return str(obj.currentText() or "").strip()
            return str(obj.text() or "").strip()

        def _int_or_none(obj):
            s = _txt(obj)
            if not s:
                return None
            try:
                return int(s)
            except Exception:
                return None

        return PlanFilter(
            band=_txt(pw.plan_filter_band),
            standard=_txt(pw.plan_filter_standard),
            bandwidth_mhz=_int_or_none(pw.plan_filter_bw),
            test_type=_txt(pw.plan_filter_test),
            channel_from=_int_or_none(pw.plan_filter_channel_from),
            channel_to=_int_or_none(pw.plan_filter_channel_to),
            enabled_state=_txt(pw.plan_filter_enabled) or "ALL",
            search_text=_txt(pw.plan_filter_search),
        )

    def _clear_filter_ui(self) -> None:
        pw = self.window.plan_widget
        for combo in (pw.plan_filter_band, pw.plan_filter_standard, pw.plan_filter_bw, pw.plan_filter_test, pw.plan_filter_enabled):
            combo.setCurrentIndex(0)
        for le in (pw.plan_filter_channel_from, pw.plan_filter_channel_to, pw.plan_filter_search):
            le.clear()


    def _build_query(self, *, page: Optional[int] = None, page_size: Optional[int] = None) -> PlanQuery:
        """
        Build the single source-of-truth query for detail / summary / runnable flows.

        Controller only composes current UI state into a query object. Actual
        filtering, sorting, paging, and grouping remain inside the query service
        and repository layers.
        """
        resolved_page = self._current_page if page is None else max(1, int(page))
        try:
            resolved_page_size = self._page_size if page_size is None else int(page_size)
        except Exception:
            resolved_page_size = self._page_size
        resolved_page_size = max(1, min(int(resolved_page_size or 200), 5000))
        return PlanQuery(
            filters=self._effective_filter(),
            sort=tuple(self._current_sort or ()),
            page=resolved_page,
            page_size=resolved_page_size,
            policy={},
        )

    def _test_counts_for_tree(self, *, ctx: PlanContext) -> Dict[str, int]:
        """
        Compatibility wrapper for tree count rendering.

        Tree counts represent the whole plan and therefore intentionally use an
        empty filter. Aggregation is delegated to the QueryService/QueryEngine
        path so the controller does not full-load rows or perform direct
        counting.
        """
        return self._query_service.context_test_counts(
            ctx=ctx,
            plan_filter=PlanFilter(),
        )

    def _effective_filter(self) -> PlanFilter:
        base = self._current_filter
        if self.window._tree_filter and self.window._tree_filter.get("test_type"):
            base = PlanFilter(
                band=base.band,
                standard=base.standard,
                phy_mode=base.phy_mode,
                bandwidth_mhz=base.bandwidth_mhz,
                channel_from=base.channel_from,
                channel_to=base.channel_to,
                test_type=self.window._tree_filter["test_type"],
                enabled_state=base.enabled_state,
                search_text=base.search_text,
            )
        if not self._group_drill_filter:
            return base
        gd = self._group_drill_filter
        return PlanFilter(
            band=gd.band or base.band,
            standard=gd.standard or base.standard,
            phy_mode=base.phy_mode,
            bandwidth_mhz=gd.bandwidth_mhz if gd.bandwidth_mhz not in (None, "") else base.bandwidth_mhz,
            channel_from=base.channel_from,
            channel_to=base.channel_to,
            test_type=gd.test_type or base.test_type,
            enabled_state=base.enabled_state,
            search_text=base.search_text,
        )

    def _set_page_label(self, start: int, end: int, total: int) -> None:
        self.window.plan_widget.page_label.setText(f"Showing {start}-{end} / {total}")
