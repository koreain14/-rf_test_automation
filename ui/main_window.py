from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QAction, QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QComboBox, QHBoxLayout, QLabel, QMainWindow, QMessageBox, QPushButton, QCheckBox,
    QSplitter, QToolBar, QTreeView, QTableView, QVBoxLayout, QWidget, QTabWidget, QListWidget, QLineEdit
)

from application.plan_service import PlanService
from domain.models import OverrideRule, Preset, Recipe, RuleSet
from ui.table_model import CaseTableModel
from ui.results_table_model import ResultsTableModel
from ui.execution_order_dialog import ExecutionOrderDialog
from ui.step_log_model import StepLogModel



@dataclass
class PlanContext:
    project_id: str
    preset_id: str
    ruleset: RuleSet
    preset: Preset
    recipe: Recipe
    overrides: List[OverrideRule]


class MainWindow(QMainWindow):
    PAGE_SIZE = 200

    def __init__(self, plan_service: PlanService, run_repo, run_service):
        super().__init__()
        self.svc = plan_service
        
        self.run_repo = run_repo
        self.run_service = run_service

        self.setWindowTitle("RF Test Platform (Prototype)")
        self.resize(1200, 800)

        self.project_id: Optional[str] = None
        self.preset_id: Optional[str] = None

        self._plans: Dict[str, PlanContext] = {}  # plan_node_id -> PlanContext
        self._current_plan_node_id: Optional[str] = None
        self._current_filter: Optional[Dict[str, Any]] = None
        self._current_offset: int = 0

        self._build_ui()
        self._load_initial_data()
        
        self._worker = None
        self._last_run_id = None

    def _build_ui(self):
        toolbar = QToolBar("Main")
        tabs = QTabWidget()
        self.btn_order = QPushButton("Execution Order")

        self.addToolBar(toolbar)

        self.project_combo = QComboBox()
        self.preset_combo = QComboBox()
        
        self.btn_start = QPushButton("Start Run")
        self.btn_stop = QPushButton("Stop")
        self.btn_rerun = QPushButton("Create Re-run (FAIL)")

        self.lbl_status = QLabel("Idle")

        toolbar.addSeparator()
        toolbar.addWidget(self.btn_start)
        toolbar.addWidget(self.btn_stop)
        toolbar.addWidget(self.btn_rerun)
        toolbar.addWidget(self.lbl_status)

        toolbar.addSeparator()
        toolbar.addWidget(self.btn_order)
        self.btn_order.clicked.connect(self.on_edit_execution_order)


        self.btn_start.clicked.connect(self.on_start_run)
        self.btn_stop.clicked.connect(self.on_stop_run)
        self.btn_rerun.clicked.connect(self.on_create_rerun)

        toolbar.addWidget(QLabel(" Project: "))
        toolbar.addWidget(self.project_combo)
        toolbar.addSeparator()
        toolbar.addWidget(QLabel(" Preset: "))
        toolbar.addWidget(self.preset_combo)

        self.btn_add_plan = QPushButton("Add Plan")
        self.btn_reload = QPushButton("Reload Plan")
        self.btn_more = QPushButton("Load More")
        self.btn_disable = QPushButton("Disable Selected")
        self.btn_enable = QPushButton("Enable Selected")
        self.chk_show_disabled = QCheckBox("Show Disabled")
        self.chk_show_disabled.setChecked(False)

        toolbar.addSeparator()
        toolbar.addWidget(self.btn_add_plan)
        toolbar.addWidget(self.btn_more)
        toolbar.addWidget(self.btn_disable)
        toolbar.addWidget(self.btn_enable)
        toolbar.addWidget(self.chk_show_disabled)
        toolbar.addWidget(self.btn_reload)

        self.btn_add_plan.clicked.connect(self.on_add_plan)
        self.btn_more.clicked.connect(self.on_load_more)
        self.btn_disable.clicked.connect(self.on_disable_selected)
        self.btn_enable.clicked.connect(self.on_enable_selected)
        self.chk_show_disabled.stateChanged.connect(lambda _s: self.on_reload_plan())
        self.btn_reload.clicked.connect(self.on_reload_plan)

        self.project_combo.currentIndexChanged.connect(self.on_project_changed)
        self.preset_combo.currentIndexChanged.connect(self.on_preset_changed)

        plan_splitter = QSplitter(Qt.Horizontal)

        # Tree
        self.tree = QTreeView()
        self.tree_model = QStandardItemModel()
        self.tree_model.setHorizontalHeaderLabels(["Scenario Tree"])
        self.tree.setModel(self.tree_model)
        self.tree.clicked.connect(self.on_tree_clicked)

        # Table
        self.table = QTableView()
        self.case_model = CaseTableModel()
        self.table.setModel(self.case_model)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.horizontalHeader().setStretchLastSection(True)

        plan_splitter .addWidget(self.tree)
        plan_splitter .addWidget(self.table)
        plan_splitter .setSizes([350, 850])
        
        tabs.addTab(plan_splitter, "Plan")
        
        # --- Results tab (새로 만들 위젯) ---
        self.results_widget = self._build_results_tab()
        tabs.addTab(self.results_widget, "Results")

        self.setCentralWidget(tabs)
        
    def _build_results_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        top = QHBoxLayout()
        self.run_combo = QComboBox()
        self.btn_refresh_runs = QPushButton("Refresh Runs")
        self.btn_load_results = QPushButton("Load Results")
        self.result_filter_status = QComboBox()
        self.result_filter_status.addItems(["ALL", "FAIL", "PASS", "SKIP", "ERROR"])
        self.btn_rerun_from_selection = QPushButton("Re-run from Selection")

        top.addWidget(QLabel("Run:"))
        top.addWidget(self.run_combo, 2)
        top.addWidget(self.btn_refresh_runs)
        top.addSpacing(12)
        top.addWidget(QLabel("Status:"))
        top.addWidget(self.result_filter_status)
        top.addWidget(self.btn_load_results)
        top.addWidget(self.btn_rerun_from_selection)


        layout.addLayout(top)

        splitter = QSplitter(Qt.Vertical)

        self.results_table = QTableView()
        self.results_model = ResultsTableModel()
        self.results_table.setModel(self.results_model)
        self.results_table.setSelectionBehavior(QTableView.SelectRows)
        self.results_table.setSelectionMode(QTableView.ExtendedSelection)
        self.results_table.horizontalHeader().setStretchLastSection(True)

        self.steps_table = QTableView()
        self.steps_model = StepLogModel()
        self.steps_table.setModel(self.steps_model)
        self.steps_table.setSelectionBehavior(QTableView.SelectRows)
        self.steps_table.horizontalHeader().setStretchLastSection(True)

        splitter.addWidget(self.results_table)
        splitter.addWidget(self.steps_table)
        splitter.setSizes([700, 300])

        layout.addWidget(splitter, 1)
        
        self.btn_refresh_runs.clicked.connect(self.on_refresh_runs)
        self.btn_load_results.clicked.connect(self.on_load_results)
        
        # 결과 선택이 바뀌면 step 로그 로드
        self.results_table.selectionModel().selectionChanged.connect(self.on_result_selection_changed)


        return w


    def _load_initial_data(self):
        project_id, preset_id = self.svc.ensure_demo_project_and_preset()
        self._reload_projects(select_project_id=project_id)
        self._reload_presets(project_id, select_preset_id=preset_id)

    # ✅ 여기 추가: 콤보 상태를 강제로 내부 변수에 반영
        self.project_id = self.project_combo.currentData()
        self.preset_id = self.preset_combo.currentData()

    # ✅ 그리고 트리/테이블 리셋 등 기존 로직을 태워야 하면 직접 호출
        if self.project_id:
            self.on_project_changed(self.project_combo.currentIndex())
        if self.preset_id:
            self.on_preset_changed(self.preset_combo.currentIndex())
    
    def _reload_projects(self, select_project_id: Optional[str] = None):
        self.project_combo.blockSignals(True)
        self.project_combo.clear()

        projects = self.svc.list_projects()
        for p in projects:
            self.project_combo.addItem(p["name"], userData=p["project_id"])

        self.project_combo.blockSignals(False)

        if select_project_id:
            idx = self.project_combo.findData(select_project_id)
            if idx >= 0:
                self.project_combo.setCurrentIndex(idx)

    def _reload_presets(self, project_id: str, select_preset_id: Optional[str] = None):
        self.preset_combo.blockSignals(True)
        self.preset_combo.clear()

        presets = self.svc.list_presets(project_id)
        for pr in presets:
            self.preset_combo.addItem(pr["name"], userData=pr["preset_id"])

        self.preset_combo.blockSignals(False)

        if select_preset_id:
            idx = self.preset_combo.findData(select_preset_id)
            if idx >= 0:
                self.preset_combo.setCurrentIndex(idx)

    def on_reload_plan(self):
        if not self._current_plan_node_id:
            return
        ctx = self._plans.get(self._current_plan_node_id)
        if not ctx:
            return

        # DB에서 overrides 다시 로드
        ctx.overrides = self.svc.load_override_objs(ctx.preset_id)

        # 테이블/페이징 리셋 후 다시 로딩
        self._current_offset = 0
        self.case_model.clear()
        self._load_page()
        
    # ---------- events ----------
    def on_project_changed(self, _idx: int):
        pid = self.project_combo.currentData()
        if not pid:
            return
        self.project_id = pid
        self._reload_presets(pid)
        # 트리/테이블 reset
        self.tree_model.removeRows(0, self.tree_model.rowCount())
        self.case_model.clear()

    def on_preset_changed(self, _idx: int):
        self.preset_id = self.preset_combo.currentData()

    def on_add_plan(self):
        if not self.project_id:
            QMessageBox.warning(self, "No project", "Select a project.")
            return
        if not self.preset_id:
            QMessageBox.warning(self, "No preset", "Select a preset.")
            return

        ruleset, preset, recipe, overrides = self.svc.build_recipe_from_preset(self.preset_id)

        # Plan node id(트리에서 찾기 위한 임의 키)
        plan_node_id = f"PLAN::{self.preset_id}"

        ctx = PlanContext(
            project_id=self.project_id,
            preset_id=self.preset_id,
            ruleset=ruleset,
            preset=preset,
            recipe=recipe,
            overrides=overrides
        )
        self._plans[plan_node_id] = ctx

        # 트리 표시: Plan > test_type 그룹
        root = self.tree_model.invisibleRootItem()
        plan_item = QStandardItem(f"{preset.name}  ({recipe.band}/{recipe.standard}/{recipe.plan_mode})")
        plan_item.setData(plan_node_id, role=Qt.UserRole)
        root.appendRow(plan_item)

        for t in recipe.test_types:
            child = QStandardItem(f"{t}")
            child.setData(plan_node_id, role=Qt.UserRole)  # 같은 plan_node_id
            child.setData({"test_type": t}, role=Qt.UserRole + 1)  # filter
            plan_item.appendRow(child)

        self.tree.expand(plan_item.index())
        self.tree.setCurrentIndex(plan_item.index())
        self._select_tree_node(plan_item)

    def on_tree_clicked(self, index):
        item = self.tree_model.itemFromIndex(index)
        if item:
            self._select_tree_node(item)

    def _select_tree_node(self, item: QStandardItem):
        plan_node_id = item.data(Qt.UserRole)
        if not plan_node_id or plan_node_id not in self._plans:
            return

        self._current_plan_node_id = plan_node_id

        # filter info (child node only)
        filter_ = item.data(Qt.UserRole + 1)
        self._current_filter = filter_ if isinstance(filter_, dict) else None

        # reset paging
        self._current_offset = 0
        self.case_model.clear()

        self._load_page()

    def _load_page(self):
        if not self._current_plan_node_id:
            return
        ctx = self._plans[self._current_plan_node_id]

        rows = self.svc.get_cases_page(
            ruleset=ctx.ruleset,
            recipe=ctx.recipe,
            overrides=ctx.overrides,
            filter_=self._current_filter,
            offset=self._current_offset,
            limit=self.PAGE_SIZE,
            show_disabled=self.chk_show_disabled.isChecked() if hasattr(self, 'chk_show_disabled') else False,
        )
        self.case_model.append_rows(rows)
        self._current_offset += len(rows)

        # 컬럼 폭 간단 조정
        self.table.resizeColumnsToContents()

    def on_load_more(self):
        self._load_page()

    def on_disable_selected(self):
        """Scenario Plan에서 선택한 케이스를 'Disable' 처리합니다.

        구현:
        - preset_id에 연결된 overrides에 action='skip' 규칙을 추가합니다.
        - 이 규칙이 적용되면 기본적으로 해당 케이스는 Plan 목록에서 사라집니다.
        - 'Show Disabled' 체크 시, disabled 케이스도 tags['_disabled']=True로 표시되어 다시 보입니다.
        """
        if not self._current_plan_node_id:
            return
        ctx = self._plans[self._current_plan_node_id]

        sel = self.table.selectionModel().selectedRows()
        cases = []
        for idx in sel:
            c = self.case_model.get_case(idx.row())
            if c:
                cases.append(c)

        if not cases:
            QMessageBox.information(self, "No selection", "Select one or more cases to disable.")
            return

        created_ids = self.svc.disable_selected_cases(
            project_id=ctx.project_id,
            preset_id=ctx.preset_id,
            cases=cases,
            priority=100,
        )
        QMessageBox.information(self, "Disabled", f"Disabled {len(cases)} cases (overrides created: {len(created_ids)}).")
        self.on_reload_plan()

    def on_enable_selected(self):
        """Scenario Plan에서 선택한 케이스를 다시 'Enable' 처리합니다.

        주의:
        - Disable된 케이스는 기본 목록에 보이지 않으므로,
          'Show Disabled'를 켠 뒤 OFF 행을 선택하고 Enable 하는 흐름을 권장합니다.
        """
        if not self._current_plan_node_id:
            return
        ctx = self._plans[self._current_plan_node_id]

        sel = self.table.selectionModel().selectedRows()
        cases = []
        for idx in sel:
            c = self.case_model.get_case(idx.row())
            if c:
                cases.append(c)

        if not cases:
            QMessageBox.information(self, "No selection", "Select one or more cases to enable.")
            return

        hit = self.svc.enable_selected_cases(
            preset_id=ctx.preset_id,
            cases=cases,
        )
        QMessageBox.information(self, "Enabled", f"Enabled cases. Overrides disabled: {hit}")
        self.on_reload_plan()


    def on_start_run(self):
        if not self._current_plan_node_id:
            QMessageBox.information(self, "No plan", "Add a plan and select it in the tree first.")
            return
        if self._worker and self._worker.isRunning():
            QMessageBox.information(self, "Running", "A run is already in progress.")
            return

        ctx = self._plans[self._current_plan_node_id]

        # run 생성
        run_id = self.run_repo.create_run(ctx.project_id, ctx.preset_id)
        self._last_run_id = run_id
        self.lbl_status.setText(f"RUNNING {run_id[:8]} ...")

        # worker 시작
        self._worker = RunWorker(
            run_service=self.run_service,
            project_id=ctx.project_id,
            preset_id=ctx.preset_id,
            run_id=run_id,
            ruleset=ctx.ruleset,
            recipe=ctx.recipe,
            overrides=ctx.overrides,
        )
        self._worker.progress.connect(lambda c, s: self.lbl_status.setText(f"RUNNING {run_id[:8]} | {c} cases | last={s}"))
        self._worker.finished.connect(self._on_run_finished)
        self._worker.start()

    def _on_run_finished(self, final_status: str, run_id: str, error_text: str):
        self.run_repo.finish_run(run_id, final_status)

        if final_status == "ERROR":
            self.lbl_status.setText(f"ERROR {run_id[:8]}")
            QMessageBox.critical(self, "Run ERROR", error_text or "No traceback captured")
            return

        self.lbl_status.setText(f"{final_status} {run_id[:8]}")
        QMessageBox.information(self, "Run finished", f"Run {run_id}\nStatus: {final_status}")

        # 선택: runs 콤보 자동 갱신
        self.on_refresh_runs()

    def on_stop_run(self):
        if self._worker and self._worker.isRunning():
            self._worker.request_stop()
            self.lbl_status.setText("Stopping...")

    def on_create_rerun(self):
        if not self._last_run_id:
            QMessageBox.information(self, "No run", "Run first to generate FAIL-based re-run.")
            return
        if not self._current_plan_node_id:
            return
        ctx = self._plans[self._current_plan_node_id]

        try:
            new_preset_id = self.svc.create_rerun_preset_from_fail(
                project_id=ctx.project_id,
                base_preset_id=ctx.preset_id,
                run_id=self._last_run_id
            )
            QMessageBox.information(self, "Re-run preset created", f"New preset created.\nPreset ID: {new_preset_id}")
            # preset 콤보 갱신
            self._reload_presets(ctx.project_id, select_preset_id=new_preset_id)
        except Exception as e:
            QMessageBox.warning(self, "Re-run failed", str(e))
    
    def on_refresh_runs(self):
        if not self.project_id:
            QMessageBox.information(self, "No project", "Select a project first.")
            return

        runs = self.run_repo.list_recent_runs(self.project_id, limit=50)

        self.run_combo.blockSignals(True)
        self.run_combo.clear()
        for r in runs:
            label = f"{r['started_at']} | {r['status']} | {r['run_id'][:8]}"
            self.run_combo.addItem(label, userData=r["run_id"])
        self.run_combo.blockSignals(False)

    def on_load_results(self):
        if not self.project_id:
            QMessageBox.information(self, "No project", "Select a project first.")
            return

        run_id = self.run_combo.currentData()
        if not run_id:
            QMessageBox.information(self, "No run", "Refresh runs and select one.")
            return

        status = self.result_filter_status.currentText()
        rows = self.run_repo.list_results(self.project_id, run_id, status=status, limit=5000)
        self.results_model.set_rows(rows)
        self.results_table.resizeColumnsToContents()
        
    def on_rerun_from_selection(self):
        if not self.project_id:
            QMessageBox.information(self, "No project", "Select a project first.")
            return

        # base preset은 “현재 Plan 탭에서 선택된 preset”을 기준으로 하자
        base_preset_id = self.preset_id
        if not base_preset_id:
            QMessageBox.information(self, "No base preset", "Select a preset (base) first.")
            return

        sel = self.results_table.selectionModel().selectedRows()
        if not sel:
            QMessageBox.information(self, "No selection", "Select result rows first.")
            return

        selected_rows = []
        for idx in sel:
            r = self.results_model.get_row(idx.row())
            if r:
                selected_rows.append(r)

        try:
            new_preset_id = self.svc.create_rerun_preset_from_selected_results(
                project_id=self.project_id,
                base_preset_id=base_preset_id,
                selected_rows=selected_rows
            )
            QMessageBox.information(self, "Re-run preset created", f"New preset created.\nPreset ID: {new_preset_id}")

            # preset 콤보 갱신해서 바로 선택되게
            self._reload_presets(self.project_id, select_preset_id=new_preset_id)

        except Exception as e:
            QMessageBox.warning(self, "Re-run failed", str(e))
            
    def on_edit_execution_order(self):
        if not self.preset_id:
            QMessageBox.information(self, "No preset", "Select a preset first.")
            return

        # preset json에서 기존 order 읽기
        pj = self.svc.repo.load_preset(self.preset_id)
        if "selection" in pj:
            sel = pj.get("selection", {})
        else:
            sel = pj  # 구포맷 fallback

        pol = sel.get("execution_policy", {})
        current_order = pol.get("test_order") or ["PSD", "OBW", "SP", "RX"]

        dlg = ExecutionOrderDialog(initial_order=current_order, parent=self)
        if dlg.exec() == dlg.Accepted:
            new_order = dlg.get_order()
            self.svc.save_execution_order(self.preset_id, new_order)
            QMessageBox.information(self, "Saved", f"Execution order saved:\n{new_order}")

            # Plan을 이미 올려놨다면 reload해서 반영 (선택)
            if self._current_plan_node_id:
                self.on_reload_plan()
                
    def on_result_selection_changed(self, selected, deselected):
        if not self.project_id:
            return
        sel = self.results_table.selectionModel().selectedRows()
        if not sel:
            self.steps_model.set_rows([])
            return

        row = self.results_model.get_row(sel[0].row())
        result_id = row.get("result_id")
        if not result_id:
            self.steps_model.set_rows([])
            return

        steps = self.run_repo.list_step_results(self.project_id, result_id)
        self.steps_model.set_rows(steps)
        self.steps_table.resizeColumnsToContents()
                
class RunWorker(QThread):
    progress = Signal(int, str)   # count, last_status
    finished = Signal(str, str, str)   # final_status, run_id, error_text

    def __init__(self, run_service, project_id, preset_id, run_id, ruleset, recipe, overrides):
        super().__init__()
        self.run_service = run_service
        self.project_id = project_id
        self.preset_id = preset_id
        self.run_id = run_id
        self.ruleset = ruleset
        self.recipe = recipe
        self.overrides = overrides
        self._stop = False
        self._error_text = ""

    def request_stop(self):
        self._stop = True

    def run(self):
        import traceback
        self._error_text = ""
        final_status = "ERROR"

        try:
            def should_stop():
                return self._stop

            def on_progress(count, status):
                self.progress.emit(count, status)

            final_status = self.run_service.run(
                project_id=self.project_id,
                preset_id=self.preset_id,
                run_id=self.run_id,
                ruleset=self.ruleset,
                recipe=self.recipe,
                overrides=self.overrides,
                should_stop=should_stop,
                on_progress=on_progress,
            )

        except Exception:
            self._error_text = traceback.format_exc()
            final_status = "ERROR"

        # ✅ 여기 중요: 항상 finished emit
        self.finished.emit(final_status, self.run_id, self._error_text)
        
