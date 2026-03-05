import sys, logging, traceback
from pathlib import Path

from PySide6.QtWidgets import QApplication

from infrastructure.db import init_db
from infrastructure.plan_repo_sqlite import PlanRepositorySQLite
from application.plan_service import PlanService
from ui.main_window import MainWindow
from infrastructure.run_repo_sqlite import RunRepositorySQLite
from application.run_service import RunService
from application.preset_seeder import seed_presets_from_folder
from application.run_service_step import RunServiceStep

log = logging.getLogger("global")

def excepthook(exctype, value, tb):
    msg = "".join(traceback.format_exception(exctype, value, tb))
    log.error("Uncaught exception:\n%s", msg)
    # 콘솔에도 출력
    sys.__excepthook__(exctype, value, tb)


def setup_logging():
    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler("logs/app.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

def main():
    init_db()

    plan_repo = PlanRepositorySQLite()
    run_repo = RunRepositorySQLite()

    # ✅ 1) 데모 프로젝트(또는 기본 프로젝트) 확보
    # (아래 함수는 없으면 PlanRepositorySQLite에 추가해야 함)
    project_id = plan_repo.ensure_demo_project("DEMO_PROJECT")

    # ✅ 2) presets/ 폴더의 JSON들을 DB로 seed
    inserted, updated = seed_presets_from_folder(
        repo=plan_repo,
        project_id=project_id,
        presets_dir=Path("presets"),
        update_policy="if_ruleset_version_changed",
    )
    print(f"[preset seed] inserted={inserted}, updated={updated}")

    svc = PlanService(repo=plan_repo, run_repo=run_repo, ruleset_dir=Path("rulesets"))
    run_service = RunServiceStep(run_repo=run_repo)

    app = QApplication(sys.argv)
    w = MainWindow(plan_service=svc, run_repo=run_repo, run_service=run_service)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
    
    