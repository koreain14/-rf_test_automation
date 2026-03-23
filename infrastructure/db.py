import sqlite3
from pathlib import Path

DB_FILE = Path("rf_platform.db")

def reset_db_file():
    if DB_FILE.exists():
        DB_FILE.unlink()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
      project_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS presets (
      preset_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      name TEXT NOT NULL,
      ruleset_id TEXT NOT NULL,
      ruleset_version TEXT NOT NULL,
      json_data TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(project_id, name)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS overrides (
      override_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      preset_id TEXT NOT NULL,
      name TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      priority INTEGER NOT NULL,
      json_data TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    """)
    
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_overrides_preset ON overrides(project_id, preset_id, priority);")
    
    # infrastructure/db.py (init_db() 안에 추가)

    # runs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      preset_id TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      status TEXT NOT NULL,              -- RUNNING/DONE/ABORTED/ERROR
      parent_run_id TEXT,
      note TEXT DEFAULT ''
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id, started_at);")

    # results
    cur.execute("""
    CREATE TABLE IF NOT EXISTS results (
      result_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      run_id TEXT NOT NULL,

      test_key TEXT NOT NULL,
      tech TEXT NOT NULL,
      regulation TEXT NOT NULL,
      band TEXT NOT NULL,
      standard TEXT NOT NULL,
      test_type TEXT NOT NULL,
      channel INTEGER NOT NULL,
      bw_mhz INTEGER NOT NULL,

      status TEXT NOT NULL,              -- PASS/FAIL/SKIP/ERROR
      margin_db REAL,
      measured_value REAL,
      limit_value REAL,

      instrument_snapshot_json TEXT,
      tags_json TEXT,
      created_at TEXT NOT NULL
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_results_run ON results(project_id, run_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_results_status ON results(project_id, status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_results_filter ON results(project_id, band, standard, test_type, channel, bw_mhz);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_results_run_key ON results(project_id, run_id, test_key);")

    # step_results (나중 확장용)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS step_results (
      step_result_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      result_id TEXT NOT NULL,
      step_name TEXT NOT NULL,
      status TEXT NOT NULL,
      artifact_uri TEXT,
      data_json TEXT,
      created_at TEXT NOT NULL
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_step_results_result ON step_results(project_id, result_id);")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS plan_case_cache (
      cache_key TEXT NOT NULL,
      project_id TEXT NOT NULL,
      preset_id TEXT NOT NULL,
      case_key TEXT NOT NULL,
      band TEXT NOT NULL DEFAULT '',
      standard TEXT NOT NULL DEFAULT '',
      phy_mode TEXT NOT NULL DEFAULT '',
      bandwidth_mhz INTEGER NOT NULL DEFAULT 0,
      channel INTEGER NOT NULL DEFAULT 0,
      frequency_mhz REAL NOT NULL DEFAULT 0,
      test_type TEXT NOT NULL DEFAULT '',
      enabled INTEGER NOT NULL DEFAULT 1,
      excluded INTEGER NOT NULL DEFAULT 0,
      deleted INTEGER NOT NULL DEFAULT 0,
      priority_tag TEXT NOT NULL DEFAULT '',
      sort_index INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (cache_key, case_key)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_plan_case_cache_lookup ON plan_case_cache(cache_key, band, standard, test_type, channel, bandwidth_mhz);")

    # Additive migration for existing DB files. CREATE TABLE IF NOT EXISTS does not
    # backfill newly introduced columns on old databases, so we must explicitly
    # ensure them here during startup.
    _ensure_plan_case_cache_columns(cur)

    conn.commit()
    conn.close()

def _ensure_plan_case_cache_columns(cur):
    existing = {str(r[1]) for r in cur.execute("PRAGMA table_info(plan_case_cache)").fetchall()}
    additions = [
        ("mode", "TEXT NOT NULL DEFAULT ''"),
        ("bw_mhz", "INTEGER NOT NULL DEFAULT 0"),
        ("center_freq_mhz", "REAL NOT NULL DEFAULT 0"),
        ("tech", "TEXT NOT NULL DEFAULT ''"),
        ("regulation", "TEXT NOT NULL DEFAULT ''"),
        ("group_name", "TEXT NOT NULL DEFAULT ''"),
    ]
    for name, ddl in additions:
        if name not in existing:
            cur.execute(f"ALTER TABLE plan_case_cache ADD COLUMN {name} {ddl}")
    cur.execute("UPDATE plan_case_cache SET mode = COALESCE(NULLIF(mode, ''), phy_mode) WHERE mode = '' OR mode IS NULL")
    cur.execute("UPDATE plan_case_cache SET bw_mhz = bandwidth_mhz WHERE bw_mhz = 0 AND bandwidth_mhz != 0")
    cur.execute("UPDATE plan_case_cache SET center_freq_mhz = frequency_mhz WHERE center_freq_mhz = 0 AND frequency_mhz != 0")
