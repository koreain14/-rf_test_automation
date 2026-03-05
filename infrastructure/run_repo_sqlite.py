# infrastructure/run_repo_sqlite.py

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from .db import get_connection


def now() -> str:
    return datetime.utcnow().isoformat()


class RunRepositorySQLite:
    def create_run(self, project_id: str, preset_id: str, parent_run_id: Optional[str] = None, note: str = "") -> str:
        run_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO runs (run_id, project_id, preset_id, started_at, finished_at, status, parent_run_id, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, project_id, preset_id, now(), None, "RUNNING", parent_run_id, note))
        conn.commit()
        conn.close()
        return run_id

    def finish_run(self, run_id: str, status: str):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        UPDATE runs SET finished_at = ?, status = ? WHERE run_id = ?
        """, (now(), status, run_id))
        conn.commit()
        conn.close()

    def append_result(self, project_id: str, run_id: str, row: Dict[str, Any]) -> str:
        """
        row must include:
          test_key, tech, regulation, band, standard, test_type, channel, bw_mhz, status
        optional:
          margin_db, measured_value, limit_value, instrument_snapshot, tags
        """
        result_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO results (
          result_id, project_id, run_id,
          test_key, tech, regulation, band, standard, test_type, channel, bw_mhz,
          status, margin_db, measured_value, limit_value,
          instrument_snapshot_json, tags_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result_id, project_id, run_id,
            row["test_key"], row["tech"], row["regulation"], row["band"], row["standard"],
            row["test_type"], int(row["channel"]), int(row["bw_mhz"]),
            row["status"],
            row.get("margin_db"), row.get("measured_value"), row.get("limit_value"),
            json.dumps(row.get("instrument_snapshot", {}), ensure_ascii=False),
            json.dumps(row.get("tags", {}), ensure_ascii=False),
            now()
        ))

        conn.commit()
        conn.close()
        return result_id

    def list_recent_runs(self, project_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT * FROM runs
        WHERE project_id = ?
        ORDER BY started_at DESC
        LIMIT ?
        """, (project_id, int(limit)))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def get_failed_cases(self, project_id: str, run_id: str) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT test_type, band, standard, channel, bw_mhz
        FROM results
        WHERE project_id = ? AND run_id = ? AND status = 'FAIL'
        """, (project_id, run_id))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    
    def list_results(self, project_id: str, run_id: str, status: str | None = None, limit: int = 5000):
        conn = get_connection()
        cur = conn.cursor()

        if status and status != "ALL":
            cur.execute("""
            SELECT result_id, status, test_type, band, standard, channel, bw_mhz, margin_db, test_key, tags_json
            FROM results
            WHERE project_id = ? AND run_id = ? AND status = ?
            ORDER BY created_at ASC
            LIMIT ?
            """, (project_id, run_id, status, int(limit)))
        else:
            cur.execute("""
            SELECT result_id, status, test_type, band, standard, channel, bw_mhz, margin_db, test_key, tags_json
            FROM results
            WHERE project_id = ? AND run_id = ?
            ORDER BY created_at ASC
            LIMIT ?
            """, (project_id, run_id, int(limit)))

        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        # tags_json에서 group 꺼내기(편의)
        for r in rows:
            try:
                import json as _json
                tags = _json.loads(r.get("tags_json") or "{}")
                r["group"] = tags.get("group", "")
            except Exception:
                r["group"] = ""
        return rows
    
    def create_result_stub(self, project_id: str, run_id: str, row: Dict[str, Any]) -> str:
        """
        row must include:
          test_key, tech, regulation, band, standard, test_type, channel, bw_mhz
        """
        result_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO results (
          result_id, project_id, run_id,
          test_key, tech, regulation, band, standard, test_type, channel, bw_mhz,
          status, margin_db, measured_value, limit_value,
          instrument_snapshot_json, tags_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result_id, project_id, run_id,
            row["test_key"], row["tech"], row["regulation"], row["band"], row["standard"],
            row["test_type"], int(row["channel"]), int(row["bw_mhz"]),
            "RUNNING",
            None, None, None,
            json.dumps(row.get("instrument_snapshot", {}), ensure_ascii=False),
            json.dumps(row.get("tags", {}), ensure_ascii=False),
            now()
        ))

        conn.commit()
        conn.close()
        return result_id

    def update_result_final(
        self,
        result_id: str,
        status: str,
        margin_db: Optional[float] = None,
        measured_value: Optional[float] = None,
        limit_value: Optional[float] = None,
    ) -> None:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        UPDATE results
        SET status=?, margin_db=?, measured_value=?, limit_value=?
        WHERE result_id=?
        """, (status, margin_db, measured_value, limit_value, result_id))
        conn.commit()
        conn.close()

    def append_step_result(
        self,
        project_id: str,
        result_id: str,
        step_name: str,
        status: str,
        data: Dict[str, Any],
        artifact_uri: Optional[str] = None,
    ) -> str:
        step_result_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO step_results (
          step_result_id, project_id, result_id,
          step_name, status, artifact_uri, data_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            step_result_id, project_id, result_id,
            step_name, status, artifact_uri,
            json.dumps(data or {}, ensure_ascii=False),
            now()
        ))
        conn.commit()
        conn.close()
        return step_result_id

    def list_step_results(self, project_id: str, result_id: str) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT step_name, status, artifact_uri, data_json, created_at
        FROM step_results
        WHERE project_id=? AND result_id=?
        ORDER BY created_at ASC
        """, (project_id, result_id))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        # data_json 파싱 편의
        for r in rows:
            try:
                r["data"] = json.loads(r.get("data_json") or "{}")
            except Exception:
                r["data"] = {}
        return rows

    def list_results(self, project_id: str, run_id: str, status: str | None = None, limit: int = 5000):
        # ✅ result_id를 포함해서 반환하도록 수정
        conn = get_connection()
        cur = conn.cursor()

        if status and status != "ALL":
            cur.execute("""
            SELECT result_id, status, test_type, band, standard, channel, bw_mhz, margin_db, test_key, tags_json
            FROM results
            WHERE project_id = ? AND run_id = ? AND status = ?
            ORDER BY created_at ASC
            LIMIT ?
            """, (project_id, run_id, status, int(limit)))
        else:
            cur.execute("""
            SELECT result_id, status, test_type, band, standard, channel, bw_mhz, margin_db, test_key, tags_json
            FROM results
            WHERE project_id = ? AND run_id = ?
            ORDER BY created_at ASC
            LIMIT ?
            """, (project_id, run_id, int(limit)))

        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

        for r in rows:
            try:
                tags = json.loads(r.get("tags_json") or "{}")
                r["group"] = tags.get("group", "")
            except Exception:
                r["group"] = ""
        return rows