import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from .db import get_connection


def now() -> str:
    return datetime.utcnow().isoformat()


class PlanRepositorySQLite:
    def create_project(self, name: str, description: str = "") -> str:
        project_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO projects (project_id, name, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """, (project_id, name, description, now(), now()))
        conn.commit()
        conn.close()
        return project_id

    def list_projects(self) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM projects ORDER BY created_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def save_preset(
        self,
        project_id: str,
        name: str,
        ruleset_id: str,
        ruleset_version: str,
        preset_json: Dict[str, Any],
    ) -> str:
        preset_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO presets (
          preset_id, project_id, name,
          ruleset_id, ruleset_version,
          json_data, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            preset_id, project_id, name, ruleset_id, ruleset_version,
            json.dumps(preset_json, ensure_ascii=False),
            now(), now()
        ))
        conn.commit()
        conn.close()
        return preset_id

    def list_presets(self, project_id: str) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT preset_id, name, ruleset_id, ruleset_version, created_at
        FROM presets
        WHERE project_id = ?
        ORDER BY created_at DESC
        """, (project_id,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def load_preset(self, preset_id: str) -> Dict[str, Any]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT json_data FROM presets WHERE preset_id = ?", (preset_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            raise ValueError("Preset not found")
        return json.loads(row["json_data"])

    def save_override(
        self,
        project_id: str,
        preset_id: str,
        name: str,
        override_json: Dict[str, Any],
        priority: int = 100,
        enabled: bool = True,
    ) -> str:
        override_id = str(uuid.uuid4())
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO overrides (
          override_id, project_id, preset_id,
          name, enabled, priority,
          json_data, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            override_id, project_id, preset_id,
            name, 1 if enabled else 0, int(priority),
            json.dumps(override_json, ensure_ascii=False),
            now(), now()
        ))
        conn.commit()
        conn.close()
        return override_id

    def list_overrides(self, preset_id: str) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT * FROM overrides
        WHERE preset_id = ?
        ORDER BY priority ASC
        """, (preset_id,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        for r in rows:
            r["json_data"] = json.loads(r["json_data"])
        return rows
    
    def find_preset_id_by_name(self, project_id: str, name: str) -> Optional[str]:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        SELECT preset_id FROM presets
        WHERE project_id = ? AND name = ?
        LIMIT 1
        """, (project_id, name))
        row = cur.fetchone()
        conn.close()
        return row["preset_id"] if row else None
    
    def update_preset_json(self, preset_id: str, preset_json: Dict[str, Any]) -> None:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        UPDATE presets
        SET json_data = ?, updated_at = ?
        WHERE preset_id = ?
        """, (json.dumps(preset_json, ensure_ascii=False), now(), preset_id))
        conn.commit()
        conn.close()

    def update_override_enabled(self, override_id: str, enabled: bool) -> None:
        """override의 enabled 플래그를 업데이트(soft enable/disable)."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE overrides
            SET enabled = ?, updated_at = ?
            WHERE override_id = ?
            """,
            (1 if enabled else 0, now(), override_id),
        )
        conn.commit()
        conn.close()

    def delete_override(self, override_id: str) -> None:
        """override를 완전 삭제(hard delete). MVP에서는 사용하지 않길 권장."""
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM overrides WHERE override_id = ?", (override_id,))
        conn.commit()
        conn.close()


        
    def ensure_demo_project(self, name: str) -> str:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT project_id FROM projects WHERE name=? LIMIT 1", (name,))
        row = cur.fetchone()
        if row:
            pid = row["project_id"]
            conn.close()
            return pid

        pid = str(uuid.uuid4())
        ts = now()

        cur.execute("""
        INSERT INTO projects (project_id, name, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """, (pid, name, ts, ts))

        conn.commit()
        conn.close()
        return pid