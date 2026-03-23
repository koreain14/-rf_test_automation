import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .db import get_connection, _ensure_plan_case_cache_columns


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


PLAN_CASE_CACHE_COLUMNS = (
    "cache_key", "project_id", "preset_id", "case_key", "band", "standard", "phy_mode", "mode",
    "bandwidth_mhz", "bw_mhz", "channel", "frequency_mhz", "center_freq_mhz", "test_type",
    "tech", "regulation", "group_name", "enabled", "excluded", "deleted", "priority_tag", "sort_index",
)
PLAN_CASE_CACHE_INSERT_SQL = f"""
            INSERT INTO plan_case_cache (
              {', '.join(PLAN_CASE_CACHE_COLUMNS)}
            ) VALUES ({', '.join(['?'] * len(PLAN_CASE_CACHE_COLUMNS))})
            """




def _ensure_plan_case_cache_schema(cur) -> None:
    """Runtime self-heal for plan_case_cache schema.

    Startup migration should normally handle this, but write-path schema ensure
    prevents old DB files or partial startup paths from causing regressions.
    """
    _ensure_plan_case_cache_columns(cur)

def _plan_case_cache_tuple(cache_key: str, project_id: str, preset_id: str, r: Dict[str, Any]) -> tuple:
    row = (
        cache_key, project_id, preset_id,
        str(r.get("case_key") or r.get("id") or r.get("key") or ""),
        str(r.get("band", "") or ""),
        str(r.get("standard", "") or ""),
        str(r.get("phy_mode", "") or ""),
        str(r.get("mode", r.get("phy_mode", "")) or ""),
        int(r.get("bandwidth_mhz", r.get("bw_mhz", 0)) or 0),
        int(r.get("bw_mhz", r.get("bandwidth_mhz", 0)) or 0),
        int(r.get("channel", 0) or 0),
        float(r.get("frequency_mhz", r.get("center_freq_mhz", 0)) or 0),
        float(r.get("center_freq_mhz", r.get("frequency_mhz", 0)) or 0),
        str(r.get("test_type", "") or ""),
        str(r.get("tech", "") or ""),
        str(r.get("regulation", "") or ""),
        str(r.get("group_name", r.get("group", "")) or ""),
        1 if bool(r.get("enabled", True)) else 0,
        1 if bool(r.get("excluded", False)) else 0,
        1 if bool(r.get("deleted", False)) else 0,
        str(r.get("priority_tag", "") or ""),
        int(r.get("sort_index", 0) or 0),
    )
    if len(row) != len(PLAN_CASE_CACHE_COLUMNS):
        raise ValueError(f"plan_case_cache tuple mismatch: expected {len(PLAN_CASE_CACHE_COLUMNS)}, got {len(row)}")
    return row

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
    def clear_plan_case_cache(self, cache_key: str) -> None:
        conn = get_connection()
        cur = conn.cursor()
        _ensure_plan_case_cache_schema(cur)
        cur.execute("DELETE FROM plan_case_cache WHERE cache_key = ?", (cache_key,))
        conn.commit()
        conn.close()

    def sync_plan_case_cache(self, cache_key: str, project_id: str, preset_id: str, rows: List[Dict[str, Any]]) -> None:
        conn = get_connection()
        cur = conn.cursor()
        _ensure_plan_case_cache_schema(cur)
        cur.execute("DELETE FROM plan_case_cache WHERE cache_key = ?", (cache_key,))
        cur.executemany(
            """
            INSERT INTO plan_case_cache (
              cache_key, project_id, preset_id, case_key, band, standard, phy_mode, mode,
              bandwidth_mhz, bw_mhz, channel, frequency_mhz, center_freq_mhz, test_type,
              tech, regulation, group_name, enabled, excluded, deleted, priority_tag, sort_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    cache_key, project_id, preset_id,
                    str(r.get("case_key") or r.get("id") or r.get("key") or ""),
                    str(r.get("band", "") or ""),
                    str(r.get("standard", "") or ""),
                    str(r.get("phy_mode", "") or ""),
                    str(r.get("mode", r.get("phy_mode", "")) or ""),
                    int(r.get("bandwidth_mhz", r.get("bw_mhz", 0)) or 0),
                    int(r.get("bw_mhz", r.get("bandwidth_mhz", 0)) or 0),
                    int(r.get("channel", 0) or 0),
                    float(r.get("frequency_mhz", r.get("center_freq_mhz", 0)) or 0),
                    float(r.get("center_freq_mhz", r.get("frequency_mhz", 0)) or 0),
                    str(r.get("test_type", "") or ""),
                    str(r.get("tech", "") or ""),
                    str(r.get("regulation", "") or ""),
                    str(r.get("group_name", r.get("group", "")) or ""),
                    1 if bool(r.get("enabled", True)) else 0,
                    1 if bool(r.get("excluded", False)) else 0,
                    1 if bool(r.get("deleted", False)) else 0,
                    str(r.get("priority_tag", "") or ""),
                    int(r.get("sort_index", 0) or 0),
                ) for r in rows
            ]
        )
        conn.commit()
        conn.close()

    def rebuild_plan_case_cache_from_iterable(self, *, cache_key: str, project_id: str, preset_id: str, rows) -> None:
        conn = get_connection()
        cur = conn.cursor()
        _ensure_plan_case_cache_schema(cur)
        cur.execute("DELETE FROM plan_case_cache WHERE cache_key = ?", (cache_key,))
        batch = []
        for r in rows:
            batch.append(
                _plan_case_cache_tuple(cache_key=cache_key, project_id=project_id, preset_id=preset_id, r=r)
            )
            if len(batch) >= 1000:
                cur.executemany(PLAN_CASE_CACHE_INSERT_SQL, batch)
                batch.clear()
        if batch:
            cur.executemany(PLAN_CASE_CACHE_INSERT_SQL, batch)
        conn.commit()
        conn.close()

    def count_plan_case_cache_rows(self, *, cache_key: str) -> int:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM plan_case_cache WHERE cache_key = ?", (cache_key,))
        row = cur.fetchone()
        conn.close()
        return int(row["cnt"] if row else 0)

    def list_plan_case_cache_rows(self, *, cache_key: str) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        _ensure_plan_case_cache_schema(cur)
        cur.execute(
            """
            SELECT case_key,
                   case_key AS id,
                   case_key AS key,
                   band,
                   standard,
                   phy_mode,
                   mode,
                   bandwidth_mhz,
                   bw_mhz,
                   channel,
                   frequency_mhz,
                   center_freq_mhz,
                   test_type,
                   tech,
                   regulation,
                   group_name,
                   enabled,
                   excluded,
                   deleted,
                   priority_tag,
                   sort_index
            FROM plan_case_cache
            WHERE cache_key = ?
            ORDER BY sort_index ASC
            """,
            (cache_key,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def _build_plan_case_cache_where(self, plan_filter: Dict[str, Any] | None):
        plan_filter = dict(plan_filter or {})
        clauses = []
        params: List[Any] = []
        if plan_filter.get("band"):
            clauses.append("band = ?")
            params.append(plan_filter["band"])
        if plan_filter.get("standard"):
            clauses.append("standard = ?")
            params.append(plan_filter["standard"])
        if plan_filter.get("phy_mode"):
            clauses.append("phy_mode = ?")
            params.append(plan_filter["phy_mode"])
        if plan_filter.get("test_type"):
            clauses.append("UPPER(test_type) = UPPER(?)")
            params.append(plan_filter["test_type"])
        if plan_filter.get("bw_mhz") not in (None, ""):
            clauses.append("bandwidth_mhz = ?")
            params.append(int(plan_filter["bw_mhz"]))
        if plan_filter.get("channel_from") not in (None, ""):
            clauses.append("channel >= ?")
            params.append(int(plan_filter["channel_from"]))
        if plan_filter.get("channel_to") not in (None, ""):
            clauses.append("channel <= ?")
            params.append(int(plan_filter["channel_to"]))
        if plan_filter.get("enabled_state") == "ENABLED":
            clauses.append("enabled = 1")
        elif plan_filter.get("enabled_state") == "DISABLED":
            clauses.append("enabled = 0")
        search = str(plan_filter.get("search_text", "") or "").strip()
        if search:
            like = f"%{search}%"
            clauses.append("(band LIKE ? OR standard LIKE ? OR phy_mode LIKE ? OR mode LIKE ? OR test_type LIKE ? OR case_key LIKE ? OR tech LIKE ? OR regulation LIKE ? OR group_name LIKE ?)")
            params.extend([like, like, like, like, like, like, like, like, like])
        return clauses, params


    def _normalize_plan_case_order_by(self, order_by: str | None) -> str:
        value = str(order_by or "sort_index ASC").strip()
        return value or "sort_index ASC"

    def _plan_case_page_bounds(self, *, page: int, page_size: int) -> tuple[int, int, int]:
        normalized_page = max(1, int(page or 1))
        normalized_page_size = max(1, min(int(page_size or 200), 5000))
        offset = max(0, (normalized_page - 1) * normalized_page_size)
        return normalized_page, normalized_page_size, offset

    def query_plan_case_group_summary_by_query(self, *, cache_key: str, query: Any, plan_filter: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        return self.query_plan_case_group_summary(cache_key=cache_key, plan_filter=plan_filter)

    def query_plan_case_count_by_query(self, *, cache_key: str, query: Any, plan_filter: Dict[str, Any] | None = None) -> int:
        return self.query_plan_case_count(cache_key=cache_key, plan_filter=plan_filter)

    def query_plan_case_page_by_query(self, *, cache_key: str, query: Any, plan_filter: Dict[str, Any] | None = None, order_by: str | None = None) -> List[Dict[str, Any]]:
        page, page_size, offset = self._plan_case_page_bounds(page=getattr(query, "page", 1), page_size=getattr(query, "page_size", 200))
        return self.query_plan_case_detail_page(
            cache_key=cache_key,
            plan_filter=plan_filter,
            order_by=self._normalize_plan_case_order_by(order_by),
            offset=offset,
            limit=page_size,
        )

    def query_plan_case_runnable_keys_by_query(self, *, cache_key: str, query: Any, plan_filter: Dict[str, Any] | None = None, order_by: str | None = None) -> List[str]:
        return self.query_plan_case_runnable_keys(
            cache_key=cache_key,
            plan_filter=plan_filter,
            order_by=self._normalize_plan_case_order_by(order_by),
        )

    def query_plan_case_group_summary(self, cache_key: str, plan_filter: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        clauses, params = self._build_plan_case_cache_where(plan_filter)
        sql = "SELECT band, standard, bandwidth_mhz, test_type, COUNT(*) AS total_count, SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled_count, SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled_count FROM plan_case_cache WHERE cache_key = ? AND deleted = 0"
        params = [cache_key] + params
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        sql += " GROUP BY band, standard, bandwidth_mhz, test_type ORDER BY band, standard, bandwidth_mhz, test_type"
        cur.execute(sql, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def query_plan_case_count(self, cache_key: str, plan_filter: Dict[str, Any] | None = None) -> int:
        conn = get_connection()
        cur = conn.cursor()
        clauses, params = self._build_plan_case_cache_where(plan_filter)
        sql = "SELECT COUNT(*) AS cnt FROM plan_case_cache WHERE cache_key = ? AND deleted = 0"
        params = [cache_key] + params
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        conn.close()
        return int(row["cnt"] if row else 0)

    def query_plan_case_detail_page(self, cache_key: str, plan_filter: Dict[str, Any] | None = None, order_by: str = "sort_index ASC", offset: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
        conn = get_connection()
        cur = conn.cursor()
        clauses, params = self._build_plan_case_cache_where(plan_filter)
        sql = "SELECT case_key, case_key AS id, case_key AS key, band, standard, phy_mode, mode, bandwidth_mhz, bw_mhz, channel, frequency_mhz, center_freq_mhz, test_type, tech, regulation, group_name, enabled, excluded, deleted, priority_tag, sort_index FROM plan_case_cache WHERE cache_key = ? AND deleted = 0"
        params = [cache_key] + params
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        sql += f" ORDER BY {self._normalize_plan_case_order_by(order_by)} LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
        cur.execute(sql, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def query_plan_case_runnable_keys(self, cache_key: str, plan_filter: Dict[str, Any] | None = None, order_by: str = "sort_index ASC") -> List[str]:
        conn = get_connection()
        cur = conn.cursor()
        clauses, params = self._build_plan_case_cache_where(plan_filter)
        sql = "SELECT case_key FROM plan_case_cache WHERE cache_key = ? AND deleted = 0 AND excluded = 0 AND enabled = 1"
        params = [cache_key] + params
        if clauses:
            sql += " AND " + " AND ".join(clauses)
        sql += f" ORDER BY {self._normalize_plan_case_order_by(order_by)}"
        cur.execute(sql, tuple(params))
        rows = [r["case_key"] for r in cur.fetchall()]
        conn.close()
        return rows
