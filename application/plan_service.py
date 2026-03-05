import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from domain.models import (
    InstrumentProfile, Match, OverrideRule, Preset, Recipe, RuleSet, TestCase
)
from domain.expand import build_recipe, expand_recipe
from domain.overrides import apply_overrides
from infrastructure.plan_repo_sqlite import PlanRepositorySQLite
from infrastructure.run_repo_sqlite import RunRepositorySQLite
from application.migrations_preset import migrate_preset_to_latest




class PlanService:
    def __init__(self, repo: PlanRepositorySQLite, run_repo: RunRepositorySQLite, ruleset_dir: Path):
        self.repo = repo
        self.run_repo = run_repo
        self.ruleset_dir = ruleset_dir
        self._ruleset_cache: Dict[str, RuleSet] = {}

    # ---------- RuleSet ----------
    def load_ruleset(self, ruleset_id: str) -> RuleSet:
        if ruleset_id in self._ruleset_cache:
            return self._ruleset_cache[ruleset_id]

        path = self.ruleset_dir / f"{ruleset_id.lower()}.json"
        # 규칙: 파일명은 kc_wlan.json 처럼 저장해두고, id=KC_WLAN에 매핑
        # 여기서는 kc_wlan.json만 대상으로 간단 매핑
        if not path.exists():
            # fallback: 직접 kc_wlan.json 찾기
            alt = self.ruleset_dir / "kc_wlan.json"
            if alt.exists() and ruleset_id == "KC_WLAN":
                path = alt
            else:
                raise FileNotFoundError(f"RuleSet json not found for {ruleset_id}")

        raw = json.loads(path.read_text(encoding="utf-8"))
        ips = {
            name: InstrumentProfile(name=name, settings=settings)
            for name, settings in raw.get("instrument_profiles", {}).items()
        }

        rs = RuleSet(
            id=raw["id"],
            version=raw["version"],
            regulation=raw["regulation"],
            tech=raw["tech"],
            bands=raw["bands"],
            instrument_profiles=ips,
            plan_modes=raw.get("plan_modes", {}),
        )
        self._ruleset_cache[ruleset_id] = rs
        return rs

    # ---------- Project/Preset ----------
    def list_projects(self) -> List[Dict[str, Any]]:
        return self.repo.list_projects()

    def ensure_demo_project_and_preset(self) -> Tuple[str, str]:
        """
        DB가 비어있으면 데모 프로젝트/프리셋 하나 만들어 UI가 바로 동작하게 함.
        반환: (project_id, preset_id)
        """
        projects = self.repo.list_projects()
        if projects:
            project_id = projects[0]["project_id"]
            presets = self.repo.list_presets(project_id)
            if presets:
                return project_id, presets[0]["preset_id"]

        project_id = self.repo.create_project("Model_KC_Test", "Demo project")
        preset_json = {
            "name": "KC_5G_UNII_LMH_Quick",
            "ruleset_id": "KC_WLAN",
            "ruleset_version": "2026.02",
            "selection": {
                "band": "5G",
                "standard": "802.11ac",
                "plan_mode": "Quick",
                "test_types": ["PSD", "OBW", "SP"],
                "bandwidth_mhz": [20, 80],
                "channels": {
                    "policy": "LOW_MID_HIGH_BY_GROUP",
                    "grouping": "UNII",
                    "groups": ["UNII-1", "UNII-2A", "UNII-2C", "UNII-3"],
                    "representatives_override": {
                        "UNII-2C": { "mid": 116 }
                    }
                },
                "instrument_profile_by_test": {
                    "PSD": "PSD_DEFAULT",
                    "OBW": "OBW_DEFAULT",
                    "SP": "SP_DEFAULT"
                }
            },
            "description": "Demo preset"
        }
        preset_id = self.repo.save_preset(
            project_id=project_id,
            name=preset_json["name"],
            ruleset_id=preset_json["ruleset_id"],
            ruleset_version=preset_json["ruleset_version"],
            preset_json=preset_json,
        )
        return project_id, preset_id

    def list_presets(self, project_id: str) -> List[Dict[str, Any]]:
        return self.repo.list_presets(project_id)

    def load_preset_obj(self, preset_id: str) -> Preset:
        pj = self.repo.load_preset(preset_id)

        # 구 포맷(=selection만 저장) 하위호환
        if "selection" not in pj:
            selection = dict(pj)
            name = selection.get("name") or "UnnamedPreset"
            ruleset_id = selection.get("ruleset_id") or "KC_WLAN"
            ruleset_version = selection.get("ruleset_version") or "2026.02"
            desc = selection.get("description", "")

            selection.pop("name", None)
            selection.pop("ruleset_id", None)
            selection.pop("ruleset_version", None)
            selection.pop("description", None)
        else:
            selection = pj["selection"]
            name = pj.get("name") or "UnnamedPreset"
            ruleset_id = pj.get("ruleset_id") or selection.get("ruleset_id") or "KC_WLAN"
            ruleset_version = pj.get("ruleset_version") or selection.get("ruleset_version") or "2026.02"
            desc = pj.get("description", "")

        return Preset(
            name=name,
            ruleset_id=ruleset_id,
            ruleset_version=ruleset_version,
            selection=selection,
            description=desc,
        )

    def load_override_objs(self, preset_id: str) -> List[OverrideRule]:
        rows = self.repo.list_overrides(preset_id)
        out: List[OverrideRule] = []
        for r in rows:
            j = r["json_data"]
            m = j.get("match", {})
            out.append(
                OverrideRule(
                    name=j.get("name", r["name"]),
                    enabled=bool(j.get("enabled", r["enabled"])),
                    priority=int(j.get("priority", r["priority"])),
                    match=Match(
                        band=m.get("band"),
                        standard=m.get("standard"),
                        test_type=m.get("test_type"),
                        channel=m.get("channel"),
                        bw_mhz=m.get("bw_mhz"),
                        group=m.get("group"),
                        segment=m.get("segment"),
                        device_class=m.get("device_class"),
                        channels=m.get("channels"),
                    ),
                    action=j["action"],
                    set_values=j.get("set_values", {}),
                )
            )
        return out

    # ---------- Recipe/Cases ----------
    def build_recipe_from_preset(self, preset_id: str) -> Tuple[RuleSet, Preset, Recipe, List[OverrideRule]]:
        """
        preset_id로부터 (ruleset, preset, recipe, overrides)를 구성합니다.

        ⚠️ 주의:
        - validate는 preset/ruleset을 모두 로드한 이후에 호출해야 합니다.
        - 이 함수의 반환값은 UI의 PlanContext에 그대로 들어가므로,
          여기서 실패하면 Add Plan에서 에러가 발생합니다.
        """
        # 1) preset 로드
        preset = self.load_preset_obj(preset_id)

        # 2) ruleset 로드 (preset이 어떤 규격을 참조하는지 기반)
        ruleset = self.load_ruleset(preset.ruleset_id)

        # 3) preset ↔ ruleset 정합성 검증
        self.validate_preset_against_ruleset(preset, ruleset)

        # 4) recipe 생성(=selection을 실제 케이스 생성 규칙으로 변환)
        recipe = build_recipe(ruleset, preset)

        # 5) overrides 로드(=UI에서 만든 예외/skip/파라미터 변경 규칙)
        overrides = self.load_override_objs(preset_id)

        return ruleset, preset, recipe, overrides

    def iter_cases(
        self,
        ruleset: RuleSet,
        recipe: Recipe,
        overrides: List[OverrideRule],
        filter_: Optional[Dict[str, Any]] = None,
        show_disabled: bool = False,
    ):
        cases = expand_recipe(ruleset, recipe)
        # show_disabled=True이면 skip override에 의해 제거되는 케이스도 화면에 표시하기 위해
        # skip 대신 tags['_disabled']=True로 마킹하여 그대로 반환합니다.
        if show_disabled:
            from domain.overrides import apply_overrides_mark_disabled
            cases = apply_overrides_mark_disabled(cases, overrides)
        else:
            cases = apply_overrides(cases, overrides)

        if not filter_:
            yield from cases
            return

        for c in cases:
            ok = True
            if "test_type" in filter_ and c.test_type != filter_["test_type"]:
                ok = False
            if "bw_mhz" in filter_ and c.bw_mhz != filter_["bw_mhz"]:
                ok = False
            if ok:
                yield c

    def get_cases_page(
        self,
        ruleset: RuleSet,
        recipe: Recipe,
        overrides: List[OverrideRule],
        filter_: Optional[Dict[str, Any]],
        offset: int,
        limit: int,
        show_disabled: bool = False,
    ) -> List[TestCase]:
        """
        MVP용 단순 페이징: iterator를 offset+limit까지 소비.
        (나중에 대규모 최적화는 별도 캐시/인덱싱으로 개선)
        """
        out: List[TestCase] = []
        it = self.iter_cases(ruleset, recipe, overrides, filter_, show_disabled=show_disabled)
        i = 0
        for c in it:
            if i >= offset and len(out) < limit:
                out.append(c)
            i += 1
            if len(out) >= limit:
                break
        return out

    # ---------- Override helpers ----------
    def create_skip_override_for_case(
        self,
        project_id: str,
        preset_id: str,
        case: TestCase,
        priority: int = 100,
    ) -> str:
        override_json = {
            "name": f"SKIP {case.test_type} CH{case.channel} BW{case.bw_mhz}",
            "enabled": True,
            "priority": priority,
            "match": {
                "band": case.band,
                "standard": case.standard,
                "test_type": case.test_type,
                "channel": case.channel,
                "bw_mhz": case.bw_mhz
            },
            "action": "skip",
            "set_values": {}
        }
        return self.repo.save_override(
            project_id=project_id,
            preset_id=preset_id,
            name=override_json["name"],
            override_json=override_json,
            priority=priority,
            enabled=True
        )
        
    def create_skip_override_for_selection(
    self,
    project_id: str,
    preset_id: str,
    cases: List[TestCase],
    priority: int = 100,
    ) -> str:
        if not cases:
            raise ValueError("No cases")

        first = cases[0]
        # 공통성 검사
        for c in cases[1:]:
            if (c.band, c.standard, c.test_type, c.bw_mhz) != (first.band, first.standard, first.test_type, first.bw_mhz):
                raise ValueError("Selection not homogeneous (band/standard/test_type/bw must match for grouped skip)")

        channels = sorted({c.channel for c in cases})

        override_json = {
            "name": f"SKIP {first.test_type} BW{first.bw_mhz} CH{channels[0]}..({len(channels)}ch)",
            "enabled": True,
            "priority": priority,
            "match": {
                "band": first.band,
                "standard": first.standard,
                "test_type": first.test_type,
                "bw_mhz": first.bw_mhz,
                "channels": channels
            },
            "action": "skip",
            "set_values": {}
        }
        return self.repo.save_override(
            project_id=project_id,
            preset_id=preset_id,
            name=override_json["name"],
            override_json=override_json,
            priority=priority,
            enabled=True
        )
        
    
    # ---------- Scenario Plan enable/disable ----------
    def disable_selected_cases(
        self,
        project_id: str,
        preset_id: str,
        cases: List[TestCase],
        priority: int = 100,
    ) -> List[str]:
        """
        선택된 케이스를 '비활성화(Disable)' 합니다.

        구현 방식(현재 MVP):
        - overrides 테이블에 action='skip' 규칙을 추가하여 해당 케이스가 expand 결과에서 제거되도록 함.
        - UI에서는 기본적으로 제거된 케이스는 보이지 않으며,
          'Show Disabled' 옵션을 켜면 tags['_disabled']=True로 표시된 상태로 다시 볼 수 있음.

        반환: 생성된 override_id 목록
        """
        if not cases:
            return []

        ids: List[str] = []
        # 가능하면 channels 묶음(skip selection)으로 1개 규칙으로 줄이고,
        # 조건이 섞여있으면 케이스별로 생성합니다.
        try:
            oid = self.create_skip_override_for_selection(project_id, preset_id, cases, priority=priority)
            ids.append(oid)
        except Exception:
            for c in cases:
                ids.append(self.create_skip_override_for_case(project_id, preset_id, c, priority=priority))
        return ids

    def enable_selected_cases(
        self,
        preset_id: str,
        cases: List[TestCase],
    ) -> int:
        """
        선택된 케이스를 다시 '활성화(Enable)' 합니다.

        구현 방식:
        - preset_id에 연결된 overrides 중 action='skip'이며
          match가 선택된 케이스(또는 channels 포함 규칙)에 매칭되는 rule을 찾아 enabled=False로 변경
        - hard delete 대신 enabled 플래그를 끄는 이유:
          나중에 어떤 케이스를 왜 제외했는지 추적(Traceability) 하기 위해.

        반환: 비활성화(disable)된 override 개수
        """
        if not cases:
            return 0

        # 선택 케이스를 빠르게 매칭하기 위한 key set
        case_keys = {(c.band, c.standard, c.test_type, c.channel, c.bw_mhz) for c in cases}

        rows = self.repo.list_overrides(preset_id)
        hit = 0
        for r in rows:
            j = r.get("json_data") or {}
            if j.get("action") != "skip":
                continue
            m = (j.get("match") or {})
            band = m.get("band")
            std = m.get("standard")
            tt = m.get("test_type")
            bw = m.get("bw_mhz")

            # 1) 단일 channel 매칭
            ch = m.get("channel")
            if ch is not None:
                if (band, std, tt, int(ch), int(bw)) in case_keys:
                    self.repo.update_override_enabled(r["override_id"], False)
                    hit += 1
                continue

            # 2) channels 리스트 매칭(그룹 skip)
            chs = m.get("channels")
            if chs:
                for ch2 in chs:
                    if (band, std, tt, int(ch2), int(bw)) in case_keys:
                        self.repo.update_override_enabled(r["override_id"], False)
                        hit += 1
                        break
        return hit

    def create_rerun_preset_from_fail(self, project_id: str, base_preset_id: str, run_id: str) -> str:
        base = self.repo.load_preset(base_preset_id)
        failed = self.run_repo.get_failed_cases(project_id, run_id)

        if not failed:
            raise ValueError("No FAIL cases found in this run.")

        # 실패 케이스에서 필요한 최소 정보만 모아서 re-run selection 구성
        test_types = sorted({r["test_type"] for r in failed})
        bw_list = sorted({int(r["bw_mhz"]) for r in failed})
        channels = sorted({int(r["channel"]) for r in failed})

        # base preset이 신포맷이면 selection을 복사, 구포맷이면 base 자체를 selection으로 취급
        if "selection" in base:
            selection = dict(base["selection"])
        else:
            selection = dict(base)

        selection["test_types"] = test_types
        selection["bandwidth_mhz"] = bw_list
        selection["channels"] = {
            "policy": "CUSTOM_LIST",
            "channels": channels
        }

        rerun_name = f"RERUN_{run_id[:8]}_{base.get('name', 'preset')}"
        rerun_json = {
            "name": rerun_name,
            "ruleset_id": base.get("ruleset_id", "KC_WLAN"),
            "ruleset_version": base.get("ruleset_version", "2026.02"),
            "selection": selection,
            "description": f"Auto-generated re-run from FAILs of run {run_id}"
        }

        new_preset_id = self.repo.save_preset(
            project_id=project_id,
            name=rerun_json["name"],
            ruleset_id=rerun_json["ruleset_id"],
            ruleset_version=rerun_json["ruleset_version"],
            preset_json=rerun_json,
        )
        return new_preset_id
    
    def create_rerun_preset_from_selected_results(
        self,
        project_id: str,
        base_preset_id: str,
        selected_rows: List[Dict[str, Any]],
    ) -> str:
        """
        selected_rows: Results 테이블의 선택된 row dict 목록
        반드시 포함: test_type, channel, bw_mhz
        band/standard는 base preset에서 가져옴(선택 row에 있어도 무방)
        """
        if not selected_rows:
            raise ValueError("No rows selected.")

        base = self.repo.load_preset(base_preset_id)

        # base preset이 신포맷이면 selection을 복사, 구포맷이면 base 자체를 selection으로 취급
        if "selection" in base:
            selection = dict(base["selection"])
        else:
            selection = dict(base)

        # 선택된 row에서 필요한 값들 집계
        test_types = sorted({r["test_type"] for r in selected_rows if r.get("test_type")})
        bw_list = sorted({int(r["bw_mhz"]) for r in selected_rows if r.get("bw_mhz") is not None})
        channels = sorted({int(r["channel"]) for r in selected_rows if r.get("channel") is not None})

        if not test_types or not bw_list or not channels:
            raise ValueError("Selected rows must include test_type, bw_mhz, channel.")

        # selection을 re-run 형태로 덮어쓰기
        selection["test_types"] = test_types
        selection["bandwidth_mhz"] = bw_list
        selection["channels"] = {
            "policy": "CUSTOM_LIST",
            "channels": channels
        }

        base_name = base.get("name", "preset")
        rerun_name = f"RERUN_SEL_{base_name}"

        rerun_json = {
            "name": rerun_name,
            "ruleset_id": base.get("ruleset_id", "KC_WLAN"),
            "ruleset_version": base.get("ruleset_version", "2026.02"),
            "selection": selection,
            "description": "Auto-generated re-run from selected results"
        }

        new_preset_id = self.repo.save_preset(
            project_id=project_id,
            name=rerun_json["name"],
            ruleset_id=rerun_json["ruleset_id"],
            ruleset_version=rerun_json["ruleset_version"],
            preset_json=rerun_json,
        )
        return new_preset_id
    
    def save_execution_order(self, preset_id: str, test_order: List[str]) -> None:
        pj = self.repo.load_preset(preset_id)

        # 신/구 포맷 모두 처리
        if "selection" not in pj:
            selection = dict(pj)
            pj = {
                "name": selection.get("name", "UnnamedPreset"),
                "ruleset_id": selection.get("ruleset_id", "KC_WLAN"),
                "ruleset_version": selection.get("ruleset_version", "2026.02"),
                "selection": selection,
                "description": selection.get("description", "")
            }

        sel = pj.setdefault("selection", {})
        sel["execution_policy"] = {
            "type": "CHANNEL_CENTRIC",
            "test_order": list(test_order),
            "include_bw_in_group": True
        }

        self.repo.update_preset_json(preset_id, pj)
        
    def load_preset_obj(self, preset_id: str) -> Preset:
        pj = self.repo.load_preset(preset_id)

        migrated, changed = migrate_preset_to_latest(pj)

        # ✅ 옵션: 개발 중에는 최신으로 자동 저장해 DB를 깨끗하게 유지
        if changed:
            self.repo.update_preset_json(preset_id, migrated)

        return Preset(
            name=migrated["name"],
            ruleset_id=migrated["ruleset_id"],
            ruleset_version=migrated["ruleset_version"],
            selection=migrated["selection"],
            description=migrated.get("description", ""),
    )
        
    def validate_preset_against_ruleset(self, preset, ruleset) -> None:
        """
        preset.selection 과 ruleset 내용을 비교해서
        - band 존재 여부
        - standard 지원 여부
        - test_types 지원 여부
        등을 검증한다.

        ✅ ruleset이 dict 기반이든, 객체(dataclass) 기반이든 모두 동작하도록 방어적으로 작성.
        """
        sel = preset.selection or {}

        band = sel.get("band")
        standard = sel.get("standard")
        test_types = sel.get("test_types", [])
        channels = sel.get("channels", {})

        if not band:
            raise ValueError("Preset selection is missing 'band'.")
        if not standard:
            raise ValueError("Preset selection is missing 'standard'.")
        if not isinstance(test_types, list) or not test_types:
            raise ValueError("Preset selection 'test_types' must be a non-empty list.")

        # --- ruleset.bands 가져오기 (dict/object 모두 지원) ---
        bands = getattr(ruleset, "bands", None)
        if bands is None:
            # ruleset 자체가 dict일 수도 있음
            if isinstance(ruleset, dict):
                bands = ruleset.get("bands", {})
            else:
                raise ValueError("Invalid ruleset: missing 'bands'")

        if band not in bands:
            # dict일 수도 있고 object일 수도 있으니 keys()를 안전하게
            try:
                available = list(bands.keys())
            except Exception:
                available = []
            raise ValueError(f"Band '{band}' is not defined in RuleSet. Available: {available}")

        band_info = bands[band]

        # --- band_info에서 standards/tests_supported 추출 (dict/object 모두 지원) ---
        if isinstance(band_info, dict):
            supported_standards = band_info.get("standards", [])
            supported_tests = band_info.get("tests_supported", [])
        else:
            supported_standards = getattr(band_info, "standards", [])
            supported_tests = getattr(band_info, "tests_supported", [])

        if standard not in supported_standards:
            raise ValueError(
                f"Standard '{standard}' not supported in band '{band}'. "
                f"Supported: {supported_standards}"
            )

        unsupported = [t for t in test_types if t not in supported_tests]
        if unsupported:
            raise ValueError(
                f"Unsupported test_types in band '{band}': {unsupported}. "
                f"Supported: {supported_tests}"
            )

        # --- channels 최소 검증 (policy만) ---
        if isinstance(channels, dict):
            policy = channels.get("policy")
            if policy == "CUSTOM_LIST":
                ch_list = channels.get("channels", [])
                if not ch_list:
                    raise ValueError("channels.policy is CUSTOM_LIST but channels.channels is empty.")