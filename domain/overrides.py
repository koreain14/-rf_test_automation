from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List

from .models import Match, OverrideRule, TestCase


def _match_case(c: TestCase, m: Match) -> bool:
    if m.band is not None and c.band != m.band:
        return False
    if m.standard is not None and c.standard != m.standard:
        return False
    if m.test_type is not None and c.test_type != m.test_type:
        return False
    if m.channel is not None and c.channel != m.channel:
        return False
    if m.bw_mhz is not None and c.bw_mhz != m.bw_mhz:
        return False
    if m.group is not None and c.tags.get("group") != m.group:
        return False
    if m.segment is not None and c.tags.get("segment") != m.segment:
        return False
    if m.device_class is not None and c.tags.get("device_class") != m.device_class:
        return False
    if m.channels is not None and c.channel not in m.channels:
        return False
    return True


def _apply_set_values(case: TestCase, set_values: Dict[str, Any]) -> TestCase:
    instr = dict(case.instrument)
    tags = dict(case.tags)

    for path, value in set_values.items():
        if path.startswith("instrument."):
            k = path.split(".", 1)[1]
            instr[k] = value
        elif path.startswith("tags."):
            k = path.split(".", 1)[1]
            tags[k] = value
        else:
            tags[path] = value

    return TestCase(
        test_type=case.test_type,
        band=case.band,
        standard=case.standard,
        channel=case.channel,
        center_freq_mhz=case.center_freq_mhz,
        bw_mhz=case.bw_mhz,
        instrument=instr,
        tags=tags,
        key=case.key,
    )


def apply_overrides(cases: Iterable[TestCase], overrides: List[OverrideRule]) -> Iterator[TestCase]:
    active = [o for o in overrides if o.enabled]
    active.sort(key=lambda o: o.priority)

    for c in cases:
        skip = False
        cur = c
        for o in active:
            if _match_case(cur, o.match):
                if o.action == "skip":
                    skip = True
                    break
                if o.action == "set":
                    cur = _apply_set_values(cur, o.set_values)
        if not skip:
            yield cur

def apply_overrides_mark_disabled(
    cases: Iterable[TestCase],
    overrides: List[OverrideRule],
) -> Iterator[TestCase]:
    """
    apply_overrides()의 확장 버전.

    - action == 'skip'에 매칭되는 케이스를 *제거하지 않고*,
      tags['_disabled']=True 로 표시한 뒤 그대로 yield 합니다.
    - UI에서 'Show Disabled' 옵션을 켰을 때 사용합니다.

    주의:
    - skip이 매칭되면 일반적으로 해당 케이스는 실행하면 안 되므로,
      set override는 적용하지 않고 disabled 마킹만 합니다(단순/명확).
    """
    active = [o for o in overrides if o.enabled]
    active.sort(key=lambda o: o.priority)

    for c in cases:
        disabled = False
        cur = c
        for o in active:
            if _match_case(cur, o.match):
                if o.action == "skip":
                    disabled = True
                    break
                if o.action == "set":
                    cur = _apply_set_values(cur, o.set_values)

        if disabled:
            tags = dict(cur.tags)
            tags["_disabled"] = True
            cur = TestCase(
                test_type=cur.test_type,
                band=cur.band,
                standard=cur.standard,
                channel=cur.channel,
                center_freq_mhz=cur.center_freq_mhz,
                bw_mhz=cur.bw_mhz,
                instrument=cur.instrument,
                tags=tags,
                key=cur.key,
            )

        yield cur
