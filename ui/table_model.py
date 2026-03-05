from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from domain.models import TestCase


class CaseTableModel(QAbstractTableModel):
    HEADERS = ["On", "Test", "Band", "Std", "Group", "Ch", "Freq(MHz)", "BW", "RBW", "VBW", "Detector", "Trace", "Key"]

    def __init__(self):
        super().__init__()
        self._rows: List[TestCase] = []

    def clear(self):
        self.beginResetModel()
        self._rows = []
        self.endResetModel()

    def append_rows(self, rows: List[TestCase]):
        if not rows:
            return
        start = len(self._rows)
        end = start + len(rows) - 1
        self.beginInsertRows(QModelIndex(), start, end)
        self._rows.extend(rows)
        self.endInsertRows()

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._rows)

    def columnCount(self, parent=QModelIndex()) -> int:
        return len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return str(section + 1)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        c = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            # tags['_disabled']=True 인 케이스는 Scenario Plan에서 제외(Disable)된 상태
            if col == 0:  return "OFF" if c.tags.get("_disabled") else "ON"
            if col == 1:  return c.test_type
            if col == 2:  return c.band
            if col == 3:  return c.standard
            if col == 4:  return c.tags.get("group", "")
            if col == 5:  return str(c.channel)
            if col == 6:  return f"{c.center_freq_mhz:.0f}" if c.center_freq_mhz else ""
            if col == 7:  return str(c.bw_mhz)
            if col == 8:  return str(c.instrument.get("rbw_hz", ""))
            if col == 9:  return str(c.instrument.get("vbw_hz", ""))
            if col == 10: return str(c.instrument.get("detector", ""))
            if col == 11: return str(c.instrument.get("trace_mode", ""))
            if col == 12: return c.key

        return None

    def get_case(self, row: int) -> Optional[TestCase]:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None