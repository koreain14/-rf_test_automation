from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt


class ResultsTableModel(QAbstractTableModel):
    HEADERS = ["Status", "Test", "Band", "Std", "Group", "Ch", "BW", "Margin(dB)", "Key"]

    def __init__(self):
        super().__init__()
        self._rows: List[Dict[str, Any]] = []

    def set_rows(self, rows: List[Dict[str, Any]]):
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()

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
        r = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == 0: return r.get("status", "")
            if col == 1: return r.get("test_type", "")
            if col == 2: return r.get("band", "")
            if col == 3: return r.get("standard", "")
            if col == 4: return r.get("group", "")
            if col == 5: return str(r.get("channel", ""))
            if col == 6: return str(r.get("bw_mhz", ""))
            if col == 7:
                m = r.get("margin_db")
                return "" if m is None else f"{m:.2f}"
            if col == 8: return r.get("test_key", "")
        return None
    
    def get_row(self, row: int) -> dict:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None