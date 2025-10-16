from __future__ import annotations

import os
from typing import Any, Dict, List

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QMessageBox
)


class RunsTab(QWidget):
    """Muestra el historial de ejecuciones (resumenes) guardados en `project['runs']`.
    Permite abrir el archivo de log asociado.
    """

    def __init__(self, project_manager, parent=None):
        super().__init__(parent)
        self.pm = project_manager

        layout = QVBoxLayout(self)
        controls = QHBoxLayout()
        self.btn_refresh = QPushButton("Refrescar")
        self.btn_open_log = QPushButton("Abrir log")
        controls.addWidget(self.btn_refresh)
        controls.addWidget(self.btn_open_log)
        controls.addStretch()
        layout.addLayout(controls)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["TS", "Tipo", "ID", "Nombre", "OK", "Log Path"])
        layout.addWidget(self.table)

        self.btn_refresh.clicked.connect(self.refresh)
        self.btn_open_log.clicked.connect(self.open_log)

        self.refresh()

    def refresh(self):
        runs: List[Dict[str, Any]] = []
        if isinstance(self.pm.project, dict):
            runs = list(self.pm.project.get('runs') or [])
        self.table.setRowCount(len(runs))
        for i, r in enumerate(runs):
            ts = str(r.get('ts') or '')
            t = str(r.get('type') or '')
            rid = str(r.get('id') or '')
            name = str(r.get('name') or '')
            ok = '✔' if r.get('ok') else ''
            lp = str(r.get('log_path') or '')
            self.table.setItem(i, 0, QTableWidgetItem(ts))
            self.table.setItem(i, 1, QTableWidgetItem(t))
            self.table.setItem(i, 2, QTableWidgetItem(rid))
            self.table.setItem(i, 3, QTableWidgetItem(name))
            self.table.setItem(i, 4, QTableWidgetItem(ok))
            self.table.setItem(i, 5, QTableWidgetItem(lp))

    def _selected_row(self) -> int:
        rows = self.table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def open_log(self):
        row = self._selected_row()
        if row < 0 or row >= self.table.rowCount():
            QMessageBox.information(self, "Runs", "Selecciona una ejecución para abrir su log.")
            return
        item = self.table.item(row, 5)
        log_path = item.text().strip() if item else ''
        if not log_path:
            QMessageBox.information(self, "Runs", "No hay log_path para esta ejecución.")
            return
        if not os.path.exists(log_path):
            QMessageBox.warning(self, "Runs", f"El archivo de log no existe:\n{log_path}")
            return
        try:
            with open(log_path, 'r', encoding='utf-8') as fh:
                content = fh.read()
            # Mostrar en diálogo simple
            from PyQt6.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QDialogButtonBox
            dlg = QDialog(self)
            dlg.setWindowTitle("Log de ejecución")
            v = QVBoxLayout(dlg)
            te = QTextEdit()
            te.setReadOnly(True)
            te.setPlainText(content)
            v.addWidget(te)
            btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
            btns.rejected.connect(dlg.reject)
            btns.accepted.connect(dlg.accept)
            v.addWidget(btns)
            dlg.resize(900, 600)
            dlg.exec()
        except Exception as e:
            QMessageBox.warning(self, "Runs", f"No se pudo abrir el log: {e}")
