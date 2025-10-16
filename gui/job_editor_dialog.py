from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional, Set

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
)


class JobEditorDialog(QDialog):
    """Diálogo para crear/editar un Job.

    Campos:
    - id (string)
    - name (string)
    - on_error: stop | continue
    - max_parallel: usa default (checkbox) o valor numérico
    - stages: JSON (lista de etapas con keys: parallel, steps)
    """

    def __init__(
        self,
        parent=None,
        *,
        defaults_max_parallel: int = 1,
        existing_ids: Optional[Iterable[str]] = None,
        mode: str = "create",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Job")
        self._existing_ids: Set[str] = set(existing_ids or [])
        self._defaults_max_parallel = int(defaults_max_parallel or 1)
        self._mode = mode

        main = QVBoxLayout(self)
        form = QFormLayout()

        self.ed_id = QLineEdit()
        self.ed_name = QLineEdit()

        self.cb_on_error = QComboBox()
        self.cb_on_error.addItems(["stop", "continue"])

        mp_layout = QHBoxLayout()
        self.chk_default_mp = QCheckBox("Usar defaults.max_parallel")
        self.sp_max_parallel = QSpinBox()
        self.sp_max_parallel.setRange(1, 1_000)
        self.sp_max_parallel.setValue(self._defaults_max_parallel)
        self.sp_max_parallel.setEnabled(False)
        mp_layout.addWidget(self.chk_default_mp)
        mp_layout.addWidget(self.sp_max_parallel)
        self.chk_default_mp.setChecked(True)
        self.chk_default_mp.toggled.connect(self._toggle_mp)

        self.ed_stages = QTextEdit()
        self.ed_stages.setPlaceholderText(
            """[
  {
    "parallel": false,
    "steps": [
      {"etl_id": "etl1", "overrides": {"12.limit": 1000}},
      {"etl_id": "etl2"}
    ]
  },
  {
    "parallel": true,
    "steps": [
      {"etl_id": "etl3"},
      {"etl_id": "etl4"}
    ]
  }
]"""
        )
        form.addRow("ID:", self.ed_id)
        form.addRow("Nombre:", self.ed_name)
        form.addRow("On Error:", self.cb_on_error)
        form.addRow("Max Parallel:", mp_layout)
        form.addRow(QLabel("Stages (JSON):"))
        form.addRow(self.ed_stages)

        main.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        main.addWidget(btns)

    # ---- Slots ----
    def _toggle_mp(self, checked: bool) -> None:
        self.sp_max_parallel.setEnabled(not checked)

    def _on_accept(self) -> None:
        job_id = self.ed_id.text().strip()
        name = self.ed_name.text().strip()
        on_error = self.cb_on_error.currentText().strip() or "stop"
        use_default_mp = self.chk_default_mp.isChecked()
        mp_val: Optional[int] = None if use_default_mp else int(self.sp_max_parallel.value())

        if not job_id:
            QMessageBox.warning(self, "Validación", "El ID es obligatorio.")
            return
        if self._mode == "create" and job_id in self._existing_ids:
            QMessageBox.warning(self, "Validación", f"Ya existe un job con ID '{job_id}'.")
            return
        if not name:
            QMessageBox.warning(self, "Validación", "El nombre es obligatorio.")
            return

        # Validar stages
        try:
            stages_raw = self.ed_stages.toPlainText().strip() or "[]"
            stages = json.loads(stages_raw)
            if not isinstance(stages, list):
                raise ValueError("'stages' debe ser una lista")
            # Validación mínima de estructura
            for st in stages:
                if not isinstance(st, dict):
                    raise ValueError("Cada stage debe ser un objeto")
                steps = st.get("steps")
                if steps is None or not isinstance(steps, list):
                    raise ValueError("Cada stage debe tener un array 'steps'")
        except Exception as e:
            QMessageBox.critical(self, "Stages inválidos", f"Error de parseo/validación de stages: {e}")
            return

        self._job: Dict[str, Any] = {
            "id": job_id,
            "name": name,
            "on_error": on_error,
            "max_parallel": mp_val,
            "stages": stages,
        }
        self.accept()

    # ---- API ----
    def set_job(self, job: Dict[str, Any]) -> None:
        self._mode = "edit"
        self.ed_id.setText(str(job.get("id") or ""))
        self.ed_name.setText(str(job.get("name") or ""))
        on_error = str(job.get("on_error") or "stop")
        idx = self.cb_on_error.findText(on_error)
        if idx >= 0:
            self.cb_on_error.setCurrentIndex(idx)
        mp = job.get("max_parallel")
        if mp is None:
            self.chk_default_mp.setChecked(True)
            self.sp_max_parallel.setValue(self._defaults_max_parallel)
            self.sp_max_parallel.setEnabled(False)
        else:
            self.chk_default_mp.setChecked(False)
            try:
                self.sp_max_parallel.setValue(int(mp))
            except Exception:
                self.sp_max_parallel.setValue(self._defaults_max_parallel)
            self.sp_max_parallel.setEnabled(True)
        # Stages pretty
        try:
            self.ed_stages.setPlainText(json.dumps(job.get("stages") or [], ensure_ascii=False, indent=2))
        except Exception:
            self.ed_stages.setPlainText("[]")

    def get_job(self) -> Dict[str, Any]:
        return getattr(self, "_job", {})
