from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional, Set

from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QTextEdit,
    QVBoxLayout,
)


class EtlEditorDialog(QDialog):
    """Diálogo para crear/editar un ETL embebido en el proyecto.

    Campos:
    - id (string)
    - name (string)
    - content (JSON con keys: nodes, edges)
    """

    def __init__(
        self,
        parent=None,
        *,
        existing_ids: Optional[Iterable[str]] = None,
        mode: str = "create",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("ETL")
        self._existing_ids: Set[str] = set(existing_ids or [])
        self._mode = mode

        main = QVBoxLayout(self)
        form = QFormLayout()

        self.ed_id = QLineEdit()
        self.ed_name = QLineEdit()

        self.txt_content = QTextEdit()
        self.txt_content.setPlaceholderText(
            """{
  "nodes": [
    {"id": 1, "type": "source", "config": {"subtype": "csv", "path": "file.csv"}}
  ],
  "edges": [
    {"source": 1, "target": 2}
  ]
}"""
        )

        form.addRow("ID:", self.ed_id)
        form.addRow("Nombre:", self.ed_name)
        form.addRow(QLabel("Contenido (JSON):"))
        form.addRow(self.txt_content)

        main.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        main.addWidget(btns)

    def _on_accept(self) -> None:
        etl_id = self.ed_id.text().strip()
        name = self.ed_name.text().strip()
        if not etl_id:
            QMessageBox.warning(self, "Validación", "El ID es obligatorio.")
            return
        if self._mode == "create" and etl_id in self._existing_ids:
            QMessageBox.warning(self, "Validación", f"Ya existe un ETL con ID '{etl_id}'.")
            return
        if not name:
            QMessageBox.warning(self, "Validación", "El nombre es obligatorio.")
            return
        # Validar contenido JSON
        try:
            content_raw = self.txt_content.toPlainText().strip() or "{}"
            content = json.loads(content_raw)
            if not isinstance(content, dict):
                raise ValueError("El contenido debe ser un objeto JSON")
            nodes = content.get("nodes")
            edges = content.get("edges")
            if nodes is None or edges is None:
                raise ValueError("El contenido debe incluir 'nodes' y 'edges'")
            if not isinstance(nodes, list) or not isinstance(edges, list):
                raise ValueError("'nodes' y 'edges' deben ser listas")
        except Exception as e:
            QMessageBox.critical(self, "Contenido inválido", f"Error de parseo/validación: {e}")
            return
        self._etl: Dict[str, Any] = {
            "id": etl_id,
            "name": name,
            "content": content,
        }
        self.accept()

    def set_etl(self, etl: Dict[str, Any]) -> None:
        self._mode = "edit"
        self.ed_id.setText(str(etl.get("id") or ""))
        self.ed_name.setText(str(etl.get("name") or ""))
        try:
            self.txt_content.setPlainText(json.dumps(etl.get("content") or {}, ensure_ascii=False, indent=2))
        except Exception:
            self.txt_content.setPlainText("{}")

    def get_etl(self) -> Dict[str, Any]:
        return getattr(self, "_etl", {})
