from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QSpinBox,
    QVBoxLayout,
)


class ServiceEditorDialog(QDialog):
    """Diálogo para crear/editar un Servicio del proyecto.

    Campos:
    - id (string)
    - name (string)
    - port (usar default o valor)
    - auth: basic_user, basic_pass, jwt_secret (opcionales)
    """

    def __init__(
        self,
        parent=None,
        *,
        default_port: int = 8080,
        existing_ids: Optional[Iterable[str]] = None,
        mode: str = "create",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Servicio")
        self._existing_ids: Set[str] = set(existing_ids or [])
        self._default_port = int(default_port or 8080)
        self._mode = mode

        main = QVBoxLayout(self)
        form = QFormLayout()

        # Identificación
        self.ed_id = QLineEdit()
        self.ed_name = QLineEdit()
        form.addRow("ID:", self.ed_id)
        form.addRow("Nombre:", self.ed_name)

        # Puerto
        self.chk_default_port = QCheckBox("Usar puerto por defecto")
        self.chk_default_port.setChecked(True)
        self.sp_port = QSpinBox()
        self.sp_port.setRange(1, 65535)
        self.sp_port.setValue(self._default_port)
        self.sp_port.setEnabled(False)
        port_layout = QHBoxLayout()
        port_layout.addWidget(self.chk_default_port)
        port_layout.addWidget(self.sp_port)
        self.chk_default_port.toggled.connect(lambda ch: self.sp_port.setEnabled(not ch))
        form.addRow("Puerto:", port_layout)

        # Auth (Basic)
        self.ed_basic_user = QLineEdit()
        self.ed_basic_pass = QLineEdit()
        self.ed_basic_pass.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Basic User:", self.ed_basic_user)
        form.addRow("Basic Pass:", self.ed_basic_pass)

        # Auth (JWT)
        self.ed_jwt_secret = QLineEdit()
        self.ed_jwt_secret.setPlaceholderText("Opcional. Define para habilitar /token y Bearer auth")
        form.addRow("JWT Secret:", self.ed_jwt_secret)

        main.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        main.addWidget(btns)

    def _on_accept(self) -> None:
        sid = self.ed_id.text().strip()
        name = self.ed_name.text().strip()
        if not sid:
            QMessageBox.warning(self, "Validación", "El ID es obligatorio.")
            return
        if self._mode == "create" and sid in self._existing_ids:
            QMessageBox.warning(self, "Validación", f"Ya existe un servicio con ID '{sid}'.")
            return
        if not name:
            QMessageBox.warning(self, "Validación", "El nombre es obligatorio.")
            return

        port_val = None if self.chk_default_port.isChecked() else int(self.sp_port.value())
        auth: Dict[str, Any] = {}
        if self.ed_basic_user.text().strip():
            auth['basic_user'] = self.ed_basic_user.text().strip()
        if self.ed_basic_pass.text().strip():
            auth['basic_pass'] = self.ed_basic_pass.text().strip()
        if self.ed_jwt_secret.text().strip():
            auth['jwt_secret'] = self.ed_jwt_secret.text().strip()

        self._service: Dict[str, Any] = {
            'id': sid,
            'name': name,
            'port': port_val,
            'auth': auth,
        }
        self.accept()

    def set_service(self, svc: Dict[str, Any], default_port: Optional[int] = None) -> None:
        self._mode = "edit"
        self.ed_id.setText(str(svc.get('id') or ''))
        self.ed_name.setText(str(svc.get('name') or ''))
        port = svc.get('port')
        if port is None:
            self.chk_default_port.setChecked(True)
            self.sp_port.setValue(int(default_port or self._default_port))
            self.sp_port.setEnabled(False)
        else:
            self.chk_default_port.setChecked(False)
            try:
                self.sp_port.setValue(int(port))
            except Exception:
                self.sp_port.setValue(int(default_port or self._default_port))
            self.sp_port.setEnabled(True)
        auth = svc.get('auth') or {}
        self.ed_basic_user.setText(str(auth.get('basic_user') or ''))
        self.ed_basic_pass.setText(str(auth.get('basic_pass') or ''))
        self.ed_jwt_secret.setText(str(auth.get('jwt_secret') or ''))

    def get_service(self) -> Dict[str, Any]:
        return getattr(self, '_service', {})
