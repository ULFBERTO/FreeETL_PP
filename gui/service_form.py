from __future__ import annotations

from typing import Any, Dict, List, Optional, Callable

from PyQt6.QtWidgets import (
    QWidget, QFormLayout, QLabel, QLineEdit, QSpinBox, QMessageBox
)


class ServiceConfigForm(QWidget):
    """Formulario simple para configurar un Servicio del proyecto (.fetl).

    Campos editables:
    - name (str)
    - port (int)
    - basic_user (str)
    - basic_pass (str)
    - jwt_secret (str)
    """

    def __init__(self, project_manager, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.pm = project_manager
        self._loading: bool = False
        self._current_index: Optional[int] = None
        self.on_change: Optional[Callable[[], None]] = None

        lay = QFormLayout(self)

        self.lbl_id = QLabel("")
        self.ed_name = QLineEdit()
        self.spn_port = QSpinBox()
        self.spn_port.setMinimum(1)
        self.spn_port.setMaximum(65535)
        self.ed_basic_user = QLineEdit()
        self.ed_basic_pass = QLineEdit()
        self.ed_basic_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.ed_jwt_secret = QLineEdit()
        self.ed_jwt_secret.setEchoMode(QLineEdit.EchoMode.Password)

        lay.addRow("ID:", self.lbl_id)
        lay.addRow("Nombre:", self.ed_name)
        lay.addRow("Puerto:", self.spn_port)
        lay.addRow("Basic User:", self.ed_basic_user)
        lay.addRow("Basic Pass:", self.ed_basic_pass)
        lay.addRow("JWT Secret:", self.ed_jwt_secret)

        # Señales de edición
        self.ed_name.editingFinished.connect(self._apply_save)
        self.spn_port.valueChanged.connect(self._apply_save)
        self.ed_basic_user.editingFinished.connect(self._apply_save)
        self.ed_basic_pass.editingFinished.connect(self._apply_save)
        self.ed_jwt_secret.editingFinished.connect(self._apply_save)

        self._set_enabled(False)

    def _set_enabled(self, enabled: bool):
        self.ed_name.setEnabled(enabled)
        self.spn_port.setEnabled(enabled)
        self.ed_basic_user.setEnabled(enabled)
        self.ed_basic_pass.setEnabled(enabled)
        self.ed_jwt_secret.setEnabled(enabled)

    def clear(self):
        self._loading = True
        try:
            self.lbl_id.setText("")
            self.ed_name.setText("")
            default_port = 8080
            try:
                default_port = int((self.pm.project.get('defaults') or {}).get('services', {}).get('port') or 8080)
            except Exception:
                default_port = 8080
            self.spn_port.setValue(default_port)
            self.ed_basic_user.setText("")
            self.ed_basic_pass.setText("")
            self.ed_jwt_secret.setText("")
            self._current_index = None
            self._set_enabled(False)
        finally:
            self._loading = False

    def load_service_by_id(self, service_id: str) -> bool:
        """Carga un servicio por ID y muestra sus valores en el formulario.
        Devuelve True si encontró el servicio, False en caso contrario.
        """
        services: List[Dict[str, Any]] = (self.pm.project.get('services') or []) if isinstance(self.pm.project, dict) else []
        idx = -1
        for i, s in enumerate(services):
            if str(s.get('id')) == str(service_id):
                idx = i
                break
        if idx < 0:
            self.clear()
            return False

        self._loading = True
        try:
            svc = services[idx]
            self._current_index = idx
            self.lbl_id.setText(str(svc.get('id') or ''))
            self.ed_name.setText(str(svc.get('name') or ''))
            port = svc.get('port')
            if port is None:
                port = (self.pm.project.get('defaults') or {}).get('services', {}).get('port') or 8080
            self.spn_port.setValue(int(port))
            auth = svc.get('auth') or {}
            self.ed_basic_user.setText(str(auth.get('basic_user') or ''))
            self.ed_basic_pass.setText(str(auth.get('basic_pass') or ''))
            self.ed_jwt_secret.setText(str(auth.get('jwt_secret') or ''))
            self._set_enabled(True)
        finally:
            self._loading = False
        return True

    def _apply_save(self):
        if self._loading:
            return
        if self._current_index is None:
            return
        try:
            services: List[Dict[str, Any]] = (self.pm.project.get('services') or []) if isinstance(self.pm.project, dict) else []
            if not (0 <= self._current_index < len(services)):
                return
            svc = dict(services[self._current_index])
            svc['name'] = self.ed_name.text().strip()
            svc['port'] = int(self.spn_port.value())
            auth = dict(svc.get('auth') or {})
            auth['basic_user'] = self.ed_basic_user.text().strip()
            auth['basic_pass'] = self.ed_basic_pass.text().strip()
            auth['jwt_secret'] = self.ed_jwt_secret.text().strip()
            svc['auth'] = auth
            services[self._current_index] = svc
            self.pm.project['services'] = services
            try:
                self.pm.save()
            except Exception:
                pass
            if self.on_change:
                try:
                    self.on_change()
                except Exception:
                    pass
        except Exception as e:
            QMessageBox.warning(self, "Servicios", f"No se pudo guardar el Servicio: {e}")
