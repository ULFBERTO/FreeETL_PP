from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
from PyQt6.QtCore import QTimer
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem,
    QTextEdit, QMessageBox, QInputDialog
)

from core.service_runner import ServiceRunner


class ServicesTab(QWidget):
    """UI para listar y controlar Servicios del proyecto (.fetl)."""

    def __init__(self, project_manager, parent=None):
        super().__init__(parent)
        self.pm = project_manager
        self.runners: Dict[str, ServiceRunner] = {}
        self._bearer_tokens: Dict[str, str] = {}

        layout = QVBoxLayout(self)

        # Controles superiores
        controls = QHBoxLayout()
        self.btn_refresh = QPushButton("Refrescar")
        self.btn_start = QPushButton("Iniciar")
        self.btn_stop = QPushButton("Detener")
        self.btn_open_health = QPushButton("Abrir /health")
        self.btn_mcp_models = QPushButton("Modelos MCP")
        self.btn_mcp_chat = QPushButton("Chat MCP")
        self.btn_get_jwt = QPushButton("Obtener JWT")
        self.btn_run_etl = QPushButton("Ejecutar ETL")
        self.btn_run_job = QPushButton("Ejecutar Job")
        self.btn_view_log = QPushButton("Ver último log")
        controls.addWidget(self.btn_refresh)
        controls.addWidget(self.btn_start)
        controls.addWidget(self.btn_stop)
        controls.addWidget(self.btn_open_health)
        controls.addWidget(self.btn_mcp_models)
        controls.addWidget(self.btn_mcp_chat)
        controls.addWidget(self.btn_get_jwt)
        controls.addWidget(self.btn_run_etl)
        controls.addWidget(self.btn_run_job)
        controls.addWidget(self.btn_view_log)
        controls.addStretch()
        layout.addLayout(controls)

        # Tabla de servicios
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels([
            "ID", "Nombre", "Puerto", "Estado", "Basic", "JWT"
        ])
        layout.addWidget(self.table)

        # Visor de logs
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMinimumHeight(160)
        layout.addWidget(self.log_view)

        # Timer para refrescar estados de runners
        self._status_timer = QTimer(self)
        self._status_timer.setInterval(500)
        self._status_timer.timeout.connect(self._refresh_runner_statuses)
        self._status_timer.start()

        # Señales
        self.btn_refresh.clicked.connect(self.refresh)
        self.btn_start.clicked.connect(self.start_selected)
        self.btn_stop.clicked.connect(self.stop_selected)
        self.btn_open_health.clicked.connect(self.open_health)
        self.btn_mcp_models.clicked.connect(self.list_mcp_models)
        self.btn_mcp_chat.clicked.connect(self.chat_mcp)
        self.btn_get_jwt.clicked.connect(self.get_jwt)
        self.btn_run_etl.clicked.connect(self.run_etl)
        self.btn_run_job.clicked.connect(self.run_job)
        self.btn_view_log.clicked.connect(self.view_last_log)

        self.refresh()

    # ---------- Utilidades ----------
    def _writer(self, msg: str):
        self.log_view.append(msg)

    def _get_selected_row(self) -> Optional[int]:
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            return None
        return rows[0].row()

    def _services(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('services') or []) if isinstance(self.pm.project, dict) else []

    def _etls(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('etls') or []) if isinstance(self.pm.project, dict) else []

    def _jobs(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []

    def _get_service_at_row(self, row: int) -> Optional[Dict[str, Any]]:
        svcs = self._services()
        if 0 <= row < len(svcs):
            return svcs[row]
        return None

    def _get_port_for_service(self, svc: Dict[str, Any]) -> int:
        port = svc.get('port')
        if port is not None:
            try:
                return int(port)
            except Exception:
                pass
        dflt = (self.pm.project.get('defaults') or {}).get('services', {}) if isinstance(self.pm.project, dict) else {}
        return int(dflt.get('port', 8080))

    def _get_requests_auth(self, svc: Dict[str, Any]):
        auth = svc.get('auth') or {}
        user = auth.get('basic_user')
        pwd = auth.get('basic_pass')
        if user and pwd:
            return (str(user), str(pwd))
        return None

    def _svc_key(self, svc: Dict[str, Any]) -> str:
        return str(svc.get('id') or svc.get('name') or 'service')

    def _get_bearer_token(self, svc: Dict[str, Any]) -> Optional[str]:
        return self._bearer_tokens.get(self._svc_key(svc))

    def _build_headers(self, svc: Dict[str, Any]) -> Optional[Dict[str, str]]:
        tok = self._get_bearer_token(svc)
        if tok:
            return {"Authorization": f"Bearer {tok}"}
        return None

    def _get_runner(self, svc: Dict[str, Any]) -> ServiceRunner:
        sid = str(svc.get('id') or svc.get('name') or 'service')
        r = self.runners.get(sid)
        if r is None:
            r = ServiceRunner(self.pm.project, svc, self.pm.logs_root(), ui_writer=self._writer)
            self.runners[sid] = r
        return r

    def _set_status(self, row: int, status: str):
        if 0 <= row < self.table.rowCount():
            self.table.setItem(row, 3, QTableWidgetItem(status))

    def _refresh_runner_statuses(self):
        svcs = self._services()
        for i, s in enumerate(svcs):
            sid = str(s.get('id') or s.get('name') or 'service')
            r = self.runners.get(sid)
            if r is None:
                self._set_status(i, 'stopped')
            else:
                self._set_status(i, 'running' if r.is_running() else 'stopped')

    # ---------- Acciones ----------
    def refresh(self):
        svcs = self._services()
        self.table.setRowCount(len(svcs))
        for i, s in enumerate(svcs):
            sid = str(s.get('id') or '')
            name = str(s.get('name') or '')
            port = self._get_port_for_service(s)
            auth = s.get('auth') or {}
            basic = '✔' if auth.get('basic_user') and auth.get('basic_pass') else ''
            jwt = '✔' if auth.get('jwt_secret') else ''
            self.table.setItem(i, 0, QTableWidgetItem(sid))
            self.table.setItem(i, 1, QTableWidgetItem(name))
            self.table.setItem(i, 2, QTableWidgetItem(str(port)))
            self.table.setItem(i, 3, QTableWidgetItem('stopped'))
            self.table.setItem(i, 4, QTableWidgetItem(basic))
            self.table.setItem(i, 5, QTableWidgetItem(jwt))

    def start_selected(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        runner = self._get_runner(svc)
        runner.start()
        self._set_status(row, 'running')
        self._writer(f"[UI] Servicio '{svc.get('id')}' iniciado")

    def stop_selected(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        sid = str(svc.get('id') or svc.get('name') or 'service')
        runner = self.runners.get(sid)
        if runner is None:
            QMessageBox.information(self, "Servicios", "El servicio no está corriendo.")
            return
        runner.stop()
        self._set_status(row, 'stopped')
        self._writer(f"[UI] Servicio '{svc.get('id')}' detenido")

    def open_health(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/health"
        auth = self._get_requests_auth(svc)
        headers = self._build_headers(svc)
        try:
            if headers:
                r = requests.get(url, timeout=2, headers=headers)
            else:
                r = requests.get(url, timeout=2, auth=auth)
            self._writer(f"[HTTP] GET {url} -> {r.status_code} {r.text}")
        except Exception as e:
            self._writer(f"[HTTP] GET {url} error: {e}")

    def list_mcp_models(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/mcp/models"
        auth = self._get_requests_auth(svc)
        headers = self._build_headers(svc)
        try:
            if headers:
                r = requests.get(url, timeout=4, headers=headers)
            else:
                r = requests.get(url, timeout=4, auth=auth)
            self._writer(f"[HTTP] GET {url} -> {r.status_code} {r.text}")
        except Exception as e:
            self._writer(f"[HTTP] GET {url} error: {e}")

    def chat_mcp(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        # Prompt input
        ok = False
        prompt, ok = QInputDialog.getText(self, "Chat MCP", "Prompt:")
        if not ok or not prompt.strip():
            return
        # Optional lang override
        defaults = (self.pm.project.get('defaults') or {}) if isinstance(self.pm.project, dict) else {}
        default_lang = ((defaults.get('mcp') or {}).get('lang')) or 'es'
        lang, ok2 = QInputDialog.getText(self, "Chat MCP", f"Idioma (enter para default='{default_lang}'): ")
        if not ok2:
            return
        lang = lang.strip() or default_lang
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/mcp/chat?lang={lang}"
        auth = self._get_requests_auth(svc)
        headers = self._build_headers(svc)
        try:
            if headers:
                r = requests.post(url, json={'prompt': prompt}, timeout=15, headers=headers)
            else:
                r = requests.post(url, json={'prompt': prompt}, timeout=15, auth=auth)
            self._writer(f"[HTTP] POST {url} -> {r.status_code} {r.text}")
        except Exception as e:
            self._writer(f"[HTTP] POST {url} error: {e}")

    def get_jwt(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        # Requiere Basic
        auth = self._get_requests_auth(svc)
        if not auth:
            QMessageBox.information(self, "JWT", "Configura usuario y contraseña Basic en el servicio.")
            return
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/token"
        try:
            r = requests.post(url, timeout=6, auth=auth)
            if not r.ok:
                self._writer(f"[HTTP] POST {url} -> {r.status_code} {r.text}")
                QMessageBox.warning(self, "JWT", f"No se pudo obtener token: {r.status_code}")
                return
            tok = (r.json() or {}).get('access_token')
            if not tok:
                QMessageBox.warning(self, "JWT", "Respuesta sin token")
                return
            self._bearer_tokens[self._svc_key(svc)] = tok
            self._writer("[JWT] Token almacenado para el servicio seleccionado")
            QMessageBox.information(self, "JWT", "Token obtenido y almacenado. Se usará Bearer para las próximas llamadas.")
        except Exception as e:
            self._writer(f"[HTTP] POST {url} error: {e}")

    def view_last_log(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        name = str(svc.get('name') or svc.get('id') or 'service')
        logs_dir = os.path.join(self.pm.logs_root(), 'services', name)
        if not os.path.isdir(logs_dir):
            QMessageBox.information(self, "Logs", "No hay logs para este servicio todavía.")
            return
        try:
            files = [os.path.join(logs_dir, f) for f in os.listdir(logs_dir) if f.endswith('.log')]
            if not files:
                QMessageBox.information(self, "Logs", "No hay logs .log en la carpeta del servicio.")
                return
            latest = max(files, key=lambda p: os.path.getmtime(p))
            with open(latest, 'r', encoding='utf-8') as fh:
                content = fh.read()
            self.log_view.setPlainText(content)
        except Exception as e:
            QMessageBox.warning(self, "Logs", f"No se pudo leer el log: {e}")

    def run_etl(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        etls = self._etls()
        if not etls:
            QMessageBox.information(self, "ETLs", "No hay ETLs en el proyecto.")
            return
        items = [str(e.get('id') or '') for e in etls if e.get('id')]
        if not items:
            QMessageBox.information(self, "ETLs", "No hay IDs de ETL válidos.")
            return
        etl_id, ok = QInputDialog.getItem(self, "Ejecutar ETL", "Selecciona ETL:", items, 0, False)
        if not ok or not etl_id:
            return
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/etl/run"
        auth = self._get_requests_auth(svc)
        headers = self._build_headers(svc)
        try:
            if headers:
                r = requests.post(url, json={'etl_id': etl_id}, timeout=120, headers=headers)
            else:
                r = requests.post(url, json={'etl_id': etl_id}, timeout=120, auth=auth)
            self._writer(f"[HTTP] POST {url} -> {r.status_code} {r.text}")
            if r.ok:
                log_path = (r.json() or {}).get('log_path')
                if log_path and os.path.exists(log_path):
                    with open(log_path, 'r', encoding='utf-8') as fh:
                        self.log_view.setPlainText(fh.read())
        except Exception as e:
            self._writer(f"[HTTP] POST {url} error: {e}")

    def run_job(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Servicios", "Selecciona un servicio en la tabla.")
            return
        svc = self._get_service_at_row(row)
        if svc is None:
            QMessageBox.warning(self, "Servicios", "Servicio inválido.")
            return
        jobs = self._jobs()
        if not jobs:
            QMessageBox.information(self, "Jobs", "No hay Jobs en el proyecto.")
            return
        items = [str(j.get('id') or '') for j in jobs if j.get('id')]
        if not items:
            QMessageBox.information(self, "Jobs", "No hay IDs de Job válidos.")
            return
        job_id, ok = QInputDialog.getItem(self, "Ejecutar Job", "Selecciona Job:", items, 0, False)
        if not ok or not job_id:
            return
        port = self._get_port_for_service(svc)
        url = f"http://127.0.0.1:{port}/job/run"
        auth = self._get_requests_auth(svc)
        headers = self._build_headers(svc)
        try:
            if headers:
                r = requests.post(url, json={'job_id': job_id}, timeout=600, headers=headers)
            else:
                r = requests.post(url, json={'job_id': job_id}, timeout=600, auth=auth)
            self._writer(f"[HTTP] POST {url} -> {r.status_code} {r.text}")
            if r.ok:
                log_path = (r.json() or {}).get('log_path')
                if log_path and os.path.exists(log_path):
                    with open(log_path, 'r', encoding='utf-8') as fh:
                        self.log_view.setPlainText(fh.read())
        except Exception as e:
            self._writer(f"[HTTP] POST {url} error: {e}")
