from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem,
    QTextEdit, QMessageBox
)
from PyQt6.QtCore import QTimer
import os
from threading import Thread, Lock
from typing import Any, Dict, List, Optional

from core.job_runner import JobRunner


class JobsTab(QWidget):
    """UI para listar y ejecutar Jobs definidos en el .fetl.
    Ejecuta en paralelo por etapas usando JobRunner y muestra logs.
    """

    def __init__(self, project_manager, parent=None):
        super().__init__(parent)
        self.pm = project_manager
        self._log_queue: List[str] = []
        self._log_lock = Lock()
        self.runner_thread: Optional[Thread] = None
        self.current_runner: Optional[JobRunner] = None
        self.running_row: Optional[int] = None

        layout = QVBoxLayout(self)

        # Controles superiores
        controls = QHBoxLayout()
        self.btn_refresh = QPushButton("Refrescar")
        self.btn_run = QPushButton("Ejecutar")
        self.btn_stop = QPushButton("Detener")
        self.btn_view_log = QPushButton("Ver último log")
        controls.addWidget(self.btn_refresh)
        controls.addWidget(self.btn_run)
        controls.addWidget(self.btn_stop)
        controls.addWidget(self.btn_view_log)
        controls.addStretch()
        layout.addLayout(controls)

        # Tabla de jobs
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels([
            "ID", "Nombre", "On Error", "Max Parallel", "# Etapas", "Estado"
        ])
        layout.addWidget(self.table)

        # Visor de logs
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMinimumHeight(180)
        layout.addWidget(self.log_view)

        # Timer para drenar logs desde thread
        self._log_timer = QTimer(self)
        self._log_timer.setInterval(200)
        self._log_timer.timeout.connect(self._drain_log_queue)

        # Señales
        self.btn_refresh.clicked.connect(self.refresh)
        self.btn_run.clicked.connect(self.run_selected)
        self.btn_stop.clicked.connect(self.stop_running)
        self.btn_view_log.clicked.connect(self.view_last_log)

        self.refresh()

    # ---------- Utilidades ----------
    def _writer(self, msg: str):
        with self._log_lock:
            self._log_queue.append(msg)

    def _drain_log_queue(self):
        msgs: List[str] = []
        with self._log_lock:
            if self._log_queue:
                msgs = self._log_queue[:]
                self._log_queue.clear()
        if msgs:
            self.log_view.append("\n".join(msgs))

    def _get_selected_row(self) -> Optional[int]:
        rows = self.table.selectionModel().selectedRows()
        if not rows:
            return None
        return rows[0].row()

    def _get_job_at_row(self, row: int) -> Optional[Dict[str, Any]]:
        jobs = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
        if 0 <= row < len(jobs):
            return jobs[row]
        return None

    def _set_status(self, row: int, status: str):
        if 0 <= row < self.table.rowCount():
            self.table.setItem(row, 5, QTableWidgetItem(status))

    # ---------- Acciones ----------
    def refresh(self):
        jobs = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
        self.table.setRowCount(len(jobs))
        for i, j in enumerate(jobs):
            jid = str(j.get('id') or '')
            name = str(j.get('name') or '')
            on_error = str(j.get('on_error') or 'stop')
            maxp = j.get('max_parallel')
            maxp_str = str(maxp) if maxp is not None else str((self.pm.project.get('defaults') or {}).get('max_parallel') or '')
            stages = j.get('stages') or []
            self.table.setItem(i, 0, QTableWidgetItem(jid))
            self.table.setItem(i, 1, QTableWidgetItem(name))
            self.table.setItem(i, 2, QTableWidgetItem(on_error))
            self.table.setItem(i, 3, QTableWidgetItem(maxp_str))
            self.table.setItem(i, 4, QTableWidgetItem(str(len(stages))))
            self.table.setItem(i, 5, QTableWidgetItem("idle"))

    def run_selected(self):
        if self.runner_thread is not None:
            QMessageBox.information(self, "Ejecución", "Ya hay un job ejecutándose.")
            return
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Jobs", "Selecciona un job en la tabla.")
            return
        job = self._get_job_at_row(row)
        if job is None:
            QMessageBox.warning(self, "Jobs", "Job inválido.")
            return
        self.log_view.clear()
        self._set_status(row, "running")
        self.running_row = row

        self.current_runner = JobRunner(self.pm.project, self.pm.logs_root(), ui_writer=self._writer)
        self._log_timer.start()

        def _target():
            try:
                result = self.current_runner.run_job(job)
                self._writer(f"[RESULT] success={result.get('success')} log={result.get('log_path')}")
            finally:
                self.current_runner = None
                self.runner_thread = None
        
        self.runner_thread = Thread(target=_target, daemon=True)
        self.runner_thread.start()

    def stop_running(self):
        if self.current_runner is not None:
            self.current_runner.request_stop()
            self._writer("[JOB] Solicitud de stop enviada")
        else:
            QMessageBox.information(self, "Ejecución", "No hay job en ejecución.")

    def view_last_log(self):
        row = self._get_selected_row()
        if row is None:
            QMessageBox.warning(self, "Jobs", "Selecciona un job en la tabla.")
            return
        job = self._get_job_at_row(row)
        if job is None:
            QMessageBox.warning(self, "Jobs", "Job inválido.")
            return
        name = str(job.get('name') or job.get('id') or 'job')
        logs_dir = os.path.join(self.pm.logs_root(), 'jobs', name)
        if not os.path.isdir(logs_dir):
            QMessageBox.information(self, "Logs", "No hay logs para este job todavía.")
            return
        try:
            files = [os.path.join(logs_dir, f) for f in os.listdir(logs_dir) if f.endswith('.log')]
            if not files:
                QMessageBox.information(self, "Logs", "No hay logs .log en la carpeta del job.")
                return
            latest = max(files, key=lambda p: os.path.getmtime(p))
            with open(latest, 'r', encoding='utf-8') as fh:
                content = fh.read()
            self.log_view.setPlainText(content)
        except Exception as e:
            QMessageBox.warning(self, "Logs", f"No se pudo leer el log: {e}")
