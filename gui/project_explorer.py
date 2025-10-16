#
from __future__ import annotations

import json
from typing import Any, Dict, List

from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QMessageBox, QTabWidget,
    QAbstractItemView
)

from core.project_manager import ProjectManager
from .job_editor_dialog import JobEditorDialog
from .etl_editor_dialog import EtlEditorDialog
from .service_editor_dialog import ServiceEditorDialog


class ProjectExplorer(QWidget):
    """Pestaña para explorar/gestionar elementos del proyecto (.fetl).

    Primera versión: gestiona Jobs (CRUD). En iteraciones futuras: ETLs/Services.
    """

    project_changed = pyqtSignal(dict)
    load_etl_requested = pyqtSignal(str)
    save_etl_requested = pyqtSignal(str)
    job_selected = pyqtSignal(str)
    service_selected = pyqtSignal(str)

    def __init__(self, pm: ProjectManager, parent=None):
        super().__init__(parent)
        self.pm = pm

        main = QVBoxLayout(self)
        self.tabs = QTabWidget()
        main.addWidget(self.tabs)

        # ---- Pestaña ETLs ----
        etl_page = QWidget()
        etl_layout = QVBoxLayout(etl_page)
        etl_controls = QHBoxLayout()
        self.btn_etl_add = QPushButton("Añadir ETL")
        self.btn_etl_edit = QPushButton("Editar ETL")
        self.btn_etl_del = QPushButton("Eliminar ETL")
        self.btn_etl_load = QPushButton("Cargar en Diseñador")
        self.btn_etl_save = QPushButton("Guardar Diseñador en ETL")
        etl_controls.addWidget(self.btn_etl_add)
        etl_controls.addWidget(self.btn_etl_edit)
        etl_controls.addWidget(self.btn_etl_del)
        etl_controls.addWidget(self.btn_etl_load)
        etl_controls.addWidget(self.btn_etl_save)
        etl_controls.addStretch()
        etl_layout.addLayout(etl_controls)
        self.tbl_etls = QTableWidget(0, 3)
        self.tbl_etls.setHorizontalHeaderLabels(["ID", "Nombre", "# Nodos"])
        # Selección por filas y única
        self.tbl_etls.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tbl_etls.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        # Doble clic / Enter activa cargar en Diseñador (usando la fila del item)
        self.tbl_etls.itemDoubleClicked.connect(lambda it: self._load_etl_by_row(it.row()))
        self.tbl_etls.itemActivated.connect(lambda it: self._load_etl_by_row(it.row()))
        etl_layout.addWidget(self.tbl_etls)
        self.tabs.addTab(etl_page, "ETLs")

        # ---- Pestaña Jobs ----
        job_page = QWidget()
        job_layout = QVBoxLayout(job_page)
        job_controls = QHBoxLayout()
        self.btn_job_add = QPushButton("Añadir Job")
        self.btn_job_edit = QPushButton("Editar Job")
        self.btn_job_del = QPushButton("Eliminar Job")
        job_controls.addWidget(self.btn_job_add)
        job_controls.addWidget(self.btn_job_edit)
        job_controls.addWidget(self.btn_job_del)
        job_controls.addStretch()
        job_layout.addLayout(job_controls)
        self.tbl_jobs = QTableWidget(0, 5)
        self.tbl_jobs.setHorizontalHeaderLabels(["ID", "Nombre", "On Error", "Max Parallel", "# Etapas"])
        self.tbl_jobs.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tbl_jobs.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        job_layout.addWidget(self.tbl_jobs)
        self.tabs.addTab(job_page, "Jobs")

        # ---- Pestaña Servicios ----
        svc_page = QWidget()
        svc_layout = QVBoxLayout(svc_page)
        svc_controls = QHBoxLayout()
        self.btn_svc_add = QPushButton("Añadir Servicio")
        self.btn_svc_edit = QPushButton("Editar Servicio")
        self.btn_svc_del = QPushButton("Eliminar Servicio")
        svc_controls.addWidget(self.btn_svc_add)
        svc_controls.addWidget(self.btn_svc_edit)
        svc_controls.addWidget(self.btn_svc_del)
        svc_controls.addStretch()
        svc_layout.addLayout(svc_controls)
        self.tbl_svcs = QTableWidget(0, 5)
        self.tbl_svcs.setHorizontalHeaderLabels(["ID", "Nombre", "Puerto", "Basic", "JWT"])
        self.tbl_svcs.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tbl_svcs.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        svc_layout.addWidget(self.tbl_svcs)
        self.tabs.addTab(svc_page, "Servicios")

        # Señales
        self.btn_job_add.clicked.connect(self._add_job)
        self.btn_job_edit.clicked.connect(self._edit_job)
        self.btn_job_del.clicked.connect(self._delete_job)
        self.btn_etl_add.clicked.connect(self._add_etl)
        self.btn_etl_edit.clicked.connect(self._edit_etl)
        self.btn_etl_del.clicked.connect(self._delete_etl)
        self.btn_etl_load.clicked.connect(self._load_selected_etl_into_designer)
        self.btn_etl_save.clicked.connect(self._save_designer_into_selected_etl)
        self.btn_svc_add.clicked.connect(self._add_service)
        self.btn_svc_edit.clicked.connect(self._edit_service)
        self.btn_svc_del.clicked.connect(self._delete_service)
        # Emitir selección de Job/Servicio
        try:
            self.tbl_jobs.itemSelectionChanged.connect(self._emit_job_selection)
            self.tbl_svcs.itemSelectionChanged.connect(self._emit_service_selection)
        except Exception:
            pass
        # Cambio de pestaña del panel Proyecto (para informar contexto)
        try:
            self.tabs.currentChanged.connect(self._on_left_tabs_changed)
        except Exception:
            pass

        self.refresh()

    # --------- Helpers ---------
    def _jobs(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []

    def _etls(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('etls') or []) if isinstance(self.pm.project, dict) else []

    def _services(self) -> List[Dict[str, Any]]:
        return (self.pm.project.get('services') or []) if isinstance(self.pm.project, dict) else []

    # Generadores de IDs/nombres únicos
    def _generate_unique_id(self, prefix: str, existing: List[str]) -> str:
        s = set(str(x) for x in existing if x)
        i = 1
        while True:
            candidate = f"{prefix}{i:03d}"
            if candidate not in s:
                return candidate
            i += 1

    def _generate_unique_name(self, base: str, existing: List[str]) -> str:
        s = set(str(x) for x in existing if x)
        if base not in s:
            return base
        i = 2
        while True:
            candidate = f"{base} {i}"
            if candidate not in s:
                return candidate
            i += 1

    def refresh(self) -> None:
        # Jobs
        jobs = self._jobs()
        self.tbl_jobs.setRowCount(len(jobs))
        for i, j in enumerate(jobs):
            jid = str(j.get('id') or '')
            name = str(j.get('name') or '')
            on_error = str(j.get('on_error') or 'stop')
            mp = j.get('max_parallel')
            mp_str = str(mp) if mp is not None else ''
            stages = j.get('stages') or []
            self.tbl_jobs.setItem(i, 0, QTableWidgetItem(jid))
            self.tbl_jobs.setItem(i, 1, QTableWidgetItem(name))
            self.tbl_jobs.setItem(i, 2, QTableWidgetItem(on_error))
            self.tbl_jobs.setItem(i, 3, QTableWidgetItem(mp_str))
            self.tbl_jobs.setItem(i, 4, QTableWidgetItem(str(len(stages))))

        # ETLs
        etls = self._etls()
        self.tbl_etls.setRowCount(len(etls))
        for i, e in enumerate(etls):
            eid = str(e.get('id') or '')
            name = str(e.get('name') or '')
            content = e.get('content') or {}
            nodes = content.get('nodes') or []
            self.tbl_etls.setItem(i, 0, QTableWidgetItem(eid))
            self.tbl_etls.setItem(i, 1, QTableWidgetItem(name))
            self.tbl_etls.setItem(i, 2, QTableWidgetItem(str(len(nodes))))

        # Servicios
        svcs = self._services()
        self.tbl_svcs.setRowCount(len(svcs))
        for i, s in enumerate(svcs):
            sid = str(s.get('id') or '')
            name = str(s.get('name') or '')
            port = s.get('port')
            dflt = (self.pm.project.get('defaults') or {}).get('services', {}) if isinstance(self.pm.project, dict) else {}
            port_str = str(port if port is not None else dflt.get('port', 8080))
            auth = s.get('auth') or {}
            basic = '✔' if auth.get('basic_user') and auth.get('basic_pass') else ''
            jwt = '✔' if auth.get('jwt_secret') else ''
            self.tbl_svcs.setItem(i, 0, QTableWidgetItem(sid))
            self.tbl_svcs.setItem(i, 1, QTableWidgetItem(name))
            self.tbl_svcs.setItem(i, 2, QTableWidgetItem(port_str))
            self.tbl_svcs.setItem(i, 3, QTableWidgetItem(basic))
            self.tbl_svcs.setItem(i, 4, QTableWidgetItem(jwt))

    def _selected_job_row(self) -> int:
        rows = self.tbl_jobs.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _selected_etl_row(self) -> int:
        rows = self.tbl_etls.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _selected_etl_id(self) -> str:
        row = self._selected_etl_row()
        if row < 0 or row >= self.tbl_etls.rowCount():
            return ""
        item = self.tbl_etls.item(row, 0)
        return item.text().strip() if item else ""

    def _selected_job_id(self) -> str:
        row = self._selected_job_row()
        if row < 0 or row >= self.tbl_jobs.rowCount():
            return ""
        it = self.tbl_jobs.item(row, 0)
        return it.text().strip() if it else ""

    def _selected_service_id(self) -> str:
        rows = self.tbl_svcs.selectionModel().selectedRows()
        if not rows:
            return ""
        row = rows[0].row()
        if row < 0 or row >= self.tbl_svcs.rowCount():
            return ""
        it = self.tbl_svcs.item(row, 0)
        return it.text().strip() if it else ""

    def _emit_job_selection(self) -> None:
        try:
            jid = self._selected_job_id()
            self.job_selected.emit(jid)
        except Exception:
            pass

    def _emit_service_selection(self) -> None:
        try:
            sid = self._selected_service_id()
            self.service_selected.emit(sid)
        except Exception:
            pass

    def _on_left_tabs_changed(self, index: int) -> None:
        # Si volvemos a ETLs, no forzamos selección; MainWindow puede volver al diseñador
        try:
            tab_name = self.tabs.tabText(index)
        except Exception:
            tab_name = ""
        if tab_name == "ETLs":
            try:
                # Emitir vacío para indicar que no hay Job/Servicio activo
                self.job_selected.emit("")
                self.service_selected.emit("")
            except Exception:
                pass
        elif tab_name == "Jobs":
            # Reemitir selección actual de Job (si existe) para forzar mostrar el formulario
            try:
                self._emit_job_selection()
            except Exception:
                pass
        elif tab_name == "Servicios":
            # Reemitir selección actual de Servicio (si existe)
            try:
                self._emit_service_selection()
            except Exception:
                pass

    def _load_selected_etl_into_designer(self) -> None:
        eid = self._selected_etl_id()
        if not eid:
            QMessageBox.information(self, "ETLs", "Selecciona un ETL para cargar en el Diseñador.")
            return
        self.load_etl_requested.emit(eid)

    def _load_etl_by_row(self, row: int) -> None:
        try:
            if row < 0 or row >= self.tbl_etls.rowCount():
                return
            item = self.tbl_etls.item(row, 0)
            eid = item.text().strip() if item else ""
            if not eid:
                return
            self.load_etl_requested.emit(eid)
        except Exception:
            pass

    def _save_designer_into_selected_etl(self) -> None:
        eid = self._selected_etl_id()
        if not eid:
            QMessageBox.information(self, "ETLs", "Selecciona un ETL para guardar el Diseñador en él.")
            return
        self.save_etl_requested.emit(eid)

    # ---- ETLs CRUD ----
    def _add_etl(self) -> None:
        if not self.pm.project:
            QMessageBox.warning(self, "Proyecto", "Abre o crea un proyecto primero.")
            return
        etls = self._etls()
        existing_ids = [str(e.get('id') or '') for e in etls]
        existing_names = [str(e.get('name') or '') for e in etls]
        new_id = self._generate_unique_id("etl-", existing_ids)
        new_name = self._generate_unique_name("Nuevo ETL", existing_names)
        new_etl = {
            "id": new_id,
            "name": new_name,
            "content": {"nodes": [], "edges": []}
        }
        etls.append(new_etl)
        self.pm.project['etls'] = etls
        try:
            self.pm.save()
        except Exception:
            pass
        self.refresh()
        self.project_changed.emit(self.pm.project)

    def _edit_etl(self) -> None:
        row = self._selected_etl_row()
        etls = self._etls()
        if row < 0 or row >= len(etls):
            QMessageBox.information(self, "ETLs", "Selecciona un ETL para editar.")
            return
        etl = etls[row]
        dlg = EtlEditorDialog(self, mode="edit")
        dlg.set_etl(etl)
        if dlg.exec():
            new_etl = dlg.get_etl()
            etls[row] = new_etl
            self.pm.project['etls'] = etls
            try:
                self.pm.save()
            except Exception:
                pass
            self.refresh()
            self.project_changed.emit(self.pm.project)

    def _delete_etl(self) -> None:
        row = self._selected_etl_row()
        etls = self._etls()
        if row < 0 or row >= len(etls):
            QMessageBox.information(self, "ETLs", "Selecciona un ETL para eliminar.")
            return
        eid = str(etls[row].get('id') or '')
        r = QMessageBox.question(self, "Eliminar", f"¿Eliminar el ETL '{eid}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if r != QMessageBox.StandardButton.Yes:
            return
        etls.pop(row)
        self.pm.project['etls'] = etls
        try:
            self.pm.save()
        except Exception:
            pass
        self.refresh()
        self.project_changed.emit(self.pm.project)

    # ---- Servicios CRUD ----
    def _selected_service_row(self) -> int:
        rows = self.tbl_svcs.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _add_service(self) -> None:
        if not self.pm.project:
            QMessageBox.warning(self, "Proyecto", "Abre o crea un proyecto primero.")
            return
        svcs = self._services()
        existing_ids = [str(s.get('id') or '') for s in svcs]
        default_port = int((self.pm.project.get('defaults') or {}).get('services', {}).get('port') or 8080)
        dlg = ServiceEditorDialog(self, default_port=default_port, existing_ids=existing_ids, mode="create")
        if dlg.exec():
            svc = dlg.get_service()
            svcs.append(svc)
            self.pm.project['services'] = svcs
            try:
                self.pm.save()
            except Exception:
                pass
            self.refresh()
            self.project_changed.emit(self.pm.project)

    def _edit_service(self) -> None:
        row = self._selected_service_row()
        svcs = self._services()
        if row < 0 or row >= len(svcs):
            QMessageBox.information(self, "Servicios", "Selecciona un servicio para editar.")
            return
        svc = svcs[row]
        default_port = int((self.pm.project.get('defaults') or {}).get('services', {}).get('port') or 8080)
        existing_ids = [str(s.get('id') or '') for s in svcs if s is not svc]
        dlg = ServiceEditorDialog(self, default_port=default_port, existing_ids=existing_ids, mode="edit")
        dlg.set_service(svc, default_port=default_port)
        if dlg.exec():
            new_svc = dlg.get_service()
            svcs[row] = new_svc
            self.pm.project['services'] = svcs
            try:
                self.pm.save()
            except Exception:
                pass
            self.refresh()
            self.project_changed.emit(self.pm.project)

    def _delete_service(self) -> None:
        row = self._selected_service_row()
        svcs = self._services()
        if row < 0 or row >= len(svcs):
            QMessageBox.information(self, "Servicios", "Selecciona un servicio para eliminar.")
            return
        sid = str(svcs[row].get('id') or '')
        r = QMessageBox.question(self, "Eliminar", f"¿Eliminar el servicio '{sid}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if r != QMessageBox.StandardButton.Yes:
            return
        svcs.pop(row)
        self.pm.project['services'] = svcs
        try:
            self.pm.save()
        except Exception:
            pass
        self.refresh()
        self.project_changed.emit(self.pm.project)

    def _add_job(self) -> None:
        if not self.pm.project:
            QMessageBox.warning(self, "Proyecto", "Abre o crea un proyecto primero.")
            return
        jobs = self._jobs()
        existing_ids = [str(j.get('id') or '') for j in jobs]
        existing_names = [str(j.get('name') or '') for j in jobs]
        defaults_mp = int((self.pm.project.get('defaults') or {}).get('max_parallel') or 1)
        new_id = self._generate_unique_id("job-", existing_ids)
        new_name = self._generate_unique_name("Nuevo Job", existing_names)
        job = {
            "id": new_id,
            "name": new_name,
            "on_error": "stop",
            "max_parallel": defaults_mp,
            "stages": []
        }
        # Añadir y guardar
        jobs.append(job)
        self.pm.project['jobs'] = jobs
        try:
            self.pm.save()
        except Exception:
            pass
        self.refresh()
        self.project_changed.emit(self.pm.project)

    def _edit_job(self) -> None:
        row = self._selected_job_row()
        jobs = self._jobs()
        if row < 0 or row >= len(jobs):
            QMessageBox.information(self, "Jobs", "Selecciona un job para editar.")
            return
        job = jobs[row]
        defaults_mp = int((self.pm.project.get('defaults') or {}).get('max_parallel') or 1)
        existing_ids = [str(j.get('id') or '') for j in jobs if j is not job]
        dlg = JobEditorDialog(self, defaults_max_parallel=defaults_mp, existing_ids=existing_ids, mode="edit")
        dlg.set_job(job)
        if dlg.exec():
            new_job = dlg.get_job()
            # Reemplazar job
            jobs[row] = new_job
            self.pm.project['jobs'] = jobs
            try:
                self.pm.save()
            except Exception:
                pass
            self.refresh()
            self.project_changed.emit(self.pm.project)

    def _delete_job(self) -> None:
        row = self._selected_job_row()
        jobs = self._jobs()
        if row < 0 or row >= len(jobs):
            QMessageBox.information(self, "Jobs", "Selecciona un job para eliminar.")
            return
        jid = str(jobs[row].get('id') or '')
        r = QMessageBox.question(self, "Eliminar", f"¿Eliminar el job '{jid}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if r != QMessageBox.StandardButton.Yes:
            return
        jobs.pop(row)
        self.pm.project['jobs'] = jobs
        try:
            self.pm.save()
        except Exception:
            pass
        self.refresh()
        self.project_changed.emit(self.pm.project)
