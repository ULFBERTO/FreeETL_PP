from __future__ import annotations

from typing import Any, Dict, List, Optional, Callable

from PyQt6.QtWidgets import (
    QWidget, QFormLayout, QLabel, QLineEdit, QComboBox, QSpinBox, QMessageBox,
    QGroupBox, QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem,
    QCheckBox, QTimeEdit, QDateEdit, QAbstractItemView
)
from PyQt6.QtCore import QDate, QTime


class JobConfigForm(QWidget):
    """Formulario simple para configurar un Job del proyecto (.fetl).

    Campos editables:
    - name (str)
    - on_error (stop|continue)
    - max_parallel (int)
    """

    def __init__(self, project_manager, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.pm = project_manager
        self._loading: bool = False
        self._current_index: Optional[int] = None
        self.on_change: Optional[Callable[[], None]] = None

        root = QVBoxLayout(self)
        form_top = QFormLayout()

        self.lbl_id = QLabel("")
        self.ed_name = QLineEdit()
        self.cmb_on_error = QComboBox()
        self.cmb_on_error.addItems(["stop", "continue"])
        self.spn_max_parallel = QSpinBox()
        self.spn_max_parallel.setMinimum(1)
        self.spn_max_parallel.setMaximum(9999)

        form_top.addRow("ID:", self.lbl_id)
        form_top.addRow("Nombre:", self.ed_name)
        form_top.addRow("On Error:", self.cmb_on_error)
        form_top.addRow("Max Parallel:", self.spn_max_parallel)
        root.addLayout(form_top)

        # ----- Programación -----
        grp_sched = QGroupBox("Programación")
        lay_sched = QFormLayout(grp_sched)
        self.time_exec = QTimeEdit()
        self.time_exec.setDisplayFormat("HH:mm")
        self.day_mon = QCheckBox("Lunes")
        self.day_tue = QCheckBox("Martes")
        self.day_wed = QCheckBox("Miércoles")
        self.day_thu = QCheckBox("Jueves")
        self.day_fri = QCheckBox("Viernes")
        self.day_sat = QCheckBox("Sábado")
        self.day_sun = QCheckBox("Domingo")
        days_row = QHBoxLayout()
        for cb in (self.day_mon, self.day_tue, self.day_wed, self.day_thu, self.day_fri, self.day_sat, self.day_sun):
            days_row.addWidget(cb)
        self.chk_indef = QCheckBox("Indefinido")
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.setDate(QDate.currentDate())
        lay_sched.addRow("Hora:", self.time_exec)
        lay_sched.addRow("Días:", self._wrap(days_row))
        lay_sched.addRow("Fin:", self._wrap(self._hbox([self.chk_indef, self.date_end])))
        root.addWidget(grp_sched)

        # Cambios de programación
        self.time_exec.timeChanged.connect(self._apply_save_schedule)
        for cb in (self.day_mon, self.day_tue, self.day_wed, self.day_thu, self.day_fri, self.day_sat, self.day_sun):
            cb.toggled.connect(self._apply_save_schedule)
        self.chk_indef.toggled.connect(self._apply_save_schedule)
        self.date_end.dateChanged.connect(self._apply_save_schedule)

        # ----- Etapas y Pasos -----
        grp_stages = QGroupBox("Etapas (secuenciales o en paralelo)")
        lay_stages = QVBoxLayout(grp_stages)
        # Controles de etapas
        controls_stage = QHBoxLayout()
        self.btn_add_stage = QPushButton("Añadir etapa")
        self.btn_del_stage = QPushButton("Eliminar etapa")
        self.chk_stage_parallel = QCheckBox("Paralelo en etapa seleccionada")
        controls_stage.addWidget(self.btn_add_stage)
        controls_stage.addWidget(self.btn_del_stage)
        controls_stage.addStretch()
        controls_stage.addWidget(self.chk_stage_parallel)
        lay_stages.addLayout(controls_stage)
        # Tabla de etapas
        self.tbl_stages = QTableWidget(0, 2)
        self.tbl_stages.setHorizontalHeaderLabels(["#", "Paralelo"]) 
        lay_stages.addWidget(self.tbl_stages)
        # Controles de pasos
        controls_steps = QHBoxLayout()
        self.chk_use_etl = QCheckBox("ETLs")
        self.cmb_etl = QComboBox()
        self.chk_use_service = QCheckBox("Servicios")
        self.cmb_service = QComboBox()
        self.cmb_action = QComboBox()
        self.cmb_action.addItems(["start", "stop"])  # para servicios
        self.btn_add_step = QPushButton("Añadir paso")
        self.btn_del_step = QPushButton("Eliminar paso")
        # Orden: [ETLs][cmb_etl] [Servicios][cmb_service][Acción] [Añadir][Eliminar]
        controls_steps.addWidget(self.chk_use_etl)
        controls_steps.addWidget(self.cmb_etl)
        controls_steps.addSpacing(12)
        controls_steps.addWidget(self.chk_use_service)
        controls_steps.addWidget(self.cmb_service)
        controls_steps.addWidget(self.cmb_action)
        controls_steps.addStretch()
        controls_steps.addWidget(self.btn_add_step)
        controls_steps.addWidget(self.btn_del_step)
        lay_stages.addLayout(controls_steps)
        # Tabla de pasos por etapa
        self.tbl_steps = QTableWidget(0, 3)
        self.tbl_steps.setHorizontalHeaderLabels(["Tipo", "ID", "Acción"])  # Tipo: ETL/Servicio; Acción para servicio: start/stop
        lay_stages.addWidget(self.tbl_steps)
        root.addWidget(grp_stages)

        # Señales de edición
        self.ed_name.editingFinished.connect(self._apply_save)
        self.cmb_on_error.currentIndexChanged.connect(self._apply_save)
        self.spn_max_parallel.valueChanged.connect(self._apply_save)
        # Señales etapas/pasos
        self.btn_add_stage.clicked.connect(self._add_stage)
        self.btn_del_stage.clicked.connect(self._del_stage)
        self.tbl_stages.itemSelectionChanged.connect(self._load_steps_for_selected_stage)
        self.chk_stage_parallel.toggled.connect(self._toggle_stage_parallel)
        self.chk_use_etl.toggled.connect(self._toggle_step_target_mode)
        self.chk_use_service.toggled.connect(self._toggle_step_target_mode)
        self.btn_add_step.clicked.connect(self._add_step)
        self.btn_del_step.clicked.connect(self._del_step)
        # Tablas solo lectura para evitar errores de escritura
        self.tbl_stages.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tbl_steps.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self._set_enabled(False)

    def _set_enabled(self, enabled: bool):
        self.ed_name.setEnabled(enabled)
        self.cmb_on_error.setEnabled(enabled)
        self.spn_max_parallel.setEnabled(enabled)
        # Scheduling
        self.time_exec.setEnabled(enabled)
        for cb in (self.day_mon, self.day_tue, self.day_wed, self.day_thu, self.day_fri, self.day_sat, self.day_sun):
            cb.setEnabled(enabled)
        self.chk_indef.setEnabled(enabled)
        self.date_end.setEnabled(enabled and (not self.chk_indef.isChecked()))
        # Stages/steps
        self.tbl_stages.setEnabled(enabled)
        self.tbl_steps.setEnabled(enabled)
        self.btn_add_stage.setEnabled(enabled)
        self.btn_del_stage.setEnabled(enabled)
        self.btn_add_step.setEnabled(enabled)
        self.btn_del_step.setEnabled(enabled)
        self.chk_stage_parallel.setEnabled(enabled and (self._selected_stage_index() is not None))
        # Step target controls
        self.chk_use_etl.setEnabled(enabled)
        self.chk_use_service.setEnabled(enabled)
        self.cmb_etl.setEnabled(enabled and self.chk_use_etl.isChecked())
        self.cmb_service.setEnabled(enabled and self.chk_use_service.isChecked())
        self.cmb_action.setEnabled(enabled and self.chk_use_service.isChecked())

    def clear(self):
        self._loading = True
        try:
            self.lbl_id.setText("")
            self.ed_name.setText("")
            self.cmb_on_error.setCurrentIndex(0)
            # Usar default del proyecto si está disponible
            try:
                dflt = (self.pm.project.get('defaults') or {}).get('max_parallel') or 1
            except Exception:
                dflt = 1
            self.spn_max_parallel.setValue(int(dflt))
            self._current_index = None
            self._set_enabled(False)
        finally:
            self._loading = False

    def load_job_by_id(self, job_id: str) -> bool:
        """Carga un job por ID y muestra sus valores en el formulario.
        Devuelve True si encontró el job, False en caso contrario.
        """
        jobs: List[Dict[str, Any]] = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
        idx = -1
        for i, j in enumerate(jobs):
            if str(j.get('id')) == str(job_id):
                idx = i
                break
        if idx < 0:
            self.clear()
            return False

        self._loading = True
        try:
            job = jobs[idx]
            self._current_index = idx
            self.lbl_id.setText(str(job.get('id') or ''))
            self.ed_name.setText(str(job.get('name') or ''))
            on_error = str(job.get('on_error') or 'stop')
            self.cmb_on_error.setCurrentIndex(max(0, self.cmb_on_error.findText(on_error)))
            mp = job.get('max_parallel')
            if mp is None:
                mp = (self.pm.project.get('defaults') or {}).get('max_parallel') or 1
            self.spn_max_parallel.setValue(int(mp))
            # Programación
            sched = job.get('schedule') or {}
            try:
                hh, mm = str(sched.get('time') or '00:00').split(':', 1)
                self.time_exec.setTime(QTime(int(hh), int(mm)))
            except Exception:
                self.time_exec.setTime(QTime(0, 0))
            days = set(int(x) for x in (sched.get('days_of_week') or []))
            self.day_mon.setChecked(1 in days or 0 in days)  # Soporta 0-domingo o 1-lunes
            self.day_tue.setChecked(2 in days)
            self.day_wed.setChecked(3 in days)
            self.day_thu.setChecked(4 in days)
            self.day_fri.setChecked(5 in days)
            self.day_sat.setChecked(6 in days or 0 in days)
            self.day_sun.setChecked(0 in days or 7 in days)
            indef = bool(sched.get('indefinite'))
            self.chk_indef.setChecked(indef)
            try:
                date_txt = str(sched.get('end_date') or '')
                if date_txt:
                    y, m, d = [int(x) for x in date_txt.split('-')]
                    self.date_end.setDate(QDate(y, m, d))
                else:
                    self.date_end.setDate(QDate.currentDate())
            except Exception:
                self.date_end.setDate(QDate.currentDate())

            # Poblar combos de destino
            self._populate_target_combos()
            # Por defecto activar ETL si hay, sino Servicio
            if self.cmb_etl.count() > 0:
                self.chk_use_etl.setChecked(True)
                self.chk_use_service.setChecked(False)
            elif self.cmb_service.count() > 0:
                self.chk_use_etl.setChecked(False)
                self.chk_use_service.setChecked(True)
            else:
                self.chk_use_etl.setChecked(True)
                self.chk_use_service.setChecked(False)
            self._toggle_step_target_mode()

            # Etapas
            self._load_stages(job)
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
            jobs: List[Dict[str, Any]] = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
            if not (0 <= self._current_index < len(jobs)):
                return
            job = dict(jobs[self._current_index])
            job['name'] = self.ed_name.text().strip()
            job['on_error'] = self.cmb_on_error.currentText().strip()
            job['max_parallel'] = int(self.spn_max_parallel.value())
            # schedule ya se guarda vía _apply_save_schedule
            jobs[self._current_index] = job
            self.pm.project['jobs'] = jobs
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
            QMessageBox.warning(self, "Jobs", f"No se pudo guardar el Job: {e}")

    # --------- Helpers UI ---------
    def _wrap(self, layout_or_widget):
        w = QWidget()
        if isinstance(layout_or_widget, QHBoxLayout) or isinstance(layout_or_widget, QVBoxLayout):
            w.setLayout(layout_or_widget)
        else:
            lay = QHBoxLayout(w)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.addWidget(layout_or_widget)
        return w

    def _hbox(self, widgets: List[QWidget]) -> QHBoxLayout:
        lay = QHBoxLayout()
        lay.setContentsMargins(0, 0, 0, 0)
        for w in widgets:
            lay.addWidget(w)
        lay.addStretch()
        return lay

    def _etl_ids(self) -> List[str]:
        try:
            return [str(e.get('id') or '') for e in (self.pm.project.get('etls') or []) if e.get('id')]
        except Exception:
            return []

    def _service_ids(self) -> List[str]:
        try:
            return [str(s.get('id') or '') for s in (self.pm.project.get('services') or []) if s.get('id')]
        except Exception:
            return []

    def _populate_target_combos(self) -> None:
        etls = self._etl_ids()
        svcs = self._service_ids()
        self.cmb_etl.blockSignals(True)
        self.cmb_service.blockSignals(True)
        try:
            self.cmb_etl.clear()
            self.cmb_etl.addItems(etls)
            self.cmb_service.clear()
            self.cmb_service.addItems(svcs)
        finally:
            self.cmb_etl.blockSignals(False)
            self.cmb_service.blockSignals(False)

    def _toggle_step_target_mode(self):
        # Solo uno puede estar activo a la vez
        if self.chk_use_etl.isChecked() and self.chk_use_service.isChecked():
            # Priorizar el último marcado; si se marcó ETL, apagar servicio
            self.chk_use_service.blockSignals(True)
            self.chk_use_service.setChecked(False)
            self.chk_use_service.blockSignals(False)
        if not self.chk_use_etl.isChecked() and not self.chk_use_service.isChecked():
            # Activar ETL por defecto si ambos apagados
            self.chk_use_etl.blockSignals(True)
            self.chk_use_etl.setChecked(True)
            self.chk_use_etl.blockSignals(False)
        # Habilitar combos según selección
        self.cmb_etl.setEnabled(self.chk_use_etl.isChecked())
        self.cmb_service.setEnabled(self.chk_use_service.isChecked())
        self.cmb_action.setEnabled(self.chk_use_service.isChecked())

    # --------- Scheduling save ---------
    def _apply_save_schedule(self, *_):
        if self._loading or self._current_index is None:
            return
        try:
            jobs: List[Dict[str, Any]] = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
            if not (0 <= self._current_index < len(jobs)):
                return
            job = dict(jobs[self._current_index])
            hh = f"{self.time_exec.time().hour():02d}"
            mm = f"{self.time_exec.time().minute():02d}"
            days = []
            # Usaremos 1..7 (L..D)
            if self.day_mon.isChecked(): days.append(1)
            if self.day_tue.isChecked(): days.append(2)
            if self.day_wed.isChecked(): days.append(3)
            if self.day_thu.isChecked(): days.append(4)
            if self.day_fri.isChecked(): days.append(5)
            if self.day_sat.isChecked(): days.append(6)
            if self.day_sun.isChecked(): days.append(7)
            indef = self.chk_indef.isChecked()
            end_date = None if indef else self.date_end.date().toString('yyyy-MM-dd')
            job['schedule'] = {
                'time': f"{hh}:{mm}",
                'days_of_week': days,
                'indefinite': bool(indef),
                'end_date': end_date,
            }
            jobs[self._current_index] = job
            self.pm.project['jobs'] = jobs
            try:
                self.pm.save()
            except Exception:
                pass
            # habilitar/inhabilitar fecha fin
            self.date_end.setEnabled(not indef)
        except Exception:
            pass

    # --------- Stages & Steps UI ---------
    def _load_stages(self, job: Dict[str, Any]):
        stages = job.get('stages') or []
        self.tbl_stages.blockSignals(True)
        try:
            self.tbl_stages.setRowCount(len(stages))
            for i, st in enumerate(stages):
                self.tbl_stages.setItem(i, 0, QTableWidgetItem(str(i + 1)))
                self.tbl_stages.setItem(i, 1, QTableWidgetItem('Sí' if st.get('parallel') else 'No'))
            # seleccionar primera
            if stages:
                self.tbl_stages.selectRow(0)
                self.chk_stage_parallel.setChecked(bool(stages[0].get('parallel')))
                self._load_steps_for_selected_stage()
            else:
                self.tbl_steps.setRowCount(0)
        finally:
            self.tbl_stages.blockSignals(False)

    def _selected_stage_index(self) -> Optional[int]:
        rows = self.tbl_stages.selectionModel().selectedRows() if self.tbl_stages.selectionModel() else []
        if not rows:
            return None
        return rows[0].row()

    def _persist_stages(self, stages: List[Dict[str, Any]]):
        if self._current_index is None:
            return
        jobs: List[Dict[str, Any]] = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
        if not (0 <= self._current_index < len(jobs)):
            return
        job = dict(jobs[self._current_index])
        job['stages'] = stages
        jobs[self._current_index] = job
        self.pm.project['jobs'] = jobs
        try:
            self.pm.save()
        except Exception:
            pass
        if self.on_change:
            try:
                self.on_change()
            except Exception:
                pass

    def _get_stages(self) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = (self.pm.project.get('jobs') or []) if isinstance(self.pm.project, dict) else []
        if self._current_index is None or not (0 <= self._current_index < len(jobs)):
            return []
        return list(jobs[self._current_index].get('stages') or [])

    def _add_stage(self):
        if self._loading or self._current_index is None:
            return
        stages = self._get_stages()
        stages.append({'parallel': False, 'steps': []})
        self._persist_stages(stages)
        self._load_stages({'stages': stages})

    def _del_stage(self):
        if self._loading or self._current_index is None:
            return
        sel = self._selected_stage_index()
        if sel is None:
            return
        stages = self._get_stages()
        if 0 <= sel < len(stages):
            stages.pop(sel)
        self._persist_stages(stages)
        self._load_stages({'stages': stages})

    def _toggle_stage_parallel(self, checked: bool):
        if self._loading or self._current_index is None:
            return
        sel = self._selected_stage_index()
        if sel is None:
            return
        stages = self._get_stages()
        if not (0 <= sel < len(stages)):
            return
        stages[sel]['parallel'] = bool(checked)
        self._persist_stages(stages)
        self.tbl_stages.setItem(sel, 1, QTableWidgetItem('Sí' if checked else 'No'))

    def _load_steps_for_selected_stage(self):
        sel = self._selected_stage_index()
        stages = self._get_stages()
        self.tbl_steps.blockSignals(True)
        try:
            if sel is None or not (0 <= sel < len(stages)):
                self.tbl_steps.setRowCount(0)
                self.chk_stage_parallel.setChecked(False)
                self._set_enabled(self._current_index is not None)
                return
            st = stages[sel]
            self.chk_stage_parallel.setChecked(bool(st.get('parallel')))
            steps = st.get('steps') or []
            self.tbl_steps.setRowCount(len(steps))
            for i, s in enumerate(steps):
                typ = 'ETL' if 'etl_id' in s else 'Servicio'
                action = s.get('action') or ('start' if 'service_id' in s else '')
                target_id = s.get('etl_id') or s.get('service_id') or ''
                self.tbl_steps.setItem(i, 0, QTableWidgetItem(typ))
                self.tbl_steps.setItem(i, 1, QTableWidgetItem(str(target_id)))
                self.tbl_steps.setItem(i, 2, QTableWidgetItem(str(action)))
        finally:
            self.tbl_steps.blockSignals(False)

    def _add_step(self):
        sel = self._selected_stage_index()
        if sel is None:
            return
        stages = self._get_stages()
        if not (0 <= sel < len(stages)):
            return
        # Crear paso según selección de UI
        step: Dict[str, Any]
        if self.chk_use_etl.isChecked():
            target_id = self.cmb_etl.currentText().strip()
            step = {'etl_id': target_id, 'overrides': {}}
        else:
            target_id = self.cmb_service.currentText().strip()
            action = self.cmb_action.currentText().strip().lower() or 'start'
            step = {'service_id': target_id, 'action': action}
        stages[sel].setdefault('steps', []).append(step)
        self._persist_stages(stages)
        self._load_steps_for_selected_stage()

    def _del_step(self):
        sel = self._selected_stage_index()
        if sel is None:
            return
        row = -1
        rows = self.tbl_steps.selectionModel().selectedRows() if self.tbl_steps.selectionModel() else []
        if rows:
            row = rows[0].row()
        if row < 0:
            return
        stages = self._get_stages()
        steps = stages[sel].get('steps') or []
        if 0 <= row < len(steps):
            steps.pop(row)
        stages[sel]['steps'] = steps
        self._persist_stages(stages)
        self._load_steps_for_selected_stage()

    def _apply_save_steps_inline(self, item):
        if self._loading or self._current_index is None:
            return
        sel = self._selected_stage_index()
        if sel is None:
            return
        r = item.row()
        c = item.column()
        stages = self._get_stages()
        if not (0 <= sel < len(stages)):
            return
        steps = list(stages[sel].get('steps') or [])
        if not (0 <= r < len(steps)):
            return
        s = dict(steps[r])
        try:
            if c == 0:  # Tipo
                val = (item.text() or '').strip().lower()
                if val.startswith('etl'):
                    # Convertir a ETL manteniendo id si fuera posible
                    s = {'etl_id': s.get('etl_id') or '', 'overrides': s.get('overrides') or {}}
                else:
                    s = {'service_id': s.get('service_id') or '', 'action': s.get('action') or 'start'}
            elif c == 1:  # ID
                val = (item.text() or '').strip()
                if 'etl_id' in s:
                    s['etl_id'] = val
                elif 'service_id' in s:
                    s['service_id'] = val
            elif c == 2:  # Acción
                val = (item.text() or '').strip().lower()
                if 'service_id' in s:
                    s['action'] = 'stop' if val.startswith('stop') else 'start'
            steps[r] = s
            stages[sel]['steps'] = steps
            self._persist_stages(stages)
        except Exception:
            pass
