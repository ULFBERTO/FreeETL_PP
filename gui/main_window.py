from PyQt6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                              QPushButton, QLabel, QMenuBar, QMenu, QStatusBar,
                              QTextEdit, QSplitter, QMessageBox, QFileDialog,
                              QToolButton, QDialog, QDialogButtonBox, QTableWidget,
                              QTableWidgetItem, QAbstractItemView, QFormLayout,
                              QTabWidget, QDockWidget, QStackedWidget)
from PyQt6.QtCore import Qt, QTimer, QPointF
from .pipeline_canvas import PipelineCanvas
from .node_palette import NodePalette
from .properties_panel import PropertiesPanel
from core.etl_engine import ETLEngine
import polars as pl
from core.project_manager import ProjectManager
from .project_settings_dialog import ProjectSettingsDialog
from .jobs_tab import JobsTab
from .project_explorer import ProjectExplorer
from .services_tab import ServicesTab
from .job_form import JobConfigForm
from .service_form import ServiceConfigForm
from .runs_tab import RunsTab

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Pipeline Builder")
        self.setGeometry(100, 100, 1500, 800)  # Ventana más ancha
        
        # Create central widget and layout
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        # Crear el menú temprano para evitar retardos visuales
        try:
            self.create_menu_bar()
        except Exception:
            pass
        
        # Usar un QSplitter para dividir la ventana
        self.main_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Widget superior con un stack que alterna Diseñador/Forms
        self.top_widget = QWidget()
        main_layout = QHBoxLayout(self.top_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Instanciar paleta de nodos (en panel lateral)
        self.node_palette = NodePalette()
        
        # Crear Designer page (canvas + propiedades)
        self.pipeline_canvas = PipelineCanvas()
        self.properties_panel = PropertiesPanel()
        self.designer_page = QWidget()
        _designer_h = QHBoxLayout(self.designer_page)
        _designer_h.setContentsMargins(0, 0, 0, 0)
        _designer_h.addWidget(self.pipeline_canvas)
        _designer_h.addWidget(self.properties_panel)
        
        # Stacked widget con páginas: Diseñador, JobForm, ServiceForm
        self.designer_stack = QStackedWidget()
        self.designer_stack.addWidget(self.designer_page)  # index 0
        # Forms se agregarán tras inicializar ProjectManager
        main_layout.addWidget(self.designer_stack)
        
        # Agregar widget superior al splitter
        self.main_splitter.addWidget(self.top_widget)
        
        # Create log panel
        self.log_panel = QTextEdit()
        self.log_panel.setReadOnly(True)
        self.log_panel.setMaximumHeight(150)
        self.log_panel.append("Bienvenido a ETL Pipeline Builder")
        
        # Agregar log panel al splitter
        self.main_splitter.addWidget(self.log_panel)
        
        # Set sizes for splitter
        self.main_splitter.setSizes([600, 150])
        
        # Agregar el splitter a una pestaña "Diseñador" (contenedor derecho)
        self.tabs = QTabWidget()
        self.designer_container = QWidget()
        designer_vlayout = QVBoxLayout(self.designer_container)
        designer_vlayout.setContentsMargins(0, 0, 0, 0)
        designer_vlayout.addWidget(self.main_splitter)
        self.tabs.addTab(self.designer_container, "Diseñador")
        
        # Inicializar Project Manager antes de crear la pestaña Jobs
        self.project_manager = ProjectManager()
        
        # Añadir panel lateral con pestañas (Proyecto/Paleta) y botón para ocultar/mostrar
        try:
            self.project_explorer = ProjectExplorer(self.project_manager, self)
            # Cuando cambie el proyecto, refrescar JobsTab si existe
            self.project_explorer.project_changed.connect(lambda _proj: getattr(self, 'jobs_tab', None) and self.jobs_tab.refresh())
            # Refrescar ServicesTab si existe
            self.project_explorer.project_changed.connect(lambda _proj: getattr(self, 'services_tab', None) and self.services_tab.refresh())
            # Cargar/Guardar ETL desde/ hacia Diseñador
            self.project_explorer.load_etl_requested.connect(self.load_etl_from_project)
            self.project_explorer.save_etl_requested.connect(self.save_designer_into_etl)

            # Crear tabs laterales usando la paleta ya instanciada
            self.left_tabs = QTabWidget()
            self.left_tabs.addTab(self.project_explorer, "Proyecto")
            self.left_tabs.addTab(self.node_palette, "Paleta")

            # Contenedor con cabecera (botón de colapsar)
            self.left_container = QWidget()
            _lc_v = QVBoxLayout(self.left_container)
            _lc_v.setContentsMargins(0, 0, 0, 0)
            _lc_v.setSpacing(4)
            _lc_header = QHBoxLayout()
            _lc_header.addWidget(QLabel("Panel"))
            _lc_header.addStretch()
            self.btn_toggle_left = QToolButton(self.left_container)
            self.btn_toggle_left.setText("◀ Panel")
            self.btn_toggle_left.setCheckable(True)
            self.btn_toggle_left.toggled.connect(self._toggle_left_panel)
            _lc_header.addWidget(self.btn_toggle_left)
            _lc_v.addLayout(_lc_header)
            _lc_v.addWidget(self.left_tabs)

            # Marcar panel lateral como listo; lo colocaremos a nivel superior junto a las pestañas centrales
            try:
                self.left_container.setMinimumWidth(260)
            except Exception:
                pass
        except Exception:
            # Fallback: marca que no hay contenedor lateral, usaremos la paleta en el splitter central
            try:
                self.left_container = None
            except Exception:
                pass
        
        # Añadir formularios al stack ahora que hay ProjectManager
        try:
            self.job_form = JobConfigForm(self.project_manager, self)
            self.service_form = ServiceConfigForm(self.project_manager, self)
            self.job_form.on_change = lambda: getattr(self, 'project_explorer', None) and self.project_explorer.refresh()
            self.service_form.on_change = lambda: getattr(self, 'project_explorer', None) and self.project_explorer.refresh()
            self.designer_stack.addWidget(self.job_form)      # index 1
            self.designer_stack.addWidget(self.service_form)  # index 2
        except Exception:
            pass

        # Conectar selección de Job/Servicio desde el panel Proyecto
        try:
            self.project_explorer.job_selected.connect(self._on_job_selected)
            self.project_explorer.service_selected.connect(self._on_service_selected)
            # Cuando cambie el proyecto, refrescar también RunsTab
            self.project_explorer.project_changed.connect(lambda _proj: getattr(self, 'runs_tab', None) and self.runs_tab.refresh())
        except Exception:
            pass

        # Añadir pestaña Jobs
        try:
            self.jobs_tab = JobsTab(self.project_manager, self)
            self.tabs.addTab(self.jobs_tab, "Jobs")
        except Exception:
            # Si falla la carga de la pestaña Jobs, continuar sin bloquear la app
            pass
        
        # Añadir pestaña Servicios
        try:
            self.services_tab = ServicesTab(self.project_manager, self)
            self.tabs.addTab(self.services_tab, "Servicios")
        except Exception:
            # Continuar si falla la carga de Servicios
            pass

        # Añadir pestaña Runs
        try:
            self.runs_tab = RunsTab(self.project_manager, self)
            self.tabs.addTab(self.runs_tab, "Runs")
        except Exception:
            pass
        
        # Colocar las pestañas centrales en el área central del QMainWindow
        main_layout_vertical = QVBoxLayout(self.central_widget)
        main_layout_vertical.setContentsMargins(0, 0, 0, 0)
        main_layout_vertical.addWidget(self.tabs)

        # Crear un QDockWidget a la izquierda para el panel lateral (Proyecto/Paleta)
        try:
            self.left_dock = QDockWidget("Panel", self)
            self.left_dock.setObjectName("LeftPanelDock")
            self.left_dock.setAllowedAreas(Qt.DockWidgetArea.LeftDockWidgetArea | Qt.DockWidgetArea.RightDockWidgetArea)
            if hasattr(self, 'left_container') and self.left_container is not None:
                self.left_dock.setWidget(self.left_container)
            else:
                self.left_dock.setWidget(self.node_palette)
            self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.left_dock)
            # Mantener sincronización con acción/botón cuando el usuario cierre/abra el dock
            try:
                self.left_dock.visibilityChanged.connect(self._sync_left_panel_visibility)
            except Exception:
                pass
            try:
                self.left_dock.setMinimumWidth(260)
            except Exception:
                pass
        except Exception:
            pass
        
        # Create ETL engine
        self.etl_engine = ETLEngine()
        
        # Project Manager ya inicializado arriba
        
        # Connect signals
        self.node_palette.node_selected.connect(self.handle_node_selected)
        self.pipeline_canvas.node_selected.connect(self.properties_panel.show_node_properties)
        self.properties_panel.node_config_changed.connect(self.handle_node_config_changed)
        
        # Página inicial: Diseñador
        try:
            self.designer_stack.setCurrentWidget(self.designer_page)
        except Exception:
            pass

    def _switch_to_designer_tab(self):
        try:
            idx = self.tabs.indexOf(self.designer_container)
            if idx != -1:
                self.tabs.setCurrentIndex(idx)
        except Exception:
            pass

    def _on_job_selected(self, job_id: str):
        """Cuando se selecciona un Job en el panel Proyecto, mostrar formulario en el área del diseñador."""
        try:
            if job_id:
                ok = False
                try:
                    ok = self.job_form.load_job_by_id(job_id)
                except Exception:
                    ok = False
                if ok:
                    self.designer_stack.setCurrentWidget(self.job_form)
                    self._switch_to_designer_tab()
                    return
            # Si no hay selección o no se pudo cargar, volver al diseñador normal
            self.designer_stack.setCurrentWidget(self.designer_page)
        except Exception:
            pass

    def _on_service_selected(self, service_id: str):
        """Cuando se selecciona un Servicio en el panel Proyecto, mostrar formulario en el área del diseñador."""
        try:
            if service_id:
                ok = False
                try:
                    ok = self.service_form.load_service_by_id(service_id)
                except Exception:
                    ok = False
                if ok:
                    self.designer_stack.setCurrentWidget(self.service_form)
                    self._switch_to_designer_tab()
                    return
            # Si no hay selección o no se pudo cargar, volver al diseñador normal
            self.designer_stack.setCurrentWidget(self.designer_page)
        except Exception:
            pass
        self.pipeline_canvas.connection_created.connect(self.log_message)
        self.properties_panel.fetch_connected_data.connect(self.fetch_data_from_connected_nodes)
        # Reaplicar botones laterales de expandir cuando cambie la configuración y se reconstruya el panel
        self.properties_panel.node_config_changed.connect(lambda *_: QTimer.singleShot(0, self._add_side_expand_buttons))
        # Clear properties panel when clicking on canvas background
        if hasattr(self.pipeline_canvas, 'background_clicked'):
            self.pipeline_canvas.background_clicked.connect(self.clear_properties_panel)
        # Añadir botones laterales tras seleccionar nodo
        self.pipeline_canvas.node_selected.connect(lambda *_: QTimer.singleShot(0, self._add_side_expand_buttons))
        # Recordar último nodo seleccionado para abrir dataframes correctos
        self.pipeline_canvas.node_selected.connect(self._remember_last_selected_node)
        
        # Connect ETL engine signals
        self.etl_engine.execution_progress.connect(self.log_message)
        self.etl_engine.execution_finished.connect(self.handle_execution_finished)
        self.etl_engine.node_executed.connect(self.handle_node_executed)
        
        # Create status bar
        self.statusBar().showMessage("Ready")
        
    def handle_node_selected(self, node_type, subtype=None):
        """Handle when a node type is selected from the palette"""
        # Get the center of the canvas
        center = self.pipeline_canvas.viewport().rect().center()
        scene_pos = self.pipeline_canvas.mapToScene(center)
        
        # Add the node to the canvas
        node_id = self.pipeline_canvas.add_node(node_type, scene_pos, subtype)
        
        # Show properties for the new node
        self.properties_panel.show_node_properties(node_id, node_type, {'subtype': subtype})
        
        # Log node creation
        self.log_message(f"Nodo {node_id} creado de tipo {node_type}")
        
    def handle_node_config_changed(self, node_id, config):
        """Maneja el cambio de configuración de un nodo"""
        # Evitar reentradas durante la reconstrucción del panel de propiedades
        try:
            if getattr(self.properties_panel, '_ui_rebuilding', False):
                self.log_message("Ignorando cambio de configuración durante reconstrucción de UI")
                return
        except Exception:
            pass
        # Actualizar la configuración del nodo en el pipeline
        if node_id in self.pipeline_canvas.graph.nodes:
            # Detectar cambios de subtipo para actualizar el título/puertos
            prev_config = self.pipeline_canvas.graph.nodes[node_id].get('config', {})
            prev_subtype = prev_config.get('subtype')
            # Guardar la configuración actualizada
            self.pipeline_canvas.graph.nodes[node_id]['config'] = config
            self.log_message(f"Configuración actualizada para nodo {node_id}")
            # Si cambió el subtipo, refrescar visual
            try:
                if prev_subtype != config.get('subtype'):
                    self.pipeline_canvas.update_node_visual(node_id)
            except Exception as e:
                self.log_message(f"Aviso: no se pudo actualizar el título del nodo {node_id}: {e}")
            
            # Obtener tipo de nodo
            node_type = self.pipeline_canvas.graph.nodes[node_id]['type']
            
            # Si hay dataframes en la configuración, verificar si hay nodos conectados
            # que necesiten recibir estos datos
            if 'dataframe' in config:
                # Propagar datos a los nodos conectados
                for target_id in self.pipeline_canvas.graph.successors(node_id):
                    # Propagar los datos actualizados
                    self.pipeline_canvas.propagate_data_to_target(node_id, target_id)
                    
                    # Actualizar el panel de propiedades si es el nodo actualmente seleccionado
                    if hasattr(self.properties_panel, 'current_node_id') and self.properties_panel.current_node_id == target_id:
                        target_type = self.pipeline_canvas.graph.nodes[target_id]['type']
                        
                        # Actualizar también el dataframe en el panel de propiedades
                        target_config = self.pipeline_canvas.graph.nodes[target_id].get('config', {})
                        if 'dataframe' in target_config:
                            self.properties_panel.set_node_dataframe(target_id, target_config['dataframe'])
                            
                        # Mostrar propiedades actualizadas
                        self.properties_panel.show_node_properties(
                            target_id, 
                            target_type, 
                            self.pipeline_canvas.graph.nodes[target_id].get('config', {})
                        )
                        
                # Si es un nodo de transformación, también debemos propagar a sus nodos destino
                if node_type == 'transform':
                    # Verificar si hay nodos destino indirectamente conectados
                    for target_id in list(self.pipeline_canvas.graph.successors(node_id)):
                        # Propagar a los nodos conectados al nodo objetivo (si es una transformación)
                        target_type = self.pipeline_canvas.graph.nodes[target_id]['type']
                        if target_type == 'transform':
                            for second_target_id in self.pipeline_canvas.graph.successors(target_id):
                                # Asegurar que la transformación se propague correctamente
                                self.pipeline_canvas.propagate_data_to_target(target_id, second_target_id)
        
    def handle_node_executed(self, node_id, dataframe):
        """Maneja el evento de nodo ejecutado"""
        # Actualizar el dataframe en el panel de propiedades
        self.properties_panel.set_node_dataframe(node_id, dataframe)
        self.log_message(f"Nodo {node_id} ejecutado con éxito - {len(dataframe)} filas")
        # Asegurar que los botones laterales estén presentes
        QTimer.singleShot(0, self._add_side_expand_buttons)
        
    def handle_execution_finished(self, success, message):
        """Maneja el final de la ejecución del pipeline"""
        if success:
            QMessageBox.information(self, "Ejecución completada", message)
        else:
            QMessageBox.critical(self, "Error en ejecución", message)
            
    def log_message(self, message):
        """Añade un mensaje al panel de log"""
        self.log_panel.append(message)
        # Scroll to bottom
        scrollbar = self.log_panel.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _toggle_left_panel(self, checked: bool):
        """Muestra/oculta el panel lateral (Proyecto/Paleta)."""
        try:
            if hasattr(self, 'left_tabs') and self.left_tabs is not None:
                self.left_tabs.setVisible(not checked)
            if hasattr(self, 'btn_toggle_left') and self.btn_toggle_left is not None:
                self.btn_toggle_left.setText("▶ Panel" if checked else "◀ Panel")
            # Mostrar/ocultar el dock completo también
            if hasattr(self, 'left_dock') and self.left_dock is not None:
                self.left_dock.setVisible(not checked)
            # Mantener sincronizada la acción del menú (checked significa visible)
            if hasattr(self, 'act_toggle_left') and self.act_toggle_left is not None:
                try:
                    self.act_toggle_left.blockSignals(True)
                    self.act_toggle_left.setChecked(not checked)
                finally:
                    self.act_toggle_left.blockSignals(False)
        except Exception:
            pass

    def _sync_left_panel_visibility(self, visible: bool):
        """Sincroniza el estado visible/invisible del dock con el botón y el menú."""
        try:
            # left_tabs dentro del dock
            if hasattr(self, 'left_tabs') and self.left_tabs is not None:
                self.left_tabs.setVisible(visible)
            # Botón del header (checked = oculto)
            if hasattr(self, 'btn_toggle_left') and self.btn_toggle_left is not None:
                try:
                    self.btn_toggle_left.blockSignals(True)
                    self.btn_toggle_left.setChecked(not visible)
                    self.btn_toggle_left.setText("▶ Panel" if not visible else "◀ Panel")
                finally:
                    self.btn_toggle_left.blockSignals(False)
            # Acción del menú Ver (checked = visible)
            if hasattr(self, 'act_toggle_left') and self.act_toggle_left is not None:
                try:
                    self.act_toggle_left.blockSignals(True)
                    self.act_toggle_left.setChecked(visible)
                finally:
                    self.act_toggle_left.blockSignals(False)
        except Exception:
            pass

    def clear_properties_panel(self):
        """Limpia el panel de propiedades y muestra un mensaje vacío."""
        panel = self.properties_panel
        # Log para verificar conexión con clic de fondo
        try:
            self.log_message("Fondo clicado: limpiando panel de propiedades")
        except Exception:
            pass
        # Prevenir reconstrucciones mientras limpiamos (autosave, timers)
        try:
            setattr(panel, '_ui_rebuilding', True)
            if hasattr(panel, '_autosave_timer') and panel._autosave_timer.isActive():
                panel._autosave_timer.stop()
            setattr(panel, '_pending_autosave', None)
        except Exception:
            pass
        # Limpiar overlays
        if hasattr(self, '_overlay_buttons'):
            try:
                for tbl, btn in list(self._overlay_buttons.items()):
                    try:
                        btn.hide()
                        btn.setParent(None)
                    except Exception:
                        pass
                self._overlay_buttons.clear()
            except Exception:
                pass
        # Desconectar señales del panel actual
        try:
            self.pipeline_canvas.node_selected.disconnect(self.properties_panel.show_node_properties)
        except Exception:
            pass
        try:
            self.properties_panel.node_config_changed.disconnect(self.handle_node_config_changed)
        except Exception:
            pass
        try:
            self.properties_panel.fetch_connected_data.disconnect(self.fetch_data_from_connected_nodes)
        except Exception:
            pass
        # Remover el panel actual del layout superior y destruir su parent
        try:
            top_layout = self.top_widget.layout()
        except Exception:
            top_layout = None
        try:
            if top_layout is not None:
                top_layout.removeWidget(self.properties_panel)
        except Exception:
            pass
        try:
            self.properties_panel.setParent(None)
        except Exception:
            pass
        # Crear un nuevo panel vacío y volver a conectar señales
        new_panel = PropertiesPanel()
        self.properties_panel = new_panel
        try:
            if top_layout is not None:
                top_layout.addWidget(new_panel)
        except Exception:
            pass
        # Reconectar señales
        self.pipeline_canvas.node_selected.connect(self.properties_panel.show_node_properties)
        self.properties_panel.node_config_changed.connect(self.handle_node_config_changed)
        self.properties_panel.fetch_connected_data.connect(self.fetch_data_from_connected_nodes)
        self.properties_panel.node_config_changed.connect(lambda *_: QTimer.singleShot(0, self._add_side_expand_buttons))
        try:
            self.log_message("Panel de propiedades reiniciado tras clic en fondo")
        except Exception:
            pass
        try:
            setattr(panel, '_ui_rebuilding', False)
        except Exception:
            pass

    def _remember_last_selected_node(self, node_id, node_type, node_data):
        """Guarda el último node_id seleccionado para usarlo en modales de datos."""
        try:
            self._last_selected_node = node_id
        except Exception:
            pass

    def _add_side_expand_buttons(self):
        """Añade botones '⤢' al lado de tablas/tabs del panel de propiedades."""
        panel = self.properties_panel
        if not hasattr(self, '_side_wrapped'):
            self._side_wrapped = set()
        # Limpiar restos de overlays antiguos si hubiera
        if hasattr(self, '_overlay_buttons') and self._overlay_buttons:
            try:
                for tbl, btn in list(self._overlay_buttons.items()):
                    try:
                        vp = tbl.viewport()
                        vp.removeEventFilter(self)
                    except Exception:
                        pass
                    try:
                        btn.hide()
                        btn.setParent(None)
                    except Exception:
                        pass
                self._overlay_buttons.clear()
            except Exception:
                pass

        def wrap_table(tbl: QTableWidget, title: str, open_mode: str = 'df'):
            if tbl is None:
                return
            if int(tbl.property('has_side_expand') or 0) == 1:
                return
            # Buscar layout ancestro que contenga directamente a la tabla
            parent = tbl.parent()
            for _ in range(6):
                if parent is None:
                    break
                lay = getattr(parent, 'layout', lambda: None)()
                if lay is None:
                    parent = parent.parent()
                    continue
                # QBoxLayouts
                try:
                    idx = lay.indexOf(tbl)
                except Exception:
                    idx = -1
                if idx != -1:
                    # Quitar y reemplazar por contenedor con botón
                    try:
                        lay.removeWidget(tbl)
                    except Exception:
                        pass
                    container = QWidget(parent)
                    hl = QHBoxLayout(container)
                    hl.setContentsMargins(0, 0, 0, 0)
                    hl.setSpacing(6)
                    hl.addWidget(tbl)
                    side = QVBoxLayout()
                    side.setContentsMargins(0, 0, 0, 0)
                    side.setSpacing(6)
                    btn = QToolButton(container)
                    btn.setText("⤢")
                    btn.setToolTip(f"Expandir {title}")
                    btn.setFixedSize(24, 24)
                    btn.setStyleSheet("background-color: rgba(0,0,0,0.06); border: 1px solid #888; border-radius: 4px;")
                    if open_mode == 'clone':
                        btn.clicked.connect(lambda _=False, t=tbl, ti=title: self._open_table_preview_modal(ti, t))
                    else:
                        btn.clicked.connect(lambda _=False: self._open_dataframe_modal_for_node(getattr(self, '_last_selected_node', None) or getattr(self.properties_panel, 'current_node_id', None)))
                    side.addWidget(btn, alignment=Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)
                    side.addStretch(1)
                    hl.addLayout(side)
                    try:
                        lay.insertWidget(idx, container)
                    except Exception:
                        lay.addWidget(container)
                    try:
                        tbl.setProperty('has_side_expand', 1)
                    except Exception:
                        pass
                    return
                # QFormLayout
                if isinstance(lay, QFormLayout):
                    try:
                        idx = lay.indexOf(tbl)
                        if idx != -1:
                            row, role = lay.getItemPosition(idx)
                            try:
                                lay.removeWidget(tbl)
                            except Exception:
                                pass
                            container = QWidget(parent)
                            hl = QHBoxLayout(container)
                            hl.setContentsMargins(0, 0, 0, 0)
                            hl.setSpacing(6)
                            hl.addWidget(tbl)
                            side = QVBoxLayout()
                            side.setContentsMargins(0, 0, 0, 0)
                            side.setSpacing(6)
                            btn = QToolButton(container)
                            btn.setText("⤢")
                            btn.setToolTip(f"Expandir {title}")
                            btn.setFixedSize(24, 24)
                            btn.setStyleSheet("background-color: rgba(0,0,0,0.06); border: 1px solid #888; border-radius: 4px;")
                            if open_mode == 'clone':
                                btn.clicked.connect(lambda _=False, t=tbl, ti=title: self._open_table_preview_modal(ti, t))
                            else:
                                btn.clicked.connect(lambda _=False: self._open_dataframe_modal_for_node(getattr(self, '_last_selected_node', None) or getattr(self.properties_panel, 'current_node_id', None)))
                            side.addWidget(btn, alignment=Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)
                            side.addStretch(1)
                            hl.addLayout(side)
                            lay.setWidget(row, QFormLayout.ItemRole.FieldRole, container)
                            try:
                                tbl.setProperty('has_side_expand', 1)
                            except Exception:
                                pass
                            return
                    except Exception:
                        pass
                parent = parent.parent()

        def wrap_tabs(tabs: QTabWidget, title: str):
            if tabs is None:
                return
            if int(tabs.property('has_side_expand') or 0) == 1:
                return
            parent = tabs.parent()
            for _ in range(6):
                if parent is None:
                    break
                lay = getattr(parent, 'layout', lambda: None)()
                if lay is None:
                    parent = parent.parent()
                    continue
                try:
                    idx = lay.indexOf(tabs)
                except Exception:
                    idx = -1
                if idx != -1:
                    try:
                        lay.removeWidget(tabs)
                    except Exception:
                        pass
                    container = QWidget(parent)
                    hl = QHBoxLayout(container)
                    hl.setContentsMargins(0, 0, 0, 0)
                    hl.setSpacing(6)
                    hl.addWidget(tabs)
                    side = QVBoxLayout()
                    side.setContentsMargins(0, 0, 0, 0)
                    side.setSpacing(6)
                    btn = QToolButton(container)
                    btn.setText("⤢")
                    btn.setToolTip(f"Expandir {title}")
                    btn.setFixedSize(24, 24)
                    btn.setStyleSheet("background-color: rgba(0,0,0,0.06); border: 1px solid #888; border-radius: 4px;")
                    btn.clicked.connect(lambda _=False, tb=tabs, ti=title: self._open_current_tab_table_modal(tb, ti))
                    side.addWidget(btn, alignment=Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)
                    side.addStretch(1)
                    hl.addLayout(side)
                    try:
                        lay.insertWidget(idx, container)
                    except Exception:
                        lay.addWidget(container)
                    try:
                        tabs.setProperty('has_side_expand', 1)
                    except Exception:
                        pass
                    return
                if isinstance(lay, QFormLayout):
                    try:
                        idx = lay.indexOf(tabs)
                        if idx != -1:
                            row, role = lay.getItemPosition(idx)
                            try:
                                lay.removeWidget(tabs)
                            except Exception:
                                pass
                            container = QWidget(parent)
                            hl = QHBoxLayout(container)
                            hl.setContentsMargins(0, 0, 0, 0)
                            hl.setSpacing(6)
                            hl.addWidget(tabs)
                            side = QVBoxLayout()
                            side.setContentsMargins(0, 0, 0, 0)
                            side.setSpacing(6)
                            btn = QToolButton(container)
                            btn.setText("⤢")
                            btn.setToolTip(f"Expandir {title}")
                            btn.setFixedSize(24, 24)
                            btn.setStyleSheet("background-color: rgba(0,0,0,0.06); border: 1px solid #888; border-radius: 4px;")
                            btn.clicked.connect(lambda _=False, tb=tabs, ti=title: self._open_current_tab_table_modal(tb, ti))
                            side.addWidget(btn, alignment=Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)
                            side.addStretch(1)
                            hl.addLayout(side)
                            lay.setWidget(row, QFormLayout.ItemRole.FieldRole, container)
                            try:
                                tabs.setProperty('has_side_expand', 1)
                            except Exception:
                                pass
                            return
                    except Exception:
                        pass
                parent = parent.parent()

        # Determinar títulos según contexto del panel
        try:
            if hasattr(panel, 'data_table') and panel.data_table is not None:
                # La vista previa debe abrir datos reales
                wrap_table(panel.data_table, "Vista previa", open_mode='df')
        except Exception:
            pass
        try:
            title_cols = "Columnas"
            if getattr(panel, 'current_node_type', None) == 'destination':
                title_cols = "Columnas a guardar"
            elif getattr(panel, 'current_node_type', None) == 'transform':
                title_cols = "Columnas a pasar"
            if hasattr(panel, 'column_selection_table') and panel.column_selection_table is not None:
                # Para selección/renombrado, también abrir datos reales
                wrap_table(panel.column_selection_table, title_cols, open_mode='df')
        except Exception:
            pass
        try:
            jf = getattr(panel, 'join_fields', None)
            if isinstance(jf, dict) and jf.get('column_table') is not None:
                wrap_table(jf['column_table'], "Columnas de salida", open_mode='df')
        except Exception:
            pass
        try:
            if hasattr(panel, 'filter_rules_table') and panel.filter_rules_table is not None:
                wrap_table(panel.filter_rules_table, "Reglas de filtro", open_mode='df')
        except Exception:
            pass
        try:
            if hasattr(panel, 'agg_group_by_table') and panel.agg_group_by_table is not None:
                wrap_table(panel.agg_group_by_table, "Group By", open_mode='df')
        except Exception:
            pass
        try:
            if hasattr(panel, 'agg_ops_table') and panel.agg_ops_table is not None:
                wrap_table(panel.agg_ops_table, "Funciones", open_mode='df')
        except Exception:
            pass
        # QTabWidget de datos de origen en Unión
        try:
            for tabs in panel.findChildren(QTabWidget):
                wrap_tabs(tabs, "Datos de origen")
        except Exception:
            pass
            # Envolver cualquier otra QTableWidget que no hayamos cubierto
            try:
                for tbl in panel.findChildren(QTableWidget):
                    if int(tbl.property('has_side_expand') or 0) != 1:
                        wrap_table(tbl, "Tabla", open_mode='df')
            except Exception:
                pass

    def _open_current_tab_table_modal(self, tabs: QTabWidget, base_title: str):
        try:
            idx = tabs.currentIndex()
            if idx < 0:
                return
            page = tabs.widget(idx)
            title = f"{base_title}: {tabs.tabText(idx)}"
            tbl = page.findChild(QTableWidget)
            if tbl is not None:
                self._open_table_preview_modal(title, tbl)
        except Exception:
            pass

    def _open_dataframe_modal_for_node(self, node_id, max_rows: int = 100, _retry: bool = False):
        # Intentar obtener DF actual
        df = None
        try:
            if node_id is None and hasattr(self, '_last_selected_node'):
                node_id = self._last_selected_node
        except Exception:
            pass
        try:
            df = self.properties_panel.get_node_dataframe(node_id)
        except Exception:
            df = None
        if df is None:
            # Intentar construir preview automáticamente para transform/destination
            try:
                node_type = self.pipeline_canvas.graph.nodes[node_id]['type'] if node_id in self.pipeline_canvas.graph.nodes else None
            except Exception:
                node_type = None
            if node_type in ('transform', 'destination') and not _retry:
                try:
                    self.fetch_data_from_connected_nodes(node_id)
                except Exception:
                    pass
                # Reintentar abrir el modal luego de una breve espera
                QTimer.singleShot(250, lambda: self._open_dataframe_modal_for_node(node_id, max_rows, _retry=True))
                return
            QMessageBox.information(self, "Vista previa", "No hay datos para mostrar")
            return
        self._open_dataframe_modal("Vista previa de datos", df, max_rows)

    def _open_dataframe_modal(self, title: str, df, max_rows: int = 100):
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        v = QVBoxLayout(dlg)
        table = QTableWidget()
        try:
            rows = min(max_rows, len(df))
            cols = list(df.columns)
        except Exception:
            rows = 0
            cols = []
        table.setRowCount(rows)
        table.setColumnCount(len(cols))
        if cols:
            table.setHorizontalHeaderLabels(cols)
        for i in range(rows):
            for j, col in enumerate(cols):
                try:
                    item = QTableWidgetItem(str(df[col][i]))
                except Exception:
                    item = QTableWidgetItem("")
                table.setItem(i, j, item)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        v.addWidget(table)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btns.rejected.connect(dlg.reject)
        btns.accepted.connect(dlg.accept)
        v.addWidget(btns)
        dlg.resize(900, 500)
        dlg.exec()

    def _open_table_preview_modal(self, title: str, source_table: QTableWidget):
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        v = QVBoxLayout(dlg)
        clone = QTableWidget()
        try:
            cols = source_table.columnCount()
            rows = source_table.rowCount()
        except Exception:
            cols = 0
            rows = 0
        clone.setRowCount(rows)
        clone.setColumnCount(cols)
        headers = []
        for c in range(cols):
            header_item = source_table.horizontalHeaderItem(c)
            headers.append(header_item.text() if header_item else f"Col {c+1}")
        if headers:
            clone.setHorizontalHeaderLabels(headers)
        for r in range(rows):
            for c in range(cols):
                src_item = source_table.item(r, c)
                text_val = None
                if src_item is not None:
                    if src_item.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                        text_val = '✔' if src_item.checkState() == Qt.CheckState.Checked else '✘'
                    else:
                        text_val = src_item.text()
                if text_val is None:
                    w = source_table.cellWidget(r, c)
                    if hasattr(w, 'text'):
                        try:
                            text_val = w.text()
                        except Exception:
                            text_val = None
                clone.setItem(r, c, QTableWidgetItem(text_val or ""))
        clone.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        v.addWidget(clone)
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        btns.rejected.connect(dlg.reject)
        btns.accepted.connect(dlg.accept)
        v.addWidget(btns)
        dlg.resize(800, 500)
        dlg.exec()

    def _apply_select_and_rename_preview(self, df, config):
        """Aplica selección y renombrado de columnas para previsualización.
        Respeta 'output_cols' y 'column_rename' del nodo fuente.
        """
        try:
            # Asegurar Polars
            if not isinstance(df, pl.DataFrame):
                try:
                    import pandas as pd
                    if isinstance(df, pd.DataFrame):
                        df = pl.from_pandas(df)
                    else:
                        df = pl.DataFrame(df)
                except Exception:
                    df = pl.DataFrame(df)

            result = df
            # Selección de columnas
            output_cols = config.get('output_cols')
            if output_cols and isinstance(output_cols, str):
                cols = [c.strip() for c in output_cols.split(',') if c.strip()]
                # Remover prefijos tipo OrigenX.
                processed_cols = [c.split('.', 1)[1] if '.' in c else c for c in cols]
                valid_cols = [c for c in processed_cols if c in result.columns]
                if valid_cols:
                    result = result.select(valid_cols)

            # Renombrado de columnas
            rename_spec = config.get('column_rename')
            if rename_spec and isinstance(rename_spec, str):
                rename_pairs = rename_spec.split(',')
                rename_dict = {}
                for pair in rename_pairs:
                    if ':' in pair:
                        old_name, new_name = pair.split(':', 1)
                        old_name = old_name.strip()
                        new_name = new_name.strip()
                        if '.' in old_name:
                            old_name = old_name.split('.', 1)[1]
                        if old_name in result.columns and new_name:
                            rename_dict[old_name] = new_name
                if rename_dict:
                    result = result.rename(rename_dict)
            return result
        except Exception as e:
            self.log_message(f"Aviso: error aplicando selección/renombrado de preview: {e}")
        return df
        
    def create_menu_bar(self):
        menubar = self.menuBar()
        
        # Proyecto
        project_menu = menubar.addMenu("Proyecto")
        new_project_action = project_menu.addAction("Nuevo Proyecto")
        open_project_action = project_menu.addAction("Abrir Proyecto")
        save_project_action = project_menu.addAction("Guardar Proyecto")
        project_menu.addSeparator()
        settings_action = project_menu.addAction("Ajustes del Proyecto")
        project_menu.addSeparator()
        save_etl_action = project_menu.addAction("Guardar ETL")
        save_jobs_action = project_menu.addAction("Guardar Jobs")
        save_services_action = project_menu.addAction("Guardar Servicios")
        
        # Edit menu
        edit_menu = menubar.addMenu("Editar")
        undo_action = edit_menu.addAction("Deshacer")
        redo_action = edit_menu.addAction("Rehacer")
        
        # Run menu
        run_menu = menubar.addMenu("Ejecutar")
        run_pipeline_action = run_menu.addAction("Ejecutar Pipeline")
        stop_pipeline_action = run_menu.addAction("Detener Pipeline")
        
        # Connect actions
        run_pipeline_action.triggered.connect(self.run_pipeline)
        stop_pipeline_action.triggered.connect(self.stop_pipeline)
        
        # Proyecto connections
        new_project_action.triggered.connect(self.new_project)
        open_project_action.triggered.connect(self.open_project)
        save_project_action.triggered.connect(self.save_project)
        settings_action.triggered.connect(self.open_project_settings)
        save_etl_action.triggered.connect(self.save_selected_etl_menu)
        save_jobs_action.triggered.connect(self.save_jobs_menu)
        save_services_action.triggered.connect(self.save_services_menu)

        # Ver (toggle panel lateral)
        view_menu = menubar.addMenu("Ver")
        self.act_toggle_left = view_menu.addAction("Panel lateral visible")
        self.act_toggle_left.setCheckable(True)
        self.act_toggle_left.setChecked(True)
        # La acción indica visible=True; _toggle_left_panel espera checked=oculto
        self.act_toggle_left.toggled.connect(lambda visible: self._toggle_left_panel(not visible))
        
    # ------------------ Proyecto (.fetl) ------------------
    def new_project(self):
        """Crea un nuevo proyecto .fetl y actualiza el título de la ventana."""
        path, _ = QFileDialog.getSaveFileName(self, "Nuevo Proyecto", "", "FreeETL Project (*.fetl)")
        if not path:
            return
        try:
            proj = self.project_manager.create_new(path)
            self.setWindowTitle(f"ETL Pipeline Builder - {proj.get('name', '')}")
            self.log_message(f"Proyecto creado: {self.project_manager.path}")
            # Refrescar vistas
            if hasattr(self, 'project_explorer') and self.project_explorer is not None:
                try:
                    self.project_explorer.refresh()
                    self.project_explorer.project_changed.emit(self.project_manager.project)
                except Exception:
                    pass
            for tab_name in ('jobs_tab', 'services_tab', 'runs_tab'):
                try:
                    tab = getattr(self, tab_name, None)
                    if tab:
                        tab.refresh()
                except Exception:
                    pass
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo crear el proyecto: {e}")

    def open_project(self):
        """Abre un proyecto .fetl existente y actualiza el título de la ventana."""
        path, _ = QFileDialog.getOpenFileName(self, "Abrir Proyecto", "", "FreeETL Project (*.fetl)")
        if not path:
            return
        try:
            proj = self.project_manager.open(path)
            self.setWindowTitle(f"ETL Pipeline Builder - {proj.get('name', '')}")
            self.log_message(f"Proyecto abierto: {self.project_manager.path}")
            # Refrescar vistas
            if hasattr(self, 'project_explorer') and self.project_explorer is not None:
                try:
                    self.project_explorer.refresh()
                    self.project_explorer.project_changed.emit(self.project_manager.project)
                except Exception:
                    pass
            for tab_name in ('jobs_tab', 'services_tab', 'runs_tab'):
                try:
                    tab = getattr(self, tab_name, None)
                    if tab:
                        tab.refresh()
                except Exception:
                    pass
            # Volver al diseñador por defecto
            try:
                self.designer_stack.setCurrentWidget(self.designer_page)
                self._switch_to_designer_tab()
            except Exception:
                pass
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo abrir el proyecto: {e}")

    def save_project(self):
        """Guarda el proyecto actual .fetl."""
        try:
            self.project_manager.save()
            self.log_message(f"Proyecto guardado: {self.project_manager.path}")
            QMessageBox.information(self, "Guardado", "Proyecto guardado correctamente.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar el proyecto: {e}")

    def open_project_settings(self):
        """Abre el diálogo de ajustes del proyecto y persiste los cambios."""
        dlg = ProjectSettingsDialog(self, self.project_manager)
        if dlg.exec():
            try:
                self.project_manager.save()
                self.log_message("Ajustes del proyecto guardados")
            except Exception:
                pass

    def save_selected_etl_menu(self) -> None:
        """Guarda el contenido del Diseñador en el ETL seleccionado en el panel Proyecto."""
        try:
            eid = ""
            if hasattr(self, 'project_explorer') and self.project_explorer is not None:
                try:
                    eid = self.project_explorer._selected_etl_id()
                except Exception:
                    eid = ""
            if not eid:
                QMessageBox.information(self, "Proyecto", "Selecciona un ETL en el panel Proyecto para guardar el Diseñador en él.")
                return
            self.save_designer_into_etl(eid)
        except Exception as e:
            QMessageBox.critical(self, "Proyecto", f"No se pudo guardar el ETL: {e}")

    def save_jobs_menu(self) -> None:
        """Persiste los Jobs del proyecto (.fetl)."""
        try:
            self.project_manager.save()
            self.log_message("Jobs guardados en el proyecto")
            QMessageBox.information(self, "Proyecto", "Jobs guardados correctamente.")
        except Exception as e:
            QMessageBox.critical(self, "Proyecto", f"No se pudieron guardar los Jobs: {e}")

    def save_services_menu(self) -> None:
        """Persiste los Servicios del proyecto (.fetl)."""
        try:
            self.project_manager.save()
            self.log_message("Servicios guardados en el proyecto")
            QMessageBox.information(self, "Proyecto", "Servicios guardados correctamente.")
        except Exception as e:
            QMessageBox.critical(self, "Proyecto", f"No se pudieron guardar los Servicios: {e}")

    def run_pipeline(self):
        """Ejecuta la pipeline actual"""
        self.statusBar().showMessage("Ejecutando pipeline...")
        self.log_message("Iniciando ejecución de pipeline...")
        
        # Verificar que la pipeline tenga nodos
        if not self.pipeline_canvas.graph.nodes:
            QMessageBox.warning(self, "Pipeline vacío", "No hay nodos en el pipeline para ejecutar")
            return
            
        # Obtener configuraciones de nodos
        node_configs = {}
        for node_id in self.pipeline_canvas.graph.nodes:
            node_configs[node_id] = self.properties_panel.get_node_config(node_id)
            
        # Configurar el motor ETL
        self.etl_engine.set_pipeline(self.pipeline_canvas.graph, node_configs)
        
        # Ejecutar el pipeline
        self.etl_engine.execute_pipeline()
        
    def stop_pipeline(self):
        """Detiene la ejecución de la pipeline"""
        self.statusBar().showMessage("Deteniendo pipeline...")
        self.log_message("Deteniendo ejecución de pipeline...")
        try:
            self.etl_engine.request_stop()
            QMessageBox.information(self, "Detener", "Se solicitó detener la ejecución. Puede demorar unos segundos.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"No fue posible solicitar detener: {e}")
        
    def fetch_data_from_connected_nodes(self, node_id):
        """Busca datos en los nodos conectados al nodo especificado"""
        self.log_message(f"Obteniendo datos para el nodo {node_id}...")
        
        # Verificar que el nodo existe
        if node_id not in self.pipeline_canvas.graph.nodes:
            self.log_message(f"Error: Nodo {node_id} no encontrado")
            return
            
        node_type = self.pipeline_canvas.graph.nodes[node_id]['type']
        node_config = self.pipeline_canvas.graph.nodes[node_id].get('config', {})
        
        # Si es un nodo de transformación, buscar los nodos conectados a sus entradas
        if node_type == 'transform':
            # Obtener nodos de entrada
            incoming_edges = list(self.pipeline_canvas.graph.in_edges(node_id))
            
            if not incoming_edges:
                self.log_message("No hay nodos conectados como entrada")
                QMessageBox.warning(self, "Sin fuentes de datos", "No hay nodos conectados como entrada. Primero conecte este nodo a una fuente de datos.")
                return
                
            # Actualizar los datos del nodo actual con los datos de los nodos de entrada
            updated_config = node_config.copy()
            data_updated = False
            
            # Para cada conexión de entrada
            for idx, (source_id, _) in enumerate(incoming_edges):
                source_config = self.pipeline_canvas.graph.nodes[source_id].get('config', {})
                
                # Verificar si el nodo fuente tiene datos
                if 'dataframe' in source_config:
                    data_updated = True
                    # Aplicar selección/renombrado definidos en el nodo fuente
                    try:
                        prepared_df = self._apply_select_and_rename_preview(source_config['dataframe'], source_config)
                    except Exception:
                        prepared_df = source_config['dataframe']
                    if idx == 0 or len(incoming_edges) == 1:
                        # Si es la primera entrada o la única, asignar como dataframe principal
                        updated_config['dataframe'] = prepared_df
                        self.log_message(f"Datos obtenidos del nodo {source_id} como fuente principal")
                    elif idx == 1 and node_config.get('subtype') in ['join', 'aggregate']:
                        # Si es la segunda entrada y el nodo es de unión o agregación
                        updated_config['other_dataframe'] = prepared_df
                        self.log_message(f"Datos obtenidos del nodo {source_id} como fuente secundaria")
                else:
                    self.log_message(f"El nodo fuente {source_id} no tiene datos disponibles")
            
            if not data_updated:
                self.log_message("No se encontraron datos en los nodos conectados")
                QMessageBox.warning(self, "Sin datos", "Los nodos conectados no contienen datos. Asegúrese de cargar datos en los nodos de origen.")
                return
                
            # Si el propio nodo es una transformación de tipo filter/map/aggregate, aplicar preview
            try:
                st = (updated_config.get('subtype') or '').lower()
                if st in ['filter', 'map', 'aggregate', 'cast'] and updated_config.get('dataframe') is not None:
                    base_df = updated_config.get('dataframe')
                    if st == 'filter':
                        base_df = self.pipeline_canvas._apply_filter_rules_preview(base_df, updated_config)
                    elif st == 'map':
                        base_df = self.pipeline_canvas._apply_map_ops_preview(base_df, updated_config)
                    elif st == 'aggregate':
                        base_df = self.pipeline_canvas._apply_aggregate_preview(base_df, updated_config)
                    elif st == 'cast':
                        base_df = self.pipeline_canvas._apply_cast_preview(base_df, updated_config)
                    # Aplicar selección/renombrado del propio nodo si existe
                    preview_df = self.pipeline_canvas._apply_select_and_rename(base_df, updated_config)
                    updated_config['dataframe'] = preview_df
            except Exception as e:
                self.log_message(f"Aviso: error construyendo preview de transformación: {e}")

            # Actualizar la configuración del nodo
            self.pipeline_canvas.graph.nodes[node_id]['config'] = updated_config
            
            # Asegurarse de que la actualización se registre para este nodo específico
            self.log_message(f"Configuración de nodo {node_id} actualizada con datos de entrada")
            
            # Para nodos de unión, verificar que se tienen ambos dataframes necesarios
            if node_config.get('subtype') == 'join':
                if 'dataframe' not in updated_config or 'other_dataframe' not in updated_config:
                    self.log_message(f"Faltan datos para la unión en el nodo {node_id}")
                    if 'dataframe' not in updated_config:
                        QMessageBox.warning(self, "Datos incompletos", "Falta el primer origen de datos para la unión.")
                    elif 'other_dataframe' not in updated_config:
                        QMessageBox.warning(self, "Datos incompletos", "Falta el segundo origen de datos para la unión.")
                else:
                    # Si no hay selección definida, usar todas las columnas calificadas por defecto
                    try:
                        df1 = updated_config.get('dataframe')
                        df2 = updated_config.get('other_dataframe')
                        if df1 is not None and df2 is not None:
                            if not updated_config.get('output_cols'):
                                cols = []
                                cols += [f"Origen1.{c}" for c in df1.columns]
                                cols += [f"Origen2.{c}" for c in df2.columns]
                                updated_config['output_cols'] = ','.join(cols)
                    except Exception:
                        pass
                    
            # Actualizar el panel de propiedades
            if hasattr(self.properties_panel, 'update_with_fetched_data'):
                self.properties_panel.update_with_fetched_data(node_id, updated_config)
                self.log_message(f"Panel de propiedades actualizado para el nodo {node_id}")
            else:
                # Si no existe el método, recargar todo el panel
                self.properties_panel.show_node_properties(node_id, node_type, updated_config)
                self.log_message(f"Panel de propiedades recargado para el nodo {node_id}")
                
            self.log_message(f"Datos actualizados para el nodo {node_id}")
            
        # Si es un nodo de destino, buscar datos del nodo de entrada conectado
        elif node_type == 'destination':
            # Obtener nodos de entrada
            incoming_edges = list(self.pipeline_canvas.graph.in_edges(node_id))
            
            if not incoming_edges:
                self.log_message("El nodo destino no tiene conexiones de entrada")
                QMessageBox.warning(self, "Sin fuentes de datos", "Este nodo destino no está conectado a ninguna fuente. Primero conecte este nodo a una fuente o transformación.")
                return
                
            # Obtener el primer nodo de entrada (los nodos destino solo tienen una entrada)
            source_id, _ = incoming_edges[0]
            source_config = self.pipeline_canvas.graph.nodes[source_id].get('config', {})
            
            # Verificar si el nodo fuente tiene datos
            if 'dataframe' in source_config:
                try:
                    # Construir preview según el tipo del nodo fuente
                    st = (source_config.get('subtype') or '').lower()
                    if st == 'join' and source_config.get('other_dataframe') is not None:
                        try:
                            preview_df = self.pipeline_canvas._build_join_selected_df(source_config)
                        except Exception:
                            preview_df = self._apply_select_and_rename_preview(source_config['dataframe'], source_config)
                    elif st == 'filter':
                        base_df = self.pipeline_canvas._apply_filter_rules_preview(source_config['dataframe'], source_config)
                        preview_df = self.pipeline_canvas._apply_select_and_rename(base_df, source_config)
                    elif st == 'map':
                        base_df = self.pipeline_canvas._apply_map_ops_preview(source_config['dataframe'], source_config)
                        preview_df = self.pipeline_canvas._apply_select_and_rename(base_df, source_config)
                    elif st == 'aggregate':
                        base_df = self.pipeline_canvas._apply_aggregate_preview(source_config['dataframe'], source_config)
                        preview_df = self.pipeline_canvas._apply_select_and_rename(base_df, source_config)
                    elif st == 'cast':
                        base_df = self.pipeline_canvas._apply_cast_preview(source_config['dataframe'], source_config)
                        preview_df = self.pipeline_canvas._apply_select_and_rename(base_df, source_config)
                    else:
                        preview_df = self._apply_select_and_rename_preview(source_config['dataframe'], source_config)
                except Exception as e:
                    self.log_message(f"Error aplicando selección/renombrado para preview: {e}")
                    preview_df = source_config['dataframe']

                # Actualizar la configuración del nodo destino con el dataframe de preview
                updated_config = node_config.copy()
                updated_config['dataframe'] = preview_df
                # Si el destino no tiene selección definida, por defecto usar todas las columnas del preview
                try:
                    if not updated_config.get('output_cols'):
                        updated_config['output_cols'] = ','.join(list(preview_df.columns))
                except Exception:
                    pass

                # Guardar la configuración actualizada
                self.pipeline_canvas.graph.nodes[node_id]['config'] = updated_config

                # Actualizar el panel de propiedades con el preview filtrado
                if hasattr(self.properties_panel, 'set_node_dataframe'):
                    self.properties_panel.set_node_dataframe(node_id, preview_df)
                    self.log_message(f"Dataframe actualizado para nodo destino {node_id} (preview filtrado)")

                # Nota: No llamamos a propagate_data_to_target aquí para no sobrescribir el preview
                self.log_message(f"Datos obtenidos del nodo {source_id} para el nodo destino {node_id}")
            else:
                self.log_message(f"El nodo fuente {source_id} no tiene datos disponibles")
                QMessageBox.warning(self, "Sin datos", "El nodo conectado no contiene datos. Asegúrese de cargar o procesar datos primero.")
                
        else:
            self.log_message(f"El nodo {node_id} no es un nodo de transformación o destino")
            QMessageBox.warning(self, "Operación no válida", "Solo los nodos de transformación y destino pueden obtener datos de otros nodos") 

    # --- Gestión de Archivos: Nuevo / Guardar / Abrir ---
    def new_pipeline(self):
        """Crea un pipeline nuevo (limpia el lienzo y estados)."""
        resp = QMessageBox.question(
            self,
            "Nuevo Pipeline",
            "Esto limpiará el pipeline actual. ¿Desea continuar?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if resp == QMessageBox.StandardButton.Yes:
            self.pipeline_canvas.clear_all()
            # Reiniciar propiedades
            if hasattr(self.properties_panel, 'node_configs'):
                self.properties_panel.node_configs.clear()
            if hasattr(self.properties_panel, 'current_dataframes'):
                self.properties_panel.current_dataframes.clear()
            self.log_message("Nuevo pipeline creado.")

    def save_pipeline(self):
        """Guarda el pipeline a un archivo JSON (.etl.json)."""
        path, _ = QFileDialog.getSaveFileName(self, "Guardar Pipeline", "", "ETL Pipeline (*.etl.json);;JSON (*.json)")
        if not path:
            return
        try:
            import json
            # Asegurar que los cambios recientes en el panel actual se guarden
            try:
                if hasattr(self.properties_panel, 'current_node_id') and self.properties_panel.current_node_id is not None:
                    n_id = self.properties_panel.current_node_id
                    n_type = getattr(self.properties_panel, 'current_node_type', None)
                    if n_type == 'transform':
                        tt = getattr(self.properties_panel, 'current_transform_type', None)
                        if tt:
                            # Guardar configuración del transform (incluye Casteo y sus tablas)
                            self.properties_panel.save_transform_config(n_id, tt)
                    elif n_type == 'destination':
                        dt = getattr(self.properties_panel, 'current_dest_type', None)
                        if dt:
                            # Guardar configuración de destino
                            self.properties_panel.save_destination_config(n_id, dt)
                    elif n_type == 'source':
                        st = getattr(self.properties_panel, 'current_source_type', None)
                        if st:
                            # Guardar configuración de origen (silencioso si posible)
                            # Evitar validaciones intrusivas; si falla, ignorar
                            try:
                                self.properties_panel.save_node_config(n_id, st)
                            except Exception:
                                pass
            except Exception:
                pass

            data = {
                'nodes': [],
                'edges': [],
            }
            # Serializar nodos
            for node_id in self.pipeline_canvas.graph.nodes:
                node = self.pipeline_canvas.graph.nodes[node_id]
                pos = node.get('position')
                if isinstance(pos, QPointF):
                    pos_dict = {'x': float(pos.x()), 'y': float(pos.y())}
                elif isinstance(pos, (tuple, list)) and len(pos) == 2:
                    pos_dict = {'x': float(pos[0]), 'y': float(pos[1])}
                else:
                    pos_dict = {'x': 0.0, 'y': 0.0}

                # Preferir configuración consolidada desde el panel de propiedades
                config = (self.properties_panel.get_node_config(node_id) or {}).copy()
                if not config:
                    config = node.get('config', {}).copy()
                config.pop('dataframe', None)
                config.pop('other_dataframe', None)

                data['nodes'].append({
                    'id': int(node_id),
                    'type': node.get('type'),
                    'position': pos_dict,
                    'config': config,
                })

            # Serializar edges
            for (src, dst) in self.pipeline_canvas.graph.edges:
                data['edges'].append({'source': int(src), 'target': int(dst)})

            # Escribir archivo
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log_message(f"Pipeline guardado en {path}")
            QMessageBox.information(self, "Guardado", "Pipeline guardado correctamente.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Error", f"Error al guardar pipeline: {e}")

    def open_pipeline(self):
        """Abre un pipeline desde archivo JSON (.etl.json)."""
        path, _ = QFileDialog.getOpenFileName(self, "Abrir Pipeline", "", "ETL Pipeline (*.etl.json);;JSON (*.json)")
        if not path:
            return
        try:
            import json
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Limpiar actual
            self.pipeline_canvas.clear_all()
            if hasattr(self.properties_panel, 'node_configs'):
                self.properties_panel.node_configs.clear()
            if hasattr(self.properties_panel, 'current_dataframes'):
                self.properties_panel.current_dataframes.clear()

            # Crear nodos
            for n in data.get('nodes', []):
                nid = int(n['id'])
                ntype = n['type']
                cfg = n.get('config', {})
                subtype = cfg.get('subtype')
                pos_dict = n.get('position', {'x': 0.0, 'y': 0.0})
                pos = QPointF(float(pos_dict.get('x', 0.0)), float(pos_dict.get('y', 0.0)))
                self.pipeline_canvas.add_node_with_id(nid, ntype, pos, subtype)
                # Establecer config cargada (sin dataframes)
                self.pipeline_canvas.graph.nodes[nid]['config'] = cfg
                # Sincronizar con panel de propiedades
                self.properties_panel.node_configs[nid] = cfg

            # Crear conexiones
            for e in data.get('edges', []):
                src = int(e['source'])
                dst = int(e['target'])
                self.pipeline_canvas.add_edge_simple(src, dst)

            self.log_message(f"Pipeline cargado desde {path}")
            
            # Auto-obtener datos para nodos de origen
            self._auto_fetch_source_data()
            
            QMessageBox.information(self, "Cargado", "Pipeline cargado correctamente.\nDatos de origen obtenidos automáticamente.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Error", f"Error al abrir pipeline: {e}")
    
    # ==== Integración Proyecto <-> Diseñador ====
    def get_current_pipeline_content(self) -> dict:
        """Serializa el pipeline actual a un dict con 'nodes' y 'edges'."""
        data = {'nodes': [], 'edges': []}
        # Serializar nodos
        for node_id in self.pipeline_canvas.graph.nodes:
            node = self.pipeline_canvas.graph.nodes[node_id]
            pos = node.get('position')
            if isinstance(pos, QPointF):
                pos_dict = {'x': float(pos.x()), 'y': float(pos.y())}
            elif isinstance(pos, (tuple, list)) and len(pos) == 2:
                pos_dict = {'x': float(pos[0]), 'y': float(pos[1])}
            else:
                pos_dict = {'x': 0.0, 'y': 0.0}
            config = (self.properties_panel.get_node_config(node_id) or {}).copy()
            if not config:
                config = node.get('config', {}).copy()
            config.pop('dataframe', None)
            config.pop('other_dataframe', None)
            data['nodes'].append({
                'id': int(node_id),
                'type': node.get('type'),
                'position': pos_dict,
                'config': config,
            })
        # Serializar edges
        for (src, dst) in self.pipeline_canvas.graph.edges:
            data['edges'].append({'source': int(src), 'target': int(dst)})
        return data

    def load_etl_content_into_designer(self, content: dict) -> None:
        """Carga un contenido de ETL (dict con 'nodes'/'edges') en el Diseñador."""
        try:
            # Limpiar actual
            self.pipeline_canvas.clear_all()
            if hasattr(self.properties_panel, 'node_configs'):
                self.properties_panel.node_configs.clear()
            if hasattr(self.properties_panel, 'current_dataframes'):
                self.properties_panel.current_dataframes.clear()
            # Crear nodos
            for n in content.get('nodes', []):
                nid = int(n['id'])
                ntype = n['type']
                cfg = n.get('config', {})
                subtype = cfg.get('subtype')
                pos_dict = n.get('position', {'x': 0.0, 'y': 0.0})
                pos = QPointF(float(pos_dict.get('x', 0.0)), float(pos_dict.get('y', 0.0)))
                self.pipeline_canvas.add_node_with_id(nid, ntype, pos, subtype)
                self.pipeline_canvas.graph.nodes[nid]['config'] = cfg
                self.properties_panel.node_configs[nid] = cfg
            # Crear conexiones
            for e in content.get('edges', []):
                src = int(e['source'])
                dst = int(e['target'])
                self.pipeline_canvas.add_edge_simple(src, dst)
            self.log_message("ETL cargado en Diseñador desde el Proyecto")
        except Exception as e:
            QMessageBox.critical(self, "Proyecto", f"No se pudo cargar el ETL en el Diseñador: {e}")

    def load_etl_from_project(self, etl_id: str) -> None:
        """Busca un ETL por id en el proyecto y lo carga en el Diseñador."""
        etls = (self.project_manager.project.get('etls') or []) if isinstance(self.project_manager.project, dict) else []
        for etl in etls:
            if str(etl.get('id')) == str(etl_id):
                self.load_etl_content_into_designer(etl.get('content') or {})
                try:
                    idx = self.tabs.indexOf(self.designer_container)
                    if idx != -1:
                        self.tabs.setCurrentIndex(idx)
                except Exception:
                    pass
                try:
                    self._auto_fetch_source_data()
                except Exception:
                    pass
                return
        QMessageBox.information(self, "Proyecto", f"ETL '{etl_id}' no encontrado en el proyecto.")

    def save_designer_into_etl(self, etl_id: str) -> None:
        """Serializa el pipeline actual y lo guarda en el ETL indicado dentro del proyecto."""
        try:
            content = self.get_current_pipeline_content()
            etls = (self.project_manager.project.get('etls') or []) if isinstance(self.project_manager.project, dict) else []
            for idx, etl in enumerate(etls):
                if str(etl.get('id')) == str(etl_id):
                    etl['content'] = content
                    etls[idx] = etl
                    self.project_manager.project['etls'] = etls
                    try:
                        self.project_manager.save()
                    except Exception:
                        pass
                    # Refrescar Explorer si existe
                    if hasattr(self, 'project_explorer'):
                        try:
                            self.project_explorer.refresh()
                            self.project_explorer.project_changed.emit(self.project_manager.project)
                        except Exception:
                            pass
                    self.log_message(f"ETL '{etl_id}' actualizado desde el Diseñador")
                    QMessageBox.information(self, "Proyecto", f"ETL '{etl_id}' actualizado desde el Diseñador.")
                    return
            QMessageBox.information(self, "Proyecto", f"ETL '{etl_id}' no encontrado en el proyecto.")
        except Exception as e:
            QMessageBox.critical(self, "Proyecto", f"No se pudo guardar el Diseñador en el ETL: {e}")
    
    def _auto_fetch_source_data(self):
        """Obtiene automáticamente los datos para todos los nodos de origen después de cargar un pipeline."""
        try:
            source_nodes = []
            # Identificar todos los nodos de origen
            for node_id in self.pipeline_canvas.graph.nodes:
                node = self.pipeline_canvas.graph.nodes[node_id]
                if node.get('type') == 'source':
                    source_nodes.append(node_id)
            
            if not source_nodes:
                self.log_message("No se encontraron nodos de origen para auto-obtener datos")
                return
            
            self.log_message(f"Auto-obteniendo datos para {len(source_nodes)} nodos de origen...")
            
            # Procesar cada nodo de origen
            for node_id in source_nodes:
                try:
                    self._auto_fetch_single_source(node_id)
                except Exception as e:
                    self.log_message(f"Error auto-obteniendo datos del nodo {node_id}: {e}")
                    continue
            
            self.log_message("Auto-obtención de datos completada")
            
        except Exception as e:
            self.log_message(f"Error en auto-obtención de datos: {e}")
    
    def _auto_fetch_single_source(self, node_id):
        """Obtiene automáticamente los datos de un nodo de origen específico."""
        node = self.pipeline_canvas.graph.nodes[node_id]
        config = node.get('config', {})
        subtype = config.get('subtype', '').lower()
        
        self.log_message(f"Auto-obteniendo datos del nodo {node_id} (subtype: {subtype})")
        
        if subtype == 'database':
            # Auto-obtener datos de base de datos
            self._auto_fetch_database_data(node_id, config)
        elif subtype in ['csv', 'excel', 'json', 'parquet']:
            # Auto-cargar archivos
            self._auto_load_file_data(node_id, config, subtype)
        else:
            self.log_message(f"Subtype '{subtype}' no soportado para auto-obtención")
    
    def _auto_fetch_database_data(self, node_id, config):
        """Auto-obtiene datos de una base de datos configurada."""
        try:
            # Verificar que todos los campos necesarios estén presentes
            required_fields = ['db_type', 'host', 'user', 'database', 'query']
            missing_fields = [field for field in required_fields if not config.get(field)]
            
            if missing_fields:
                self.log_message(f"Nodo {node_id}: Faltan campos de BD: {missing_fields}")
                return
            
            db_type = config['db_type']
            host = config['host']
            port = config.get('port', '')
            user = config['user']
            password = config.get('password', '')
            database = config['database']
            query = config['query']
            
            # Construir URL de conexión
            from sqlalchemy import create_engine, text
            from sqlalchemy.engine import URL
            import pandas as pd
            import polars as pl
            
            url = None
            connect_args = {}
            
            if db_type == 'MySQL':
                url = URL.create(
                    drivername='mysql+pymysql',
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else None,
                    database=database or None,
                )
                connect_args = {'connect_timeout': 10}
            elif db_type == 'PostgreSQL':
                url = URL.create(
                    drivername='postgresql+psycopg2',
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else None,
                    database=database or None,
                )
                connect_args = {'connect_timeout': 10}
            elif db_type == 'SQL Server':
                url = URL.create(
                    drivername='mssql+pyodbc',
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else None,
                    database=database or None,
                    query={'driver': 'ODBC Driver 17 for SQL Server'}
                )
                connect_args = {'timeout': 10}
            elif db_type == 'SQLite':
                url = URL.create(drivername='sqlite', database=database)
            else:
                self.log_message(f"Tipo de BD no soportado: {db_type}")
                return
            
            # Ejecutar consulta
            engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args or {})
            with engine.connect() as conn:
                pdf = pd.read_sql(text(query), conn)
            
            try:
                engine.dispose()
            except Exception:
                pass
            
            # Convertir a Polars
            df = pl.from_pandas(pdf) if hasattr(pdf, 'columns') else pl.DataFrame(pdf)
            
            # Guardar en configuración
            config['dataframe'] = df
            self.pipeline_canvas.graph.nodes[node_id]['config'] = config
            self.properties_panel.node_configs[node_id] = config
            self.properties_panel.current_dataframes[node_id] = df
            
            self.log_message(f"Nodo {node_id}: Datos de BD obtenidos automáticamente ({len(df)} filas)")
        
        except Exception as e:
            self.log_message(f"Error auto-obteniendo datos de BD del nodo {node_id}: {e}")
    
    def _auto_load_file_data(self, node_id, config, file_type):
        """Auto-carga datos de un archivo configurado."""
        try:
            file_path = config.get('path')
            if not file_path:
                self.log_message(f"Nodo {node_id}: No hay ruta de archivo configurada")
                return
            
            import os
            if not os.path.exists(file_path):
                self.log_message(f"Nodo {node_id}: Archivo no encontrado: {file_path}")
                return
            
            import pandas as pd
            import polars as pl
            
            # Cargar según el tipo de archivo
            if file_type == 'csv':
                try:
                    df = pl.read_csv(file_path)
                except:
                    try:
                        df = pl.read_csv(file_path, encoding='latin-1')
                    except:
                        pdf = pd.read_csv(file_path, encoding='latin-1')
                        df = pl.from_pandas(pdf) 
            elif file_type == 'excel':
                try:
                    df = pl.read_excel(file_path)
                except:
                    pdf = pd.read_excel(file_path)
                    df = pl.from_pandas(pdf)
            elif file_type == 'json':
                df = pl.read_json(file_path)
            elif file_type == 'parquet':
                df = pl.read_parquet(file_path)
            else:
                self.log_message(f"Tipo de archivo no soportado: {file_type}")
                return
            
            # Guardar en configuración
            config['dataframe'] = df
            self.pipeline_canvas.graph.nodes[node_id]['config'] = config
            self.properties_panel.node_configs[node_id] = config
            self.properties_panel.current_dataframes[node_id] = df
            
            self.log_message(f"Nodo {node_id}: Archivo {file_type.upper()} cargado automáticamente ({len(df)} filas)")
        
        except Exception as e:
            self.log_message(f"Error auto-cargando archivo del nodo {node_id}: {e}")