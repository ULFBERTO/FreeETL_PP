from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QLabel, QPushButton, 
                            QFileDialog, QTableWidget, QTableWidgetItem,
                            QComboBox, QLineEdit, QFormLayout, QGroupBox,
                            QHBoxLayout, QMessageBox, QTabWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
import polars as pl
import pandas as pd
import os
import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class PropertiesPanel(QWidget):
    node_config_changed = pyqtSignal(int, dict)  # Señal cuando cambia la configuración de un nodo
    fetch_connected_data = pyqtSignal(int)  # Señal para solicitar datos de nodos conectados
    
    def __init__(self):
        super().__init__()
        self.setFixedWidth(300)
        self.setup_ui()
        # Almacenamiento para la configuración de los nodos
        self.node_configs = {}  # {node_id: config_dict}
        self.current_dataframes = {}  # {node_id: df}
        # Bandera para evitar autosaves reentrantes durante la reconstrucción del panel
        self._ui_rebuilding = False
        # Autosave diferido
        self._autosave_timer = QTimer(self)
        self._autosave_timer.setSingleShot(True)
        self._autosave_timer.timeout.connect(self._do_autosave)
        self._pending_autosave = None  # (kind, node_id)

    def _schedule_autosave(self, kind, node_id, delay_ms: int = 120):
        """Programa un autosave diferido para evitar ráfagas de eventos."""
        try:
            self._pending_autosave = (kind, node_id)
            if self._autosave_timer.isActive():
                self._autosave_timer.stop()
            self._autosave_timer.start(delay_ms)
        except Exception as e:
            print(f"[PropertiesPanel] Error schedule autosave: {e}")

    def _do_autosave(self):
        """Ejecuta el autosave pendiente si no estamos reconstruyendo la UI."""
        if self._pending_autosave is None:
            return
        kind, node_id = self._pending_autosave
        # Si aún estamos reconstruyendo, reintentar un poco más tarde
        if getattr(self, '_ui_rebuilding', False):
            self._autosave_timer.start(120)
            return
        try:
            if kind == 'source':
                self.save_node_config(node_id, getattr(self, 'current_source_type', 'CSV'))
            elif kind == 'transform':
                self.save_transform_config(node_id, getattr(self, 'current_transform_type', 'Filtro'))
            elif kind == 'destination':
                self.save_destination_config(node_id, getattr(self, 'current_dest_type', 'CSV'))
        finally:
            self._pending_autosave = None
        
    def setup_ui(self):
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)
        
        # Panel vacío inicial
        self.empty_label = QLabel("Seleccione un nodo para configurar")
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.layout.addWidget(self.empty_label)
        
    def show_node_properties(self, node_id, node_type, node_data):
        # Activar bandera de reconstrucción para evitar autosaves reentrantes
        self._ui_rebuilding = True
        # Antes de limpiar, intentar bloquear señales de tablas existentes
        try:
            if hasattr(self, 'column_selection_table') and self.column_selection_table is not None:
                self.column_selection_table.blockSignals(True)
        except Exception:
            pass
        try:
            if hasattr(self, 'data_table') and self.data_table is not None:
                self.data_table.blockSignals(True)
        except Exception:
            pass
        # Limpiar el panel actual de forma segura (sin deleteLater para evitar segfaults)
        try:
            # Limpiar listas de referencias a widgets previos
            if hasattr(self, 'column_checkboxes'):
                self.column_checkboxes = []
            if hasattr(self, 'column_rename_fields'):
                self.column_rename_fields = []
            if hasattr(self, 'join_fields'):
                self.join_fields = {}
        except Exception:
            pass
        for i in reversed(range(self.layout.count())):
            item = self.layout.itemAt(i)
            w = item.widget()
            if w is not None:
                w.setParent(None)
        # Evitar referencias obsoletas a tablas genéricas
        try:
            self.column_selection_table = None
        except Exception:
            pass
        try:
            self.data_table = None
        except Exception:
            pass
            
        # Guardar el ID y tipo de nodo actual
        self.current_node_id = node_id
        self.current_node_type = node_type
        
        # Inicializar la configuración si no existe
        if node_id not in self.node_configs:
            self.node_configs[node_id] = node_data.copy() if isinstance(node_data, dict) else {}
            
        # Usar la configuración guardada
        self.current_node_data = self.node_configs[node_id]
            
        if node_type == 'source':
            self.show_source_properties(node_id, self.current_node_data)
        elif node_type == 'transform':
            self.show_transform_properties(node_id, self.current_node_data)
        elif node_type == 'destination':
            self.show_destination_properties(node_id, self.current_node_data)
        self._ui_rebuilding = False
            
    def show_source_properties(self, node_id, node_data):
        # Grupo para propiedades de fuente
        source_group = QGroupBox("Propiedades de Fuente")
        source_layout = QFormLayout()
        
        # Selector de tipo de fuente
        source_type = QComboBox()
        source_type.addItems(["CSV", "Excel", "JSON", "Parquet", "Base de Datos", "API"])
        
        # Establecer el subtipo actual si existe
        subtype = node_data.get('subtype')
        if subtype == 'csv':
            source_type.setCurrentText("CSV")
        elif subtype == 'excel':
            source_type.setCurrentText("Excel")
        elif subtype == 'json':
            source_type.setCurrentText("JSON")
        elif subtype == 'parquet':
            source_type.setCurrentText("Parquet")
        elif subtype == 'database':
            source_type.setCurrentText("Base de Datos")
        elif subtype == 'api':
            source_type.setCurrentText("API")
            
        source_layout.addRow("Tipo:", source_type)
        
        # Botón Limpiar (auto-guardado activo, sin botón Guardar)
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        source_layout.addRow(button_layout)
        
        # Configuración específica según el tipo
        self.current_source_type = source_type.currentText()
        source_type.currentTextChanged.connect(lambda text: self.update_source_type(node_id, text))
        
        # --- Selección de columnas obligatoria para todos los tipos de fuente ---
        # Cargar el dataframe si existe
        df = node_data.get('dataframe')
        if df is not None:
            # Tabla para mostrar datos
            self.data_table = QTableWidget()
            self.setup_data_preview(self.data_table, df)
            source_layout.addRow("Vista previa:", self.data_table)
            
            # Tabla de selección y renombrado de columnas
            self.column_selection_table = QTableWidget()
            self.column_selection_table.setColumnCount(4)
            self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como", "Tipo"])
            self.column_checkboxes = []
            self.column_rename_fields = []
            self._setup_column_selection_table(node_id, df, node_data)
            source_layout.addRow("Columnas a pasar:", self.column_selection_table)
            # Auto-guardado al cambiar selección/renombrado de columnas
            self.column_selection_table.itemChanged.connect(lambda *_: self._schedule_autosave('source', node_id))
            for rf in self.column_rename_fields:
                try:
                    rf.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))
                except Exception:
                    pass
        
        # Path del archivo y botón de carga (solo para fuentes basadas en archivos)
        if (subtype in ('csv', 'excel', 'json', 'parquet')) or (source_type.currentText() in ("CSV", "Excel", "JSON", "Parquet")):
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            source_layout.addRow("Ruta del archivo:", file_path)
            self.file_path_field = file_path
            file_path.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))

        if subtype == 'csv' or source_type.currentText() == "CSV":
            load_button = QPushButton("Cargar Archivo")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='csv'))
            source_layout.addRow(load_button)
        elif subtype == 'excel' or source_type.currentText() == "Excel":
            load_button = QPushButton("Cargar Archivo Excel")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='excel'))
            source_layout.addRow(load_button)
        elif subtype == 'json' or source_type.currentText() == "JSON":
            load_button = QPushButton("Cargar Archivo JSON")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='json'))
            source_layout.addRow(load_button)
        elif subtype == 'parquet' or source_type.currentText() == "Parquet":
            load_button = QPushButton("Cargar Archivo Parquet")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='parquet'))
            source_layout.addRow(load_button)
        
        if subtype == 'database' or source_type.currentText() == "Base de Datos":
            # Configuración de base de datos
            db_type = QComboBox()
            db_type.addItems(["MySQL", "PostgreSQL", "SQL Server", "SQLite"])
            source_layout.addRow("Base de datos:", db_type)
            
            # Establecer valores guardados si existen
            if 'db_type' in node_data:
                db_type.setCurrentText(node_data['db_type'])
            
            host = QLineEdit()
            host.setText(node_data.get('host', ''))
            source_layout.addRow("Host:", host)
            
            port = QLineEdit()
            port.setText(node_data.get('port', ''))
            source_layout.addRow("Puerto:", port)
            
            user = QLineEdit()
            user.setText(node_data.get('user', ''))
            source_layout.addRow("Usuario:", user)
            
            password = QLineEdit()
            password.setText(node_data.get('password', ''))
            password.setEchoMode(QLineEdit.EchoMode.Password)
            source_layout.addRow("Contraseña:", password)
            
            database = QLineEdit()
            database.setText(node_data.get('database', ''))
            source_layout.addRow("Base de datos:", database)
            
            query = QLineEdit()
            query.setText(node_data.get('query', ''))
            source_layout.addRow("Consulta SQL:", query)
            
            # Guardar referencias a los campos
            self.db_fields = {
                'db_type': db_type,
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database,
                'query': query
            }
            # Auto-guardado para campos de BD
            db_type.currentTextChanged.connect(lambda *_: self._schedule_autosave('source', node_id))
            for _fld in [host, port, user, password, database, query]:
                _fld.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))
            # Botones Base de Datos: Probar conexión y Vista previa
            btn_row = QHBoxLayout()
            test_btn = QPushButton("Probar conexión")
            test_btn.clicked.connect(self._on_test_source_db_connection)
            preview_btn = QPushButton("Vista previa")
            preview_btn.setToolTip("Ejecuta la consulta y muestra una vista previa (hasta 100 filas)")
            preview_btn.clicked.connect(lambda: self._on_preview_source_db(node_id))
            btn_row.addWidget(test_btn)
            btn_row.addWidget(preview_btn)
            source_layout.addRow(btn_row)
            
        elif subtype == 'api' or source_type.currentText() == "API":
            # Configuración de API
            url = QLineEdit()
            url.setText(node_data.get('url', ''))
            source_layout.addRow("URL:", url)
            
            method = QComboBox()
            method.addItems(["GET", "POST", "PUT", "DELETE"])
            if 'method' in node_data:
                method.setCurrentText(node_data['method'])
            source_layout.addRow("Método:", method)
            
            headers = QLineEdit()
            headers.setText(node_data.get('headers', ''))
            source_layout.addRow("Headers:", headers)
            
            params = QLineEdit()
            params.setText(node_data.get('params', ''))
            source_layout.addRow("Parámetros:", params)
            
            # Guardar referencias a los campos
            self.api_fields = {
                'url': url,
                'method': method,
                'headers': headers,
                'params': params
            }
            # Auto-guardado para campos de API
            method.currentTextChanged.connect(lambda *_: self._schedule_autosave('source', node_id))
            url.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))
            headers.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))
            params.editingFinished.connect(lambda: self._schedule_autosave('source', node_id))
            
        source_group.setLayout(source_layout)
        self.layout.addWidget(source_group)
        
    def show_transform_properties(self, node_id, node_data):
        # Grupo para propiedades de transformación
        transform_group = QGroupBox("Propiedades de Transformación")
        transform_layout = QFormLayout()
        
        # Selector de tipo de transformación
        transform_type = QComboBox()
        transform_type.addItems(["Filtro", "Unión", "Agregación", "Mapeo", "Casteo"])
        
        # Establecer el subtipo actual si existe
        subtype = node_data.get('subtype')
        if subtype == 'filter':
            transform_type.setCurrentText("Filtro")
        elif subtype == 'join':
            transform_type.setCurrentText("Unión")
        elif subtype == 'aggregate':
            transform_type.setCurrentText("Agregación")
        elif subtype == 'map':
            transform_type.setCurrentText("Mapeo")
        elif subtype == 'cast':
            transform_type.setCurrentText("Casteo")
            
        transform_layout.addRow("Tipo:", transform_type)
        
        # Botón Limpiar (auto-guardado activo)
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        # Botón para obtener datos manualmente de los nodos conectados
        get_data_button = QPushButton("⟳ Obtener datos")
        get_data_button.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        get_data_button.setToolTip("Obtiene los datos de los nodos conectados a las entradas")
        get_data_button.clicked.connect(lambda: self.fetch_data_from_connected_nodes(node_id))
        transform_layout.addRow(get_data_button)
        
        transform_layout.addRow(button_layout)
        
        # Campos específicos según el tipo
        self.current_transform_type = transform_type.currentText()
        transform_type.currentTextChanged.connect(lambda text: self.update_transform_type(node_id, text))
        
        if subtype == 'filter' or transform_type.currentText() == "Filtro":
            # UI amigable para REGLAS DE FILTRO
            filter_group = QGroupBox("Reglas de filtro")
            fg_layout = QVBoxLayout()

            # Modo: Todas (AND) / Cualquiera (OR)
            mode_combo = QComboBox()
            mode_combo.addItems(["Todas (AND)", "Cualquiera (OR)"])
            mode_val = (node_data.get('filter_mode') or 'all').lower()
            mode_combo.setCurrentText("Cualquiera (OR)" if mode_val == 'any' else "Todas (AND)")
            self.filter_mode_combo = mode_combo
            fg_layout.addWidget(QLabel("Modo de combinación de reglas:"))
            fg_layout.addWidget(mode_combo)

            # Tabla de reglas
            rules_table = QTableWidget()
            rules_table.setColumnCount(3)
            rules_table.setHorizontalHeaderLabels(["Columna", "Operador", "Valor"])
            self.filter_rules_table = rules_table

            # Cargar columnas disponibles
            cols = []
            try:
                df_for_cols = node_data.get('dataframe')
                if df_for_cols is not None:
                    if isinstance(df_for_cols, pd.DataFrame):
                        cols = list(df_for_cols.columns)
                    else:
                        cols = list(df_for_cols.columns)
            except Exception:
                pass

            # Operadores soportados
            ops = ['>', '<', '==', '!=', '>=', '<=', 'contains', 'in', 'isnull', 'notnull']

            # Cargar reglas existentes
            existing = node_data.get('filter_rules') if isinstance(node_data.get('filter_rules'), list) else []
            if not existing and node_data.get('filter_expr'):
                existing = []  # no intentamos parsear textos complejos
            if not existing:
                rules_table.setRowCount(1)
                # Fila vacía por defecto
                row = 0
                col_cb = QComboBox(); col_cb.addItems(cols)
                op_cb = QComboBox(); op_cb.addItems(ops)
                val_le = QLineEdit()
                rules_table.setCellWidget(row, 0, col_cb)
                rules_table.setCellWidget(row, 1, op_cb)
                rules_table.setCellWidget(row, 2, val_le)
                # Autosave
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            else:
                rules_table.setRowCount(len(existing))
                for row, r in enumerate(existing):
                    col_cb = QComboBox(); col_cb.addItems(cols)
                    if r.get('column') in cols:
                        col_cb.setCurrentText(r.get('column'))
                    op_cb = QComboBox(); op_cb.addItems(ops)
                    if r.get('op') in ops:
                        op_cb.setCurrentText(r.get('op'))
                    val_le = QLineEdit()
                    if r.get('value') is not None and r.get('op') not in ('isnull', 'notnull'):
                        val_le.setText(str(r.get('value')))
                    rules_table.setCellWidget(row, 0, col_cb)
                    rules_table.setCellWidget(row, 1, op_cb)
                    rules_table.setCellWidget(row, 2, val_le)
                    col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))

            # Botones de gestión
            btns = QHBoxLayout()
            add_btn = QPushButton("Agregar regla")
            del_btn = QPushButton("Eliminar seleccionadas")
            def add_rule():
                r = rules_table.rowCount()
                rules_table.insertRow(r)
                col_cb = QComboBox(); col_cb.addItems(cols)
                op_cb = QComboBox(); op_cb.addItems(ops)
                val_le = QLineEdit()
                rules_table.setCellWidget(r, 0, col_cb)
                rules_table.setCellWidget(r, 1, op_cb)
                rules_table.setCellWidget(r, 2, val_le)
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                self._schedule_autosave('transform', node_id)
            def del_selected():
                rows = sorted({i.row() for i in rules_table.selectedIndexes()}, reverse=True)
                for r in rows:
                    rules_table.removeRow(r)
                self._schedule_autosave('transform', node_id)
            add_btn.clicked.connect(add_rule)
            del_btn.clicked.connect(del_selected)
            btns.addWidget(add_btn)
            btns.addWidget(del_btn)

            fg_layout.addWidget(rules_table)
            fg_layout.addLayout(btns)
            filter_group.setLayout(fg_layout)
            transform_layout.addRow(filter_group)
            mode_combo.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
            
        elif subtype == 'join' or transform_type.currentText() == "Unión":
            # SECCIÓN 1: CONFIGURACIÓN BÁSICA DE LA UNIÓN
            join_config_group = QGroupBox("Configuración de Unión")
            join_config_layout = QFormLayout()
            
            # Tipo de unión
            join_type = QComboBox()
            join_type.addItems(["Inner", "Left", "Right", "Outer"])
            if 'join_type' in node_data:
                join_type.setCurrentText(node_data['join_type'])
            join_config_layout.addRow("Tipo de unión:", join_type)
            
            # Columnas de unión
            join_cols = QLineEdit()
            join_cols.setText(node_data.get('join_cols', ''))
            join_cols.setPlaceholderText("columna1,columna2")
            join_config_layout.addRow("Columnas de unión:", join_cols)

            # Pares de claves (izquierda:derecha) opcional
            join_pairs = QLineEdit()
            join_pairs.setText(node_data.get('join_pairs', ''))
            join_pairs.setPlaceholderText("col_izq:col_der,col2_izq:col2_der")
            join_config_layout.addRow("Pares de claves (opcional):", join_pairs)

            # Sufijo para columnas de la derecha
            right_suffix = QLineEdit()
            right_suffix.setText(node_data.get('right_suffix', '_right'))
            right_suffix.setPlaceholderText("_right")
            join_config_layout.addRow("Sufijo derecha:", right_suffix)

            # Acciones rápidas: detectar claves e intercambiar entradas
            detect_btn = QPushButton("Detectar claves")
            swap_btn = QPushButton("Intercambiar entradas")
            detect_btn.clicked.connect(lambda: self._detect_join_keys(node_id))
            swap_btn.clicked.connect(lambda: self._swap_join_inputs(node_id))
            join_config_layout.addRow(detect_btn, swap_btn)
            
            join_config_group.setLayout(join_config_layout)
            transform_layout.addRow(join_config_group)
            # Auto-guardado de campos de unión
            join_type.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
            join_cols.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            join_pairs.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            right_suffix.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            
            # SECCIÓN 2: VISUALIZACIÓN DE DATOS DE ENTRADA
            data_tabs = QTabWidget()
            
            # Pestaña 1: Datos de la primera entrada
            if 'dataframe' in node_data:
                df1 = node_data['dataframe']
                table1 = QTableWidget()
                self.setup_data_preview(table1, df1)
                data_tabs.addTab(table1, "Origen 1")
                
            # Pestaña 2: Datos de la segunda entrada
            if 'other_dataframe' in node_data:
                df2 = node_data['other_dataframe']
                table2 = QTableWidget()
                self.setup_data_preview(table2, df2)
                data_tabs.addTab(table2, "Origen 2")
                
            transform_layout.addRow("Datos de origen:", data_tabs)
            
            # SECCIÓN 3: SELECCIÓN DE COLUMNAS PARA LA SALIDA
            column_selection_group = QGroupBox("Selección de Columnas para Salida")
            column_selection_layout = QVBoxLayout()
            
            # Crear tabla para seleccionar columnas
            column_table = QTableWidget()
            column_table.setColumnCount(4)
            column_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como", "Tipo"])
            
            # Obtener todas las columnas disponibles
            all_columns = []
            
            # Inicializar listas para checkboxes y campos de renombrado
            self.column_checkboxes = []
            self.column_rename_fields = []
            
            if 'dataframe' in node_data:
                df1 = node_data['dataframe']
                all_columns.extend([f"Origen1.{col}" for col in df1.columns])
                
            if 'other_dataframe' in node_data:
                df2 = node_data['other_dataframe']
                all_columns.extend([f"Origen2.{col}" for col in df2.columns])
                
            # Extraer selección y renombrado guardados (usar nombres calificados Origen1./Origen2.)
            selected_full = []
            renamed_full = {}
            if 'output_cols' in node_data and node_data['output_cols']:
                selected_full = [col.strip() for col in node_data['output_cols'].split(',') if col.strip()]
            if 'column_rename' in node_data and node_data['column_rename']:
                rename_pairs = node_data['column_rename'].split(',')
                for pair in rename_pairs:
                    if ':' in pair:
                        old_name, new_name = pair.split(':', 1)
                        renamed_full[old_name.strip()] = new_name.strip()
            
            # Llenar la tabla con todas las columnas disponibles
            column_table.setRowCount(len(all_columns))
            
            for i, col_name in enumerate(all_columns):
                # Columna 1: Checkbox para seleccionar
                checkbox = QTableWidgetItem()
                base_col = col_name.split('.', 1)[-1]
                is_selected = (col_name in selected_full)
                if not selected_full:
                    is_selected = True
                checkbox.setCheckState(Qt.CheckState.Checked if is_selected else Qt.CheckState.Unchecked)
                column_table.setItem(i, 0, checkbox)
                self.column_checkboxes.append(checkbox)
                
                # Columna 2: Nombre de la columna
                column_name_item = QTableWidgetItem(col_name)
                column_table.setItem(i, 1, column_name_item)
                
                # Columna 3: Campo para renombrar
                rename_field = QLineEdit()
                if col_name in renamed_full:
                    rename_field.setText(renamed_full[col_name])
                else:
                    rename_field.setPlaceholderText(base_col)
                column_table.setCellWidget(i, 2, rename_field)
                self.column_rename_fields.append(rename_field)

                # Columna 4: Tipo (solo lectura)
                try:
                    dtype_txt = ""
                    if col_name.startswith("Origen1.") and 'dataframe' in node_data:
                        df1 = node_data.get('dataframe')
                        dtype_txt = self._dtype_str(df1, base_col)
                    elif col_name.startswith("Origen2.") and 'other_dataframe' in node_data:
                        df2 = node_data.get('other_dataframe')
                        dtype_txt = self._dtype_str(df2, base_col)
                    dtype_item = QTableWidgetItem(dtype_txt)
                    dtype_item.setFlags(dtype_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    column_table.setItem(i, 3, dtype_item)
                except Exception:
                    pass
            
            # Ajustar el tamaño de la tabla
            column_table.resizeColumnsToContents()
            column_selection_layout.addWidget(column_table)
            
            # Botones para seleccionar/deseleccionar todas las columnas
            select_buttons_layout = QHBoxLayout()
            
            select_all_button = QPushButton("Seleccionar Todas")
            select_all_button.clicked.connect(lambda: self.select_all_columns(True))
            select_buttons_layout.addWidget(select_all_button)
            
            deselect_all_button = QPushButton("Deseleccionar Todas")
            deselect_all_button.clicked.connect(lambda: self.select_all_columns(False))
            select_buttons_layout.addWidget(deselect_all_button)
            
            column_selection_layout.addLayout(select_buttons_layout)
            
            # Asignar el layout al grupo
            column_selection_group.setLayout(column_selection_layout)
            transform_layout.addRow(column_selection_group)
            
            # Guardar referencias para usar al guardar
            self.join_fields = {
                'join_type': join_type,
                'join_cols': join_cols,
                'join_pairs': join_pairs,
                'right_suffix': right_suffix,
                'column_table': column_table
            }
            # Auto-guardado para selección/renombrado de columnas
            column_table.itemChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
            for rf in self.column_rename_fields:
                try:
                    rf.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                except Exception:
                    pass
            
        elif subtype == 'aggregate' or transform_type.currentText() == "Agregación":
            # UI amigable para Agregación
            agg_group = QGroupBox("Configuración de Agregación")
            agg_layout = QVBoxLayout()

            # Tabla de Group By (checkbox + nombre)
            gb_table = QTableWidget(); gb_table.setColumnCount(2)
            gb_table.setHorizontalHeaderLabels(["Agrupar", "Columna"])
            self.agg_group_by_table = gb_table

            cols = []
            try:
                df_for_cols = node_data.get('dataframe')
                if df_for_cols is not None:
                    if isinstance(df_for_cols, pd.DataFrame):
                        cols = list(df_for_cols.columns)
                    else:
                        cols = list(df_for_cols.columns)
            except Exception:
                pass

            saved_group_by = []
            if isinstance(node_data.get('group_by_list'), list):
                saved_group_by = [str(c) for c in node_data['group_by_list']]
            elif node_data.get('group_by'):
                saved_group_by = [c.strip() for c in str(node_data['group_by']).split(',') if c.strip()]

            gb_table.setRowCount(len(cols))
            for i, c in enumerate(cols):
                chk = QTableWidgetItem()
                chk.setFlags(chk.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                chk.setCheckState(Qt.CheckState.Checked if c in saved_group_by else Qt.CheckState.Unchecked)
                gb_table.setItem(i, 0, chk)
                name_item = QTableWidgetItem(c)
                name_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
                gb_table.setItem(i, 1, name_item)
            gb_table.itemChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))

            # Tabla de funciones (Columna, Función, Alias)
            agg_table = QTableWidget(); agg_table.setColumnCount(3)
            agg_table.setHorizontalHeaderLabels(["Columna", "Función", "Alias"])
            self.agg_ops_table = agg_table

            funcs = ['sum', 'avg', 'min', 'max', 'count']
            saved_aggs = node_data.get('aggs') if isinstance(node_data.get('aggs'), list) else []
            if not saved_aggs and node_data.get('agg_funcs'):
                saved_aggs = []  # no intentamos parsear textos complejos aquí

            if not saved_aggs:
                agg_table.setRowCount(1)
                r = 0
                col_cb = QComboBox(); col_cb.addItems(cols)
                fn_cb = QComboBox(); fn_cb.addItems(funcs)
                alias_le = QLineEdit()
                agg_table.setCellWidget(r, 0, col_cb)
                agg_table.setCellWidget(r, 1, fn_cb)
                agg_table.setCellWidget(r, 2, alias_le)
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                fn_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                alias_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            else:
                agg_table.setRowCount(len(saved_aggs))
                for r, ag in enumerate(saved_aggs):
                    col_cb = QComboBox(); col_cb.addItems(cols)
                    if ag.get('col') in cols:
                        col_cb.setCurrentText(ag.get('col'))
                    fn_cb = QComboBox(); fn_cb.addItems(funcs)
                    if ag.get('func') in funcs:
                        fn_cb.setCurrentText(ag.get('func'))
                    alias_le = QLineEdit(); alias_le.setText(ag.get('as',''))
                    agg_table.setCellWidget(r, 0, col_cb)
                    agg_table.setCellWidget(r, 1, fn_cb)
                    agg_table.setCellWidget(r, 2, alias_le)
                    col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    fn_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    alias_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))

            # Botones de gestión para aggs
            abtns = QHBoxLayout()
            add_agg_btn = QPushButton("Agregar función")
            del_agg_btn = QPushButton("Eliminar seleccionadas")
            def add_agg():
                r = agg_table.rowCount(); agg_table.insertRow(r)
                col_cb = QComboBox(); col_cb.addItems(cols)
                fn_cb = QComboBox(); fn_cb.addItems(funcs)
                alias_le = QLineEdit()
                agg_table.setCellWidget(r, 0, col_cb)
                agg_table.setCellWidget(r, 1, fn_cb)
                agg_table.setCellWidget(r, 2, alias_le)
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                fn_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                alias_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                self._schedule_autosave('transform', node_id)
            def del_agg():
                rows = sorted({i.row() for i in agg_table.selectedIndexes()}, reverse=True)
                for r in rows:
                    agg_table.removeRow(r)
                self._schedule_autosave('transform', node_id)
            add_agg_btn.clicked.connect(add_agg)
            del_agg_btn.clicked.connect(del_agg)
            abtns.addWidget(add_agg_btn)
            abtns.addWidget(del_agg_btn)

            agg_layout.addWidget(QLabel("Agrupar por:"))
            agg_layout.addWidget(gb_table)
            agg_layout.addWidget(QLabel("Funciones:"))
            agg_layout.addWidget(agg_table)
            agg_layout.addLayout(abtns)
            agg_group.setLayout(agg_layout)
            transform_layout.addRow(agg_group)
            
        elif subtype == 'map' or transform_type.currentText() == "Mapeo":
            # UI amigable para MAPEOS
            map_group = QGroupBox("Operaciones de mapeo")
            mg_layout = QVBoxLayout()
            ops_table = QTableWidget(); ops_table.setColumnCount(5)
            ops_table.setHorizontalHeaderLabels(["Nuevo nombre", "Operación", "A", "B", "Valor"])
            self.map_ops_table = ops_table

            # Columnas disponibles
            cols = []
            try:
                df_for_cols = node_data.get('dataframe')
                if df_for_cols is not None:
                    if isinstance(df_for_cols, pd.DataFrame):
                        cols = list(df_for_cols.columns)
                    else:
                        cols = list(df_for_cols.columns)
            except Exception:
                pass

            # Cargar operaciones existentes
            existing_ops = node_data.get('map_ops') if isinstance(node_data.get('map_ops'), list) else []
            if not existing_ops and node_data.get('map_expr'):
                existing_ops = []
            if not existing_ops:
                ops_table.setRowCount(1)
                r = 0
                new_le = QLineEdit()
                op_cb = QComboBox(); op_cb.addItems(['add','sub','mul','div','concat','literal','copy','upper','lower','length'])
                a_cb = QComboBox(); a_cb.addItems(cols)
                b_cb = QComboBox(); b_cb.addItems(cols)
                val_le = QLineEdit()
                ops_table.setCellWidget(r,0,new_le)
                ops_table.setCellWidget(r,1,op_cb)
                ops_table.setCellWidget(r,2,a_cb)
                ops_table.setCellWidget(r,3,b_cb)
                ops_table.setCellWidget(r,4,val_le)
                new_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                a_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                b_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            else:
                ops_table.setRowCount(len(existing_ops))
                for r, op in enumerate(existing_ops):
                    new_le = QLineEdit(); new_le.setText(op.get('new_col',''))
                    op_cb = QComboBox(); op_cb.addItems(['add','sub','mul','div','concat','literal','copy','upper','lower','length'])
                    if op.get('op_type'):
                        op_cb.setCurrentText(op.get('op_type'))
                    a_cb = QComboBox(); a_cb.addItems(cols)
                    if op.get('a') in cols:
                        a_cb.setCurrentText(op.get('a'))
                    b_cb = QComboBox(); b_cb.addItems(cols)
                    if op.get('b') in cols:
                        b_cb.setCurrentText(op.get('b'))
                    val_le = QLineEdit()
                    if op.get('value') is not None:
                        val_le.setText(str(op.get('value')))
                    ops_table.setCellWidget(r,0,new_le)
                    ops_table.setCellWidget(r,1,op_cb)
                    ops_table.setCellWidget(r,2,a_cb)
                    ops_table.setCellWidget(r,3,b_cb)
                    ops_table.setCellWidget(r,4,val_le)
                    new_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                    op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    a_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    b_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))

            # Botones
            mbtns = QHBoxLayout()
            add_btn = QPushButton("Agregar operación")
            del_btn = QPushButton("Eliminar seleccionadas")
            def add_op():
                r = ops_table.rowCount()
                ops_table.insertRow(r)
                new_le = QLineEdit()
                op_cb = QComboBox(); op_cb.addItems(['add','sub','mul','div','concat','literal','copy','upper','lower','length'])
                a_cb = QComboBox(); a_cb.addItems(cols)
                b_cb = QComboBox(); b_cb.addItems(cols)
                val_le = QLineEdit()
                ops_table.setCellWidget(r,0,new_le)
                ops_table.setCellWidget(r,1,op_cb)
                ops_table.setCellWidget(r,2,a_cb)
                ops_table.setCellWidget(r,3,b_cb)
                ops_table.setCellWidget(r,4,val_le)
                new_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                op_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                a_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                b_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                val_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                self._schedule_autosave('transform', node_id)
            def del_ops():
                rows = sorted({i.row() for i in ops_table.selectedIndexes()}, reverse=True)
                for r in rows:
                    ops_table.removeRow(r)
                self._schedule_autosave('transform', node_id)
            add_btn.clicked.connect(add_op)
            del_btn.clicked.connect(del_ops)
            mbtns.addWidget(add_btn)
            mbtns.addWidget(del_btn)

            mg_layout.addWidget(ops_table)
            mg_layout.addLayout(mbtns)
            map_group.setLayout(mg_layout)
            transform_layout.addRow(map_group)

        elif subtype == 'cast' or transform_type.currentText() == "Casteo":
            # UI para Casteo de tipos
            cast_group = QGroupBox("Casteo de tipos de columna")
            cg_layout = QVBoxLayout()
            cast_table = QTableWidget(); cast_table.setColumnCount(3)
            cast_table.setHorizontalHeaderLabels(["Columna", "Tipo destino", "Formato (opcional)"])
            self.cast_ops_table = cast_table

            # Columnas disponibles
            cols = []
            try:
                df_for_cols = node_data.get('dataframe')
                if df_for_cols is not None:
                    cols = list(df_for_cols.columns)
            except Exception:
                pass

            # Tipos soportados
            type_choices = ["Int64", "Int32", "Float64", "Float32", "Utf8", "Boolean", "Date", "Datetime"]

            saved_ops = node_data.get('cast_ops') if isinstance(node_data.get('cast_ops'), list) else []
            # Si no hay df disponible (p.ej. al abrir pipeline), usar columnas guardadas como opciones
            if not cols and saved_ops:
                try:
                    saved_cols = [op.get('col') for op in saved_ops if op.get('col')]
                    cols = sorted(list({c for c in saved_cols}))
                except Exception:
                    pass
            if not saved_ops:
                cast_table.setRowCount(1)
                r = 0
                col_cb = QComboBox(); col_cb.addItems(cols)
                to_cb = QComboBox(); to_cb.addItems(type_choices)
                fmt_le = QLineEdit()
                fmt_le.setPlaceholderText("%Y-%m-%d, %Y-%m-%d %H:%M:%S, etc.")
                cast_table.setCellWidget(r, 0, col_cb)
                cast_table.setCellWidget(r, 1, to_cb)
                cast_table.setCellWidget(r, 2, fmt_le)
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                to_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                fmt_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                # Auto-aplicar preview tras cambios
                col_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                to_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                fmt_le.editingFinished.connect(lambda: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
            else:
                cast_table.setRowCount(len(saved_ops))
                for r, op in enumerate(saved_ops):
                    col_cb = QComboBox(); col_cb.addItems(cols)
                    # Asegurar que la columna guardada esté disponible en el combo
                    saved_col = op.get('col')
                    if saved_col and saved_col not in cols:
                        col_cb.addItem(saved_col)
                    if saved_col:
                        col_cb.setCurrentText(saved_col)
                    to_cb = QComboBox(); to_cb.addItems(type_choices)
                    if op.get('to') in type_choices:
                        to_cb.setCurrentText(op.get('to'))
                    fmt_le = QLineEdit(); fmt_le.setText(op.get('fmt',''))
                    fmt_le.setPlaceholderText("%Y-%m-%d, %Y-%m-%d %H:%M:%S, etc.")
                    cast_table.setCellWidget(r, 0, col_cb)
                    cast_table.setCellWidget(r, 1, to_cb)
                    cast_table.setCellWidget(r, 2, fmt_le)
                    col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    to_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                    fmt_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                    # Auto-aplicar preview tras cambios
                    col_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                    to_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                    fmt_le.editingFinished.connect(lambda: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))

            # Botones de gestión
            cbtns = QHBoxLayout()
            add_cast_btn = QPushButton("Agregar casteo")
            del_cast_btn = QPushButton("Eliminar seleccionadas")
            def add_cast():
                r = cast_table.rowCount(); cast_table.insertRow(r)
                col_cb = QComboBox(); col_cb.addItems(cols)
                to_cb = QComboBox(); to_cb.addItems(type_choices)
                fmt_le = QLineEdit(); fmt_le.setPlaceholderText("%Y-%m-%d, %Y-%m-%d %H:%M:%S, etc.")
                cast_table.setCellWidget(r, 0, col_cb)
                cast_table.setCellWidget(r, 1, to_cb)
                cast_table.setCellWidget(r, 2, fmt_le)
                col_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                to_cb.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
                fmt_le.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
                # Auto-aplicar preview tras cambios
                col_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                to_cb.currentTextChanged.connect(lambda *_: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                fmt_le.editingFinished.connect(lambda: QTimer.singleShot(200, lambda: self.fetch_connected_data.emit(node_id)))
                self._schedule_autosave('transform', node_id)
            def del_cast():
                rows = sorted({i.row() for i in cast_table.selectedIndexes()}, reverse=True)
                for r in rows:
                    cast_table.removeRow(r)
                self._schedule_autosave('transform', node_id)
            add_cast_btn.clicked.connect(add_cast)
            del_cast_btn.clicked.connect(del_cast)
            cbtns.addWidget(add_cast_btn)
            cbtns.addWidget(del_cast_btn)

            cg_layout.addWidget(cast_table)
            cg_layout.addLayout(cbtns)
            # Acción explícita: aplicar casteo ahora
            apply_btn = QPushButton("Aplicar casteo ahora")
            apply_btn.setToolTip("Aplica los casteos definidos y refresca vista previa y tipos")
            apply_btn.clicked.connect(lambda: self.fetch_connected_data.emit(node_id))
            cg_layout.addWidget(apply_btn)
            cast_group.setLayout(cg_layout)
            transform_layout.addRow(cast_group)
            
        # --- Selección de columnas obligatoria para todos los nodos de transformación ---
        # Nota: para 'Unión' ya se muestra una sección dedicada de selección/renombrado,
        # por lo que NO se debe renderizar la tabla genérica para evitar inconsistencias.
        df = node_data.get('dataframe')
        is_join = (node_data.get('subtype') == 'join')
        if df is not None:
            self.data_table = QTableWidget()
            self.setup_data_preview(self.data_table, df)
            transform_layout.addRow("Vista previa:", self.data_table)
            if not is_join:
                self.column_selection_table = QTableWidget()
                self.column_selection_table.setColumnCount(4)
                self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como", "Tipo"])
                self.column_checkboxes = []
                self.column_rename_fields = []
                self._setup_column_selection_table(node_id, df, node_data)
                transform_layout.addRow("Columnas a pasar:", self.column_selection_table)
        
        transform_group.setLayout(transform_layout)
        self.layout.addWidget(transform_group)
        
    def show_destination_properties(self, node_id, node_data):
        # Grupo para propiedades de destino
        dest_group = QGroupBox("Propiedades de Destino")
        dest_layout = QFormLayout()
        
        # Selector de tipo de destino
        dest_type = QComboBox()
        dest_type.addItems(["CSV", "Excel", "JSON", "Parquet", "Base de Datos", "API"])
        
        # Establecer el subtipo actual si existe
        subtype = node_data.get('subtype')
        if subtype == 'csv':
            dest_type.setCurrentText("CSV")
        elif subtype == 'excel':
            dest_type.setCurrentText("Excel")
        elif subtype == 'json':
            dest_type.setCurrentText("JSON")
        elif subtype == 'parquet':
            dest_type.setCurrentText("Parquet")
        elif subtype == 'database':
            dest_type.setCurrentText("Base de Datos")
        elif subtype == 'api':
            dest_type.setCurrentText("API")
            
        dest_layout.addRow("Tipo:", dest_type)
        
        # Botón "Obtener Datos" para previsualizar
        fetch_data_button = QPushButton("Obtener Datos")
        fetch_data_button.setToolTip("Obtiene y muestra los datos que llegarían a este destino")
        fetch_data_button.clicked.connect(lambda: self.fetch_data_for_destination(node_id))
        dest_layout.addRow(fetch_data_button)
        
        # Botón Limpiar (auto-guardado activo)
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        dest_layout.addRow(button_layout)
        
        # Campos específicos según el tipo
        self.current_dest_type = dest_type.currentText()
        dest_type.currentTextChanged.connect(lambda text: self.update_destination_type(node_id, text))
        
        # --- Selección de columnas obligatoria para todos los nodos de destino ---
        df = node_data.get('dataframe')
        if df is not None:
            self.data_table = QTableWidget()
            self.setup_data_preview(self.data_table, df)
            dest_layout.addRow("Vista previa:", self.data_table)
            self.column_selection_table = QTableWidget()
            self.column_selection_table.setColumnCount(4)
            self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como", "Tipo"])
            self.column_checkboxes = []
            self.column_rename_fields = []
            self._setup_column_selection_table(node_id, df, node_data)
            dest_layout.addRow("Columnas a guardar:", self.column_selection_table)
            # Auto-guardado al cambiar selección/renombrado de columnas
            self.column_selection_table.itemChanged.connect(lambda *_: self._schedule_autosave('destination', node_id))
            for rf in self.column_rename_fields:
                try:
                    rf.editingFinished.connect(lambda: self._schedule_autosave('destination', node_id))
                except Exception:
                    pass
        if subtype == 'csv' or dest_type.currentText() == "CSV":
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            dest_layout.addRow("Ruta del archivo:", file_path)
            self.dest_file_path = file_path
            file_path.editingFinished.connect(lambda: self._schedule_autosave('destination', node_id))
            select_path = QPushButton("Seleccionar ruta")
            select_path.clicked.connect(lambda: self.select_output_path('csv'))
            dest_layout.addRow(select_path)
            format_type = QComboBox()
            format_type.addItems(["CSV", "Excel", "Parquet", "JSON"])
            if 'format' in node_data:
                format_type.setCurrentText(node_data['format'])
            dest_layout.addRow("Formato:", format_type)
            self.dest_format = format_type
            format_type.currentTextChanged.connect(lambda *_: self._schedule_autosave('destination', node_id))
        elif subtype == 'excel' or dest_type.currentText() == "Excel":
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            dest_layout.addRow("Ruta del archivo:", file_path)
            self.dest_file_path = file_path
            select_path = QPushButton("Seleccionar ruta")
            select_path.clicked.connect(lambda: self.select_output_path('excel'))
            dest_layout.addRow(select_path)
            format_type = QComboBox()
            format_type.addItems(["Excel"])
            if 'format' in node_data:
                format_type.setCurrentText(node_data['format'])
            dest_layout.addRow("Formato:", format_type)
            self.dest_format = format_type
        elif subtype == 'json' or dest_type.currentText() == "JSON":
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            dest_layout.addRow("Ruta del archivo:", file_path)
            self.dest_file_path = file_path
            select_path = QPushButton("Seleccionar ruta")
            select_path.clicked.connect(lambda: self.select_output_path('json'))
            dest_layout.addRow(select_path)
            format_type = QComboBox()
            format_type.addItems(["JSON"])
            if 'format' in node_data:
                format_type.setCurrentText(node_data['format'])
            dest_layout.addRow("Formato:", format_type)
            self.dest_format = format_type
        elif subtype == 'parquet' or dest_type.currentText() == "Parquet":
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            dest_layout.addRow("Ruta del archivo:", file_path)
            self.dest_file_path = file_path
            select_path = QPushButton("Seleccionar ruta")
            select_path.clicked.connect(lambda: self.select_output_path('parquet'))
            dest_layout.addRow(select_path)
            format_type = QComboBox()
            format_type.addItems(["Parquet"])
            if 'format' in node_data:
                format_type.setCurrentText(node_data['format'])
            dest_layout.addRow("Formato:", format_type)
            self.dest_format = format_type
        # Campos de Base de Datos (si aplica)
        if subtype == 'database' or dest_type.currentText() == "Base de Datos":
            db_type = QComboBox()
            db_type.addItems(["MySQL", "PostgreSQL", "SQL Server", "SQLite"])
            if 'db_type' in node_data:
                db_type.setCurrentText(node_data['db_type'])
            dest_layout.addRow("Base de datos:", db_type)

            host = QLineEdit(); host.setText(node_data.get('host', ''))
            port = QLineEdit(); port.setText(node_data.get('port', ''))
            user = QLineEdit(); user.setText(node_data.get('user', ''))
            password = QLineEdit(); password.setText(node_data.get('password', ''))
            password.setEchoMode(QLineEdit.EchoMode.Password)
            database = QLineEdit(); database.setText(node_data.get('database', ''))
            table = QLineEdit(); table.setText(node_data.get('table', ''))
            if_exists = QComboBox(); if_exists.addItems(["replace", "append", "fail"])
            if 'if_exists' in node_data:
                if_exists.setCurrentText(node_data['if_exists'])

            dest_layout.addRow("Host:", host)
            dest_layout.addRow("Puerto:", port)
            dest_layout.addRow("Usuario:", user)
            dest_layout.addRow("Contraseña:", password)
            dest_layout.addRow("Base de datos:", database)
            dest_layout.addRow("Tabla:", table)
            dest_layout.addRow("Si existe:", if_exists)

            self.dest_db_fields = {
                'db_type': db_type,
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database,
                'table': table,
                'if_exists': if_exists,
            }
            # Auto-guardado para BD destino
            db_type.currentTextChanged.connect(lambda *_: self._schedule_autosave('destination', node_id))
            if_exists.currentTextChanged.connect(lambda *_: self._schedule_autosave('destination', node_id))
            for _fld in [host, port, user, password, database, table]:
                _fld.editingFinished.connect(lambda: self._schedule_autosave('destination', node_id))
            # Botón Probar conexión (destino)
            test_btn = QPushButton("Probar conexión")
            test_btn.clicked.connect(self._on_test_dest_db_connection)
            dest_layout.addRow(test_btn)

        # Campos de API (si aplica)
        if subtype == 'api' or dest_type.currentText() == "API":
            url = QLineEdit(); url.setText(node_data.get('url', ''))
            method = QComboBox(); method.addItems(["POST", "PUT", "PATCH", "DELETE"])
            if 'method' in node_data:
                method.setCurrentText(node_data['method'])
            headers = QLineEdit(); headers.setText(node_data.get('headers', ''))
            params = QLineEdit(); params.setText(node_data.get('params', ''))
            batch_size = QLineEdit(); batch_size.setText(str(node_data.get('batch_size', '500')))

            dest_layout.addRow("URL:", url)
            dest_layout.addRow("Método:", method)
            dest_layout.addRow("Headers (k:v,k:v):", headers)
            dest_layout.addRow("Params (k:v,k:v):", params)
            dest_layout.addRow("Tamaño de lote:", batch_size)

            self.dest_api_fields = {
                'url': url,
                'method': method,
                'headers': headers,
                'params': params,
                'batch_size': batch_size,
            }
            # Auto-guardado para API destino
            method.currentTextChanged.connect(lambda *_: self._schedule_autosave('destination', node_id))
            for _fld in [url, headers, params, batch_size]:
                _fld.editingFinished.connect(lambda: self._schedule_autosave('destination', node_id))

        dest_group.setLayout(dest_layout)
        self.layout.addWidget(dest_group)
        
    def _on_test_source_db_connection(self):
        try:
            fields = getattr(self, 'db_fields', None)
            if not fields:
                QMessageBox.warning(self, "Probar conexión", "No hay campos de base de datos disponibles en esta vista.")
                return
            self._test_db_connection(fields, "Fuente")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al probar conexión: {e}")
    
    def _on_test_dest_db_connection(self):
        try:
            fields = getattr(self, 'dest_db_fields', None)
            if not fields:
                QMessageBox.warning(self, "Probar conexión", "No hay campos de base de datos disponibles en esta vista.")
                return
            self._test_db_connection(fields, "Destino")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al probar conexión: {e}")
    
    def _on_preview_source_db(self, node_id):
        """Ejecuta la consulta del origen BD y muestra una vista previa (hasta 100 filas)."""
        try:
            fields = getattr(self, 'db_fields', None)
            if not fields:
                QMessageBox.warning(self, "Vista previa", "No hay campos de base de datos disponibles en esta vista.")
                return
            db_type = fields['db_type'].currentText().strip()
            host = fields['host'].text().strip()
            port = fields['port'].text().strip()
            user = fields['user'].text().strip()
            password = fields['password'].text().strip()
            database = fields['database'].text().strip()
            query = fields['query'].text().strip()
            if not query:
                QMessageBox.warning(self, "Vista previa", "Ingrese una consulta SQL para poder previsualizar.")
                return
            # Construir URL como en _test_db_connection
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
                connect_args = {'connect_timeout': 5}
            elif db_type == 'PostgreSQL':
                url = URL.create(
                    drivername='postgresql+psycopg2',
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else None,
                    database=database or None,
                )
                connect_args = {'connect_timeout': 5}
            elif db_type == 'SQL Server':
                driver_name = 'ODBC Driver 17 for SQL Server'
                url = URL.create(
                    drivername='mssql+pyodbc',
                    username=user or None,
                    password=password or None,
                    host=f"{host},{port}" if host and port else (host or None),
                    database=database or None,
                    query={'driver': driver_name}
                )
                connect_args = {'timeout': 5}
            elif db_type == 'SQLite':
                if not database:
                    QMessageBox.warning(self, "Vista previa", "Para SQLite especifique el archivo de base de datos en 'Base de datos'.")
                    return
                url = URL.create(drivername='sqlite', database=database)
            else:
                QMessageBox.warning(self, "Vista previa", f"Tipo de base de datos no soportado: {db_type}")
                return
            # Usar la consulta tal cual como vista previa (sin LIMIT/TOP)
            q = query.strip().rstrip(';')
            preview_sql = q
            # Ejecutar y mostrar
            import pandas as pd
            engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args or {})
            with engine.connect() as conn:
                pdf = pd.read_sql(text(preview_sql), conn)
            try:
                engine.dispose()
            except Exception:
                pass
            # Convertir a Polars
            df = pl.from_pandas(pdf) if hasattr(pdf, 'columns') else pl.DataFrame(pdf)
            # Guardar y refrescar UI
            self.node_configs.setdefault(node_id, {})
            self.node_configs[node_id]['dataframe'] = df
            self.current_dataframes[node_id] = df
            # Disparar cambio para propagar preview a nodos conectados
            self.node_config_changed.emit(node_id, self.node_configs[node_id])
            # Reconstruir panel para mostrar Vista previa + Columnas a pasar
            self.show_node_properties(node_id, 'source', self.node_configs[node_id])
            self.log_message("Vista previa de base de datos cargada (hasta 100 filas)")
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Vista previa", f"Error al obtener vista previa: {e}")
    def _test_db_connection(self, fields, titulo="Fuente"):
        """Prueba la conexión a la base de datos usando SQLAlchemy."""
        try:
            db_type = fields['db_type'].currentText().strip()
            host = fields['host'].text().strip() if 'host' in fields else ''
            port = fields['port'].text().strip() if 'port' in fields else ''
            user = fields['user'].text().strip() if 'user' in fields else ''
            password = fields['password'].text().strip() if 'password' in fields else ''
            database = fields['database'].text().strip() if 'database' in fields else ''
            # Construir URL de SQLAlchemy
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
                connect_args = {'connect_timeout': 5}
            elif db_type == 'PostgreSQL':
                url = URL.create(
                    drivername='postgresql+psycopg2',
                    username=user or None,
                    password=password or None,
                    host=host or None,
                    port=int(port) if port else None,
                    database=database or None,
                )
                connect_args = {'connect_timeout': 5}
            elif db_type == 'SQL Server':
                # Requiere ODBC Driver instalado en el sistema
                driver_name = 'ODBC Driver 17 for SQL Server'
                url = URL.create(
                    drivername='mssql+pyodbc',
                    username=user or None,
                    password=password or None,
                    host=f"{host},{port}" if host and port else (host or None),
                    database=database or None,
                    query={'driver': driver_name}
                )
                connect_args = {'timeout': 5}
            elif db_type == 'SQLite':
                # En SQLite, 'database' debe ser ruta a archivo .db
                if not database:
                    QMessageBox.warning(self, "Probar conexión", "Para SQLite, especifique la ruta del archivo en 'Base de datos'.")
                    return
                url = URL.create(drivername='sqlite', database=database)
            else:
                QMessageBox.warning(self, "Probar conexión", f"Tipo de base de datos no soportado: {db_type}")
                return
            if url is None:
                QMessageBox.warning(self, "Probar conexión", "No se pudo construir la URL de conexión.")
                return
            # Probar conexión
            engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args or {})
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            try:
                engine.dispose()
            except Exception:
                pass
            QMessageBox.information(self, f"Probar conexión ({titulo})", "Conexión exitosa.")
        except Exception as e:
            QMessageBox.critical(self, f"Probar conexión ({titulo})", f"Error de conexión: {e}")
        
    def update_source_type(self, node_id, new_type):
        """Actualiza el tipo de fuente y reconstruye el panel"""
        # Evitar reentradas si estamos reconstruyendo UI
        if getattr(self, '_ui_rebuilding', False):
            return
        self.current_source_type = new_type
        # Convertir el tipo UI a subtipo interno
        subtype_map = {"CSV": "csv", "Excel": "excel", "JSON": "json", "Parquet": "parquet", "Base de Datos": "database", "API": "api"}
        subtype = subtype_map.get(new_type, "csv")
        
        # Guardar el subtipo en la configuración del nodo
        self.node_configs[node_id]['subtype'] = subtype
        # Limpiar dataframe y ruta al cambiar tipo para evitar estados inconsistentes
        try:
            self.node_configs[node_id].pop('dataframe', None)
            self.node_configs[node_id].pop('other_dataframe', None)
            self.node_configs[node_id]['path'] = ''
        except Exception:
            pass
        if node_id in self.current_dataframes:
            try:
                del self.current_dataframes[node_id]
            except Exception:
                pass
        # Cancelar autosaves pendientes para este nodo
        try:
            if self._autosave_timer.isActive():
                self._autosave_timer.stop()
            self._pending_autosave = None
        except Exception:
            pass
        # Reconstruir el panel de propiedades de forma diferida para evitar reentradas
        QTimer.singleShot(0, lambda: self.show_node_properties(node_id, 'source', self.node_configs[node_id]))
        # Notificar cambio para actualizar visual del nodo (título y puertos)
        QTimer.singleShot(0, lambda: self.node_config_changed.emit(node_id, self.node_configs[node_id]))
        
    def update_transform_type(self, node_id, new_type):
        """Actualiza el tipo de transformación y reconstruye el panel"""
        if getattr(self, '_ui_rebuilding', False):
            return
        self.current_transform_type = new_type
        # Convertir el tipo UI a subtipo interno
        subtype_map = {"Filtro": "filter", "Unión": "join", "Agregación": "aggregate", "Mapeo": "map"}
        subtype = subtype_map.get(new_type, "filter")
        
        # Guardar el subtipo en la configuración del nodo
        self.node_configs[node_id]['subtype'] = subtype
        # Reconstruir el panel de propiedades de forma diferida
        QTimer.singleShot(0, lambda: self.show_node_properties(node_id, 'transform', self.node_configs[node_id]))
        QTimer.singleShot(0, lambda: self.node_config_changed.emit(node_id, self.node_configs[node_id]))
        
    def update_destination_type(self, node_id, new_type):
        """Actualiza el tipo de destino y reconstruye el panel"""
        if getattr(self, '_ui_rebuilding', False):
            return
        self.current_dest_type = new_type
        subtype_map = {"CSV": "csv", "Excel": "excel", "JSON": "json", "Parquet": "parquet", "Base de Datos": "database", "API": "api"}
        subtype = subtype_map.get(new_type, "csv")
        self.node_configs[node_id]['subtype'] = subtype
        QTimer.singleShot(0, lambda: self.show_node_properties(node_id, 'destination', self.node_configs[node_id]))
        QTimer.singleShot(0, lambda: self.node_config_changed.emit(node_id, self.node_configs[node_id]))
        
    def clear_node_config(self, node_id):
        """Limpia la configuración de un nodo"""
        # Mantener solo el subtipo
        subtype = self.node_configs[node_id].get('subtype')
        self.node_configs[node_id] = {'subtype': subtype}
        
        # Si hay un dataframe asociado, eliminarlo
        if node_id in self.current_dataframes:
            del self.current_dataframes[node_id]
            
        # Reconstruir el panel según el tipo de nodo
        if self.current_node_type == 'source':
            self.show_node_properties(node_id, 'source', self.node_configs[node_id])
        elif self.current_node_type == 'transform':
            self.show_node_properties(node_id, 'transform', self.node_configs[node_id])
        elif self.current_node_type == 'destination':
            self.show_node_properties(node_id, 'destination', self.node_configs[node_id])
            
        QMessageBox.information(self, "Nodo reiniciado", "La configuración del nodo ha sido reiniciada")
        
    def save_node_config(self, node_id, source_type):
        """Guarda la configuración de un nodo de origen"""
        # Evitar autosave durante reconstrucción de UI
        if getattr(self, '_ui_rebuilding', False):
            return
        config = self.node_configs.get(node_id, {})
        # Snapshot previo para evitar emisiones redundantes
        prev_fp = (
            str(config.get('subtype', '')),
            str(config.get('path', '')),
            str(config.get('output_cols', '')),
            str(config.get('column_rename', '')),
            str(config.get('db_type', '')),
            str(config.get('host', '')),
            str(config.get('port', '')),
            str(config.get('user', '')),
            str(config.get('database', '')),
            str(config.get('query', '')),
            str(config.get('url', '')),
            str(config.get('method', '')),
            str(config.get('headers', '')),
            str(config.get('params', '')),
        )
        
        # Guardar configuración según el tipo
        if source_type in ("CSV", "Excel", "JSON", "Parquet"):
            if hasattr(self, 'file_path_field'):
                try:
                    config['path'] = self.file_path_field.text()
                except RuntimeError:
                    # El widget pudo haber sido destruido si se reconstruyó el panel
                    pass
            
            # Guardar mapeo de columnas si existe
            rows = 0
            if hasattr(self, 'column_selection_table') and self.column_selection_table is not None:
                try:
                    rows = self.column_selection_table.rowCount()
                except RuntimeError:
                    rows = 0
            if rows > 0:
                selected_columns = []
                rename_mappings = []
                for i in range(rows):
                    try:
                        checkbox = self.column_selection_table.item(i, 0)
                        col_item = self.column_selection_table.item(i, 1)
                        col_name = col_item.text() if col_item else None
                        rename_field = self.column_selection_table.cellWidget(i, 2)
                        if checkbox and checkbox.checkState() == Qt.CheckState.Checked and col_name:
                            selected_columns.append(col_name)
                            if rename_field and rename_field.text().strip():
                                rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
                    except RuntimeError:
                        continue
                config['output_cols'] = ','.join(selected_columns)
                config['column_rename'] = ','.join(rename_mappings)
                
        elif source_type == "Base de Datos":
            if hasattr(self, 'db_fields'):
                for key, field in self.db_fields.items():
                    if isinstance(field, QComboBox):
                        config[key] = field.currentText()
                    else:
                        config[key] = field.text()
                        
        elif source_type == "API":
            if hasattr(self, 'api_fields'):
                for key, field in self.api_fields.items():
                    if isinstance(field, QComboBox):
                        config[key] = field.currentText()
                    else:
                        config[key] = field.text()
        
        # Validaciones básicas
        try:
            if source_type in ("CSV", "Excel", "JSON", "Parquet"):
                if not config.get('path'):
                    QMessageBox.warning(self, "Falta ruta", "Debe seleccionar la ruta del archivo de origen.")
                    return
            elif source_type == "Base de Datos":
                db_type = (config.get('db_type') or '').strip()
                if not db_type:
                    QMessageBox.warning(self, "Datos incompletos", "Seleccione el tipo de base de datos.")
                    return
                if db_type == 'SQLite':
                    if not (config.get('database')):
                        QMessageBox.warning(self, "Datos incompletos", "Para SQLite especifique el archivo de base de datos.")
                        return
                else:
                    required = ['host', 'port', 'user', 'password', 'database', 'query']
                    missing = [k for k in required if not (config.get(k) and str(config.get(k)).strip())]
                    if missing:
                        QMessageBox.warning(self, "Datos incompletos", f"Faltan campos: {', '.join(missing)}")
                        return
            elif source_type == "API":
                if not (config.get('url') and str(config.get('url')).strip()):
                    QMessageBox.warning(self, "Datos incompletos", "Debe especificar la URL del API.")
                    return
        except Exception as e:
            self.log_message(f"Error en validación de origen: {e}")
            QMessageBox.critical(self, "Error", f"Error validando datos del origen: {e}")
            return

        # Guardar configuración
        self.node_configs[node_id] = config
        # Fingerprint nuevo para detectar cambios reales
        new_fp = (
            str(config.get('subtype', '')),
            str(config.get('path', '')),
            str(config.get('output_cols', '')),
            str(config.get('column_rename', '')),
            str(config.get('db_type', '')),
            str(config.get('host', '')),
            str(config.get('port', '')),
            str(config.get('user', '')),
            str(config.get('database', '')),
            str(config.get('query', '')),
            str(config.get('url', '')),
            str(config.get('method', '')),
            str(config.get('headers', '')),
            str(config.get('params', '')),
        )
        if new_fp == prev_fp:
            # No hay cambios efectivos; no emitir
            return
        # Emitir señal de cambio de configuración solo si hubo cambios
        self.node_config_changed.emit(node_id, config)
        
        # Auto-guardado: evitar popups ruidosos
        self.log_message("Configuración de origen guardada (auto)")
        
    def save_transform_config(self, node_id, transform_type):
        """Guarda la configuración de un nodo de transformación"""
        # Evitar autosave durante reconstrucción de UI
        if getattr(self, '_ui_rebuilding', False):
            return
        config = self.node_configs.get(node_id, {})
        
        # Mantener los dataframes existentes
        if 'dataframe' in config:
            config['dataframe'] = config['dataframe']
        if 'other_dataframe' in config:
            config['other_dataframe'] = config['other_dataframe']
            
        # Guardar el subtipo según el tipo de transformación
        if transform_type == "Filtro":
            config['subtype'] = 'filter'
            if hasattr(self, 'filter_expr_field'):
                config['filter_expr'] = self.filter_expr_field.text()
                
        elif transform_type == "Unión":
            config['subtype'] = 'join'
            if hasattr(self, 'join_fields'):
                # Guardar tipo de unión y columnas de unión
                config['join_type'] = self.join_fields['join_type'].currentText()
                config['join_cols'] = self.join_fields['join_cols'].text()
                # Guardar pares y sufijo derecha
                if 'join_pairs' in self.join_fields:
                    config['join_pairs'] = self.join_fields['join_pairs'].text()
                if 'right_suffix' in self.join_fields:
                    config['right_suffix'] = self.join_fields['right_suffix'].text() or '_right'
                
                # Guardar selección de columnas de salida
                if hasattr(self, 'column_checkboxes') and hasattr(self, 'column_rename_fields'):
                    selected_columns = []
                    rename_mappings = []
                    
                    column_table = self.join_fields['column_table']
                    
                    # Validar el número de filas en la tabla
                    if column_table.rowCount() > 0:
                        for i in range(len(self.column_checkboxes)):
                            if i >= len(self.column_checkboxes) or i >= column_table.rowCount():
                                continue
                                
                            checkbox = self.column_checkboxes[i]
                            if checkbox and checkbox.checkState() == Qt.CheckState.Checked:
                                column_item = column_table.item(i, 1)
                                if column_item:
                                    column_name = column_item.text()
                                    selected_columns.append(column_name)
                                    
                                    # Verificar si hay un renombrado para esta columna
                                    if i < len(self.column_rename_fields):
                                        rename_field = self.column_rename_fields[i]
                                        if rename_field and rename_field.text().strip():
                                            rename_mappings.append(f"{column_name}:{rename_field.text().strip()}")
                        
                        # Guardar selecciones
                        config['output_cols'] = ','.join(selected_columns)
                        config['column_rename'] = ','.join(rename_mappings)
                    else:
                        self.log_message("No hay columnas disponibles para seleccionar")
                
        elif transform_type == "Agregación":
            config['subtype'] = 'aggregate'
            if hasattr(self, 'agg_fields'):
                config['group_by'] = self.agg_fields['group_by'].text()
                config['agg_funcs'] = self.agg_fields['agg_funcs'].text()
                
        elif transform_type == "Mapeo":
            config['subtype'] = 'map'
            if hasattr(self, 'map_expr_field'):
                config['map_expr'] = self.map_expr_field.text()
        elif transform_type == "Casteo":
            config['subtype'] = 'cast'
            # Guardar estructuras de casteo
            try:
                ops = []
                if hasattr(self, 'cast_ops_table') and self.cast_ops_table is not None:
                    ct = self.cast_ops_table
                    for row in range(ct.rowCount()):
                        col_cb = ct.cellWidget(row, 0)
                        to_cb = ct.cellWidget(row, 1)
                        fmt_le = ct.cellWidget(row, 2)
                        col = col_cb.currentText() if isinstance(col_cb, QComboBox) else ''
                        to = to_cb.currentText() if isinstance(to_cb, QComboBox) else ''
                        fmt = fmt_le.text().strip() if isinstance(fmt_le, QLineEdit) else ''
                        if col and to:
                            ops.append({'col': col, 'to': to, 'fmt': fmt})
                config['cast_ops'] = ops
            except Exception:
                pass
        
        # Guardar selección y renombrado de columnas para tablas genéricas (no para 'Unión')
        if config.get('subtype') != 'join':
            rows = 0
            if hasattr(self, 'column_selection_table') and self.column_selection_table is not None:
                try:
                    rows = self.column_selection_table.rowCount()
                except RuntimeError:
                    rows = 0
            if rows > 0:
                selected_columns = []
                rename_mappings = []
                for i in range(rows):
                    try:
                        checkbox = self.column_selection_table.item(i, 0)
                        col_item = self.column_selection_table.item(i, 1)
                        col_name = col_item.text() if col_item else None
                        rename_field = self.column_selection_table.cellWidget(i, 2)
                        if checkbox and checkbox.checkState() == Qt.CheckState.Checked and col_name:
                            selected_columns.append(col_name)
                            if rename_field and rename_field.text().strip():
                                rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
                    except RuntimeError:
                        continue
                config['output_cols'] = ','.join(selected_columns)
                config['column_rename'] = ','.join(rename_mappings)
        
        # Guardar estructuras de Filtro
        if config.get('subtype') == 'filter':
            try:
                # Modo
                if hasattr(self, 'filter_mode_combo') and self.filter_mode_combo is not None:
                    mode_txt = self.filter_mode_combo.currentText()
                    config['filter_mode'] = 'any' if 'Cualquiera' in mode_txt else 'all'
                # Reglas
                rules = []
                if hasattr(self, 'filter_rules_table') and self.filter_rules_table is not None:
                    rt = self.filter_rules_table
                    for row in range(rt.rowCount()):
                        col_cb = rt.cellWidget(row, 0)
                        op_cb = rt.cellWidget(row, 1)
                        val_le = rt.cellWidget(row, 2)
                        col = col_cb.currentText() if isinstance(col_cb, QComboBox) else None
                        op = op_cb.currentText() if isinstance(op_cb, QComboBox) else None
                        val_raw = val_le.text().strip() if isinstance(val_le, QLineEdit) else ''
                        if not col or not op:
                            continue
                        val: object = None
                        if op in ('isnull', 'notnull'):
                            val = None
                        elif op == 'in':
                            val = [v.strip() for v in val_raw.split(',') if v.strip()]
                        else:
                            # intentar numérico
                            try:
                                val = float(val_raw)
                            except Exception:
                                val = val_raw
                        rules.append({'column': col, 'op': op, 'value': val})
                config['filter_rules'] = rules
            except Exception:
                pass

        # Guardar estructuras de Agregación
        if config.get('subtype') == 'aggregate':
            try:
                # Group By
                group_by_list = []
                if hasattr(self, 'agg_group_by_table') and self.agg_group_by_table is not None:
                    gbt = self.agg_group_by_table
                    for row in range(gbt.rowCount()):
                        chk = gbt.item(row, 0)
                        name_item = gbt.item(row, 1)
                        if chk and name_item and chk.checkState() == Qt.CheckState.Checked:
                            group_by_list.append(name_item.text())
                config['group_by_list'] = group_by_list

                # Funciones
                aggs = []
                if hasattr(self, 'agg_ops_table') and self.agg_ops_table is not None:
                    at = self.agg_ops_table
                    for row in range(at.rowCount()):
                        col_cb = at.cellWidget(row, 0)
                        fn_cb = at.cellWidget(row, 1)
                        alias_le = at.cellWidget(row, 2)
                        col = col_cb.currentText() if isinstance(col_cb, QComboBox) else ''
                        func = fn_cb.currentText() if isinstance(fn_cb, QComboBox) else ''
                        alias = alias_le.text().strip() if isinstance(alias_le, QLineEdit) else ''
                        if col and func:
                            aggs.append({'col': col, 'func': func, 'as': alias})
                config['aggs'] = aggs
            except Exception:
                pass

        # Guardar estructuras de Mapeo
        if config.get('subtype') == 'map':
            try:
                ops = []
                if hasattr(self, 'map_ops_table') and self.map_ops_table is not None:
                    mt = self.map_ops_table
                    for row in range(mt.rowCount()):
                        new_le = mt.cellWidget(row, 0)
                        op_cb = mt.cellWidget(row, 1)
                        a_cb = mt.cellWidget(row, 2)
                        b_cb = mt.cellWidget(row, 3)
                        val_le = mt.cellWidget(row, 4)
                        new_col = new_le.text().strip() if isinstance(new_le, QLineEdit) else ''
                        op_type = op_cb.currentText() if isinstance(op_cb, QComboBox) else ''
                        a = a_cb.currentText() if isinstance(a_cb, QComboBox) else ''
                        b = b_cb.currentText() if isinstance(b_cb, QComboBox) else ''
                        val_text = val_le.text().strip() if isinstance(val_le, QLineEdit) else ''
                        value = None
                        if op_type == 'literal':
                            # Mantener como texto literal
                            value = val_text
                        else:
                            value = val_text if val_text != '' else None
                        if new_col and op_type:
                            ops.append({'new_col': new_col, 'op_type': op_type, 'a': a or None, 'b': b or None, 'value': value})
                config['map_ops'] = ops
            except Exception:
                pass

        # Guardar estructuras de Casteo
        if config.get('subtype') == 'cast':
            try:
                ops = []
                if hasattr(self, 'cast_ops_table') and self.cast_ops_table is not None:
                    ct = self.cast_ops_table
                    for row in range(ct.rowCount()):
                        col_cb = ct.cellWidget(row, 0)
                        to_cb = ct.cellWidget(row, 1)
                        fmt_le = ct.cellWidget(row, 2)
                        col = col_cb.currentText() if isinstance(col_cb, QComboBox) else ''
                        to = to_cb.currentText() if isinstance(to_cb, QComboBox) else ''
                        fmt = fmt_le.text().strip() if isinstance(fmt_le, QLineEdit) else ''
                        if col and to:
                            ops.append({'col': col, 'to': to, 'fmt': fmt})
                config['cast_ops'] = ops
            except Exception:
                pass

        # Actualizar la configuración del nodo
        self.node_configs[node_id] = config
        
        # Emitir señal de cambio de configuración
        self.node_config_changed.emit(node_id, config)
        
        # Mostrar mensaje de confirmación
        self.log_message(f"Configuración guardada para el nodo {node_id}")
        return True
        
    def save_destination_config(self, node_id, dest_type):
        """Guarda la configuración de un nodo de destino"""
        # Evitar autosave durante reconstrucción de UI
        if getattr(self, '_ui_rebuilding', False):
            return
        config = self.node_configs.get(node_id, {})
        
        # Guardar configuración según el tipo
        if dest_type in ("CSV", "Excel", "JSON", "Parquet"):
            if hasattr(self, 'dest_file_path'):
                try:
                    config['path'] = self.dest_file_path.text()
                except RuntimeError:
                    # El widget pudo haber sido destruido; mantener valor previo si existe
                    pass
            if hasattr(self, 'dest_format'):
                try:
                    config['format'] = self.dest_format.currentText()
                except RuntimeError:
                    pass

        elif dest_type == "Base de Datos":
            if hasattr(self, 'dest_db_fields'):
                for key, field in self.dest_db_fields.items():
                    try:
                        if isinstance(field, QComboBox):
                            config[key] = field.currentText()
                        else:
                            config[key] = field.text()
                    except RuntimeError:
                        # Campo ya destruido; ignorar
                        pass
                        
        elif dest_type == "API":
            if hasattr(self, 'dest_api_fields'):
                for key, field in self.dest_api_fields.items():
                    try:
                        if isinstance(field, QComboBox):
                            config[key] = field.currentText()
                        else:
                            config[key] = field.text()
                    except RuntimeError:
                        pass
        
        # Guardar selección y renombrado de columnas
        rows = 0
        if hasattr(self, 'column_selection_table') and self.column_selection_table is not None:
            try:
                rows = self.column_selection_table.rowCount()
            except RuntimeError:
                rows = 0
        if rows > 0:
            selected_columns = []
            rename_mappings = []
            for i in range(rows):
                try:
                    checkbox = self.column_selection_table.item(i, 0)
                    col_item = self.column_selection_table.item(i, 1)
                    col_name = col_item.text() if col_item else None
                    rename_field = self.column_selection_table.cellWidget(i, 2)
                    if checkbox and checkbox.checkState() == Qt.CheckState.Checked and col_name:
                        selected_columns.append(col_name)
                        if rename_field and rename_field.text().strip():
                            rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
                except RuntimeError:
                    continue
            config['output_cols'] = ','.join(selected_columns)
            config['column_rename'] = ','.join(rename_mappings)

        # Validaciones básicas
        try:
            if dest_type in ("CSV", "Excel", "JSON", "Parquet"):
                if not (config.get('path') and str(config.get('path')).strip()):
                    QMessageBox.warning(self, "Falta ruta", "Debe seleccionar la ruta del archivo de destino.")
                    return
            elif dest_type == "Base de Datos":
                db_type = (config.get('db_type') or '').strip()
                if not db_type:
                    QMessageBox.warning(self, "Datos incompletos", "Seleccione el tipo de base de datos.")
                    return
                if db_type == 'SQLite':
                    if not (config.get('database')):
                        QMessageBox.warning(self, "Datos incompletos", "Para SQLite especifique el archivo de base de datos.")
                        return
                else:
                    required = ['host', 'port', 'user', 'password', 'database', 'table']
                    missing = [k for k in required if not (config.get(k) and str(config.get(k)).strip())]
                    if missing:
                        QMessageBox.warning(self, "Datos incompletos", f"Faltan campos: {', '.join(missing)}")
                        return
            elif dest_type == "API":
                if not (config.get('url') and str(config.get('url')).strip()):
                    QMessageBox.warning(self, "Datos incompletos", "Debe especificar la URL del API.")
                    return
                # Validar batch_size numérico
                try:
                    if 'batch_size' in config and str(config['batch_size']).strip():
                        config['batch_size'] = int(str(config['batch_size']).strip())
                except Exception:
                    QMessageBox.warning(self, "Valor inválido", "El tamaño de lote debe ser un número entero.")
                    return
        except Exception as e:
            self.log_message(f"Error en validación de destino: {e}")
            QMessageBox.critical(self, "Error", f"Error validando datos del destino: {e}")
            return
        
        # Guardar configuración
        self.node_configs[node_id] = config
        
        # Emitir señal de cambio de configuración
        self.node_config_changed.emit(node_id, config)
        
        QMessageBox.information(self, "Configuración guardada", "La configuración del nodo ha sido guardada")
        
    def select_output_path(self, file_type=None):
        # Determinar filtro y extensión por tipo
        if file_type == 'excel':
            format_filter = "Excel (*.xlsx)"
            extension = ".xlsx"
        elif file_type == 'json':
            format_filter = "JSON (*.json)"
            extension = ".json"
        elif file_type == 'parquet':
            format_filter = "Parquet (*.parquet)"
            extension = ".parquet"
        else:
            format_filter = "CSV (*.csv)"
            extension = ".csv"
        default_dir = os.path.expanduser("~")
        directory = QFileDialog.getExistingDirectory(
            self,
            "Seleccionar carpeta para guardar archivo",
            default_dir
        )
        if directory:
            file_name, _ = QFileDialog.getSaveFileName(
                self,
                "Guardar archivo",
                directory + "/output" + extension,
                "Excel (*.xlsx);;CSV (*.csv);;JSON (*.json);;Parquet (*.parquet)",
                format_filter
            )
            if file_name:
                valid_exts = ['.csv', '.xlsx', '.json', '.parquet']
                if not any(file_name.endswith(ext) for ext in valid_exts):
                    file_name += extension
                if hasattr(self, 'dest_file_path'):
                    self.dest_file_path.setText(file_name)
                if hasattr(self, 'current_node_id') and self.current_node_id in self.node_configs:
                    self.node_configs[self.current_node_id]['path'] = file_name
                    self.node_config_changed.emit(self.current_node_id, self.node_configs[self.current_node_id])
                self.log_message(f"Ruta de destino seleccionada: {file_name}")
        
    def log_message(self, message):
        """Muestra un mensaje en la consola para depuración"""
        print(f"[PropertiesPanel] {message}")
        
    def load_file(self, node_id, file_type=None):
        if file_type == 'excel':
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo Excel",
                "",
                "Excel (*.xlsx)"
            )
        elif file_type == 'csv':
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo CSV",
                "",
                "CSV (*.csv)"
            )
        elif file_type == 'json':
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo JSON",
                "",
                "JSON (*.json)"
            )
        elif file_type == 'parquet':
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo Parquet",
                "",
                "Parquet (*.parquet)"
            )
        else:
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo",
                "",
                "Todos los archivos (*.*);;CSV (*.csv);;Excel (*.xlsx);;JSON (*.json);;Parquet (*.parquet)"
            )
        
        if file_name:
            try:
                if file_type == 'excel' or file_name.endswith('.xlsx'):
                    try:
                        df = pl.read_excel(file_name)
                    except Exception:
                        pdf = pd.read_excel(file_name)
                        df = pl.from_pandas(pdf)
                elif file_type == 'csv' or file_name.endswith('.csv'):
                    try:
                        df = pl.read_csv(file_name)
                    except:
                        try:
                            df = pl.read_csv(file_name, encoding='latin-1')
                        except:
                            df = pd.read_csv(file_name, encoding='latin-1')
                            df = pl.from_pandas(df)
                elif file_type == 'parquet' or file_name.endswith('.parquet'):
                    df = pl.read_parquet(file_name)
                elif file_name.endswith('.json'):
                    df = pl.read_json(file_name)
                else:
                    try:
                        df = pl.read_csv(file_name)
                    except:
                        try:
                            df = pl.read_csv(file_name, encoding='latin-1')
                        except:
                            df = pd.read_csv(file_name, encoding='latin-1')
                            df = pl.from_pandas(df)
                # Guardar la ruta del archivo y dataframe en los datos del nodo
                self.node_configs[node_id]['path'] = file_name
                self.node_configs[node_id]['dataframe'] = df
                self.current_dataframes[node_id] = df
                self.node_config_changed.emit(node_id, self.node_configs[node_id])
                # En vez de show_data_preview, refresco el panel completo:
                self.show_node_properties(node_id, self.current_node_type, self.node_configs[node_id])
                print(f"Archivo cargado correctamente: {file_name}")
                QMessageBox.information(self, "Archivo cargado", f"Archivo cargado correctamente: {file_name}")
            except Exception as e:
                print(f"Error al cargar el archivo: {e}")
                import traceback
                traceback.print_exc()
                QMessageBox.critical(self, "Error", f"Error al cargar el archivo: {e}")
        
    def show_existing_data(self, node_id):
        """Muestra datos ya cargados para un nodo"""
        if node_id in self.node_configs and 'dataframe' in self.node_configs[node_id]:
            df = self.node_configs[node_id]['dataframe']
            self.show_data_preview(df)
            self.update_column_mapping(df.columns)
                
    def show_data_preview(self, df):
        """Muestra una vista previa de los datos en la tabla"""
        self.setup_data_preview(self.data_table, df)
                
    def setup_data_preview(self, table_widget, df):
        """Configura una tabla con los datos del dataframe"""
        # Configurar la tabla
        table_widget.setRowCount(min(10, len(df)))
        table_widget.setColumnCount(len(df.columns))
        table_widget.setHorizontalHeaderLabels(df.columns)
        
        # Llenar la tabla con datos
        for i in range(min(10, len(df))):
            for j, col in enumerate(df.columns):
                try:
                    item = QTableWidgetItem(str(df[col][i]))
                    table_widget.setItem(i, j, item)
                except:
                    item = QTableWidgetItem("Error")
                    table_widget.setItem(i, j, item)
                
    def update_column_mapping(self, columns):
        """Actualiza la tabla de mapeo de columnas"""
        # Obtener mapeo existente
        existing_map = {}
        if self.current_node_id in self.node_configs and 'column_mapping' in self.node_configs[self.current_node_id]:
            existing_map = self.node_configs[self.current_node_id]['column_mapping']
            
        self.column_selection_table.setRowCount(len(columns))
        for i, col in enumerate(columns):
            # Columna original
            item = QTableWidgetItem(col)
            self.column_selection_table.setItem(i, 0, item)
            
            # Nuevo nombre (usar mapeo existente si hay)
            mapped_name = existing_map.get(col, col)
            item = QTableWidgetItem(mapped_name)
            self.column_selection_table.setItem(i, 1, item)
            
    def get_node_dataframe(self, node_id):
        """Obtiene el dataframe asociado a un nodo"""
        if node_id in self.current_dataframes:
            return self.current_dataframes[node_id]
        if node_id in self.node_configs and 'dataframe' in self.node_configs[node_id]:
            return self.node_configs[node_id]['dataframe']
        return None
        
    def set_node_dataframe(self, node_id, df):
        """Establece el dataframe para un nodo y actualiza la UI si es el nodo actual"""
        self.current_dataframes[node_id] = df
        if node_id in self.node_configs:
            self.node_configs[node_id]['dataframe'] = df
            
        # Si es el nodo actual, actualizar la UI
        if hasattr(self, 'current_node_id') and self.current_node_id == node_id:
            # Volver a mostrar las propiedades del nodo
            self.show_node_properties(node_id, self.current_node_type, self.node_configs[node_id])
            
    def get_node_config(self, node_id):
        """Obtiene la configuración de un nodo"""
        return self.node_configs.get(node_id, {}) 

    def select_all_columns(self, select_all=True):
        """Selecciona o deselecciona todas las columnas en la tabla de selección"""
        if getattr(self, '_ui_rebuilding', False):
            return
        if hasattr(self, 'column_checkboxes'):
            for checkbox in self.column_checkboxes:
                if checkbox:  # Verificar que el checkbox existe
                    checkbox.setCheckState(Qt.CheckState.Checked if select_all else Qt.CheckState.Unchecked)
            
            # Guardar automáticamente la selección actual si hay un nodo activo
            if hasattr(self, 'current_node_id') and self.current_node_id is not None:
                current_node_type = getattr(self, 'current_transform_type', None)
                if current_node_type == "Unión":
                    self.save_transform_config(self.current_node_id, "Unión")
                
    def fetch_data_from_connected_nodes(self, node_id):
        """Solicita datos de nodos conectados"""
        # Mostrar un mensaje de procesamiento más informativo
        msg_box = QMessageBox()
        msg_box.setWindowTitle("Obteniendo datos")
        msg_box.setText("Obteniendo datos de los nodos conectados...\nEsto puede tomar unos segundos.")
        msg_box.setStandardButtons(QMessageBox.StandardButton.NoButton)
        msg_box.setIcon(QMessageBox.Icon.Information)
        
        # Mostrar el mensaje sin bloquear y configurar un temporizador para cerrarlo
        msg_box.show()
        
        # Emitir la señal para obtener los datos
        self.fetch_connected_data.emit(node_id)
        
        # Cerrar el mensaje después de un breve retraso
        QTimer.singleShot(500, msg_box.close)
        
    def update_with_fetched_data(self, node_id, config):
        """Actualiza el panel de propiedades con los datos obtenidos"""
        if self.current_node_id == node_id:
            # Antes de volver a mostrar las propiedades, guardar el tipo y subtipo
            node_type = self.current_node_type
            subtype = config.get('subtype')
            
            # Guardar la configuración actualizada en el nodo
            self.node_configs[node_id] = config
            
            # Reconstruir completamente el panel de propiedades para el nodo
            self.show_node_properties(node_id, node_type, config)
            
            # Llenar la tabla de selección de columnas si es un nodo de unión
            if node_type == 'transform' and subtype == 'join':
                # Asegurarse de que las columnas se muestren en la tabla de selección
                all_columns = []
                if 'dataframe' in config:
                    df1 = config['dataframe']
                    all_columns.extend([f"Origen1.{col}" for col in df1.columns])
                if 'other_dataframe' in config:
                    df2 = config['other_dataframe']
                    all_columns.extend([f"Origen2.{col}" for col in df2.columns])

                # Parsear selección/renombrado existente
                selected_cols = []
                renamed_cols = {}
                if 'output_cols' in config and config['output_cols']:
                    selected_cols = [c.strip().split('.', 1)[-1] for c in config['output_cols'].split(',') if c.strip()]
                if 'column_rename' in config and config['column_rename']:
                    for pair in config['column_rename'].split(','):
                        if ':' in pair:
                            old_name, new_name = pair.split(':', 1)
                            renamed_cols[old_name.strip().split('.', 1)[-1]] = new_name.strip()

                # Verificar si tenemos una tabla de columnas configurada
                if hasattr(self, 'join_fields') and 'column_table' in self.join_fields and len(all_columns) > 0:
                    column_table = self.join_fields['column_table']
                    # Bloquear señales para evitar reentradas durante el llenado
                    try:
                        column_table.blockSignals(True)
                    except Exception:
                        pass
                    column_table.setRowCount(len(all_columns))

                    # Reiniciar listas internas para evitar IndexError
                    self.column_checkboxes = []
                    self.column_rename_fields = []

                    for i, col_name in enumerate(all_columns):
                        base_col = col_name.split('.', 1)[-1]
                        # Columna 1: Checkbox
                        checkbox = QTableWidgetItem()
                        is_selected = True if not selected_full else (col_name in selected_full)
                        checkbox.setCheckState(Qt.CheckState.Checked if is_selected else Qt.CheckState.Unchecked)
                        column_table.setItem(i, 0, checkbox)
                        self.column_checkboxes.append(checkbox)

                        # Columna 2: Nombre
                        column_name_item = QTableWidgetItem(col_name)
                        column_table.setItem(i, 1, column_name_item)

                        # Columna 3: Renombrar
                        rename_field = QLineEdit()
                        if col_name in renamed_full:
                            rename_field.setText(renamed_full[col_name])
                        else:
                            rename_field.setPlaceholderText(base_col)
                        column_table.setCellWidget(i, 2, rename_field)
                        self.column_rename_fields.append(rename_field)
                        # Auto-guardado con edición finalizada
                        try:
                            rename_field.editingFinished.connect(lambda: self.save_transform_config(node_id, "Unión"))
                        except Exception:
                            pass

                    column_table.resizeColumnsToContents()
                    # Desbloquear señales al final
                    try:
                        column_table.blockSignals(False)
                    except Exception:
                        pass
            
            # Mostrar mensaje informativo más específico
            if node_type == 'transform' and subtype == 'join':
                if 'dataframe' in config and 'other_dataframe' in config:
                    df1 = config['dataframe']
                    df2 = config['other_dataframe']
                    msg = f"Datos actualizados con éxito:\n"
                    msg += f"Origen 1: {len(df1)} filas, {len(df1.columns)} columnas\n"
                    msg += f"Origen 2: {len(df2)} filas, {len(df2.columns)} columnas"
                    QMessageBox.information(self, "Datos obtenidos", msg)
                elif 'dataframe' in config:
                    df = config['dataframe']
                    QMessageBox.information(self, "Datos obtenidos", f"Datos de origen principal actualizados: {len(df)} filas, {len(df.columns)} columnas")
                else:
                    QMessageBox.information(self, "Datos actualizados", "Se recibieron datos, pero no se detectó ningún DataFrame")
            else:
                if 'dataframe' in config:
                    df = config['dataframe']
                    QMessageBox.information(self, "Datos obtenidos", f"Datos actualizados: {len(df)} filas, {len(df.columns)} columnas")
                else:
                    QMessageBox.information(self, "Datos actualizados", "Se recibieron datos, pero no se detectó ningún DataFrame") 

    def fetch_data_for_destination(self, node_id):
        """Obtiene los datos que llegarían al nodo destino desde sus nodos conectados"""
        # Mostrar un mensaje de procesamiento
        msg_box = QMessageBox()
        msg_box.setWindowTitle("Obteniendo datos")
        msg_box.setText("Obteniendo datos de entrada para previsualización...\nEsto puede tomar unos segundos.")
        msg_box.setStandardButtons(QMessageBox.StandardButton.NoButton)
        msg_box.setIcon(QMessageBox.Icon.Information)
        msg_box.show()
        
        try:
            # Emitir la señal para obtener los datos
            self.fetch_connected_data.emit(node_id)
            
            # Verificar si se han recibido datos después de un tiempo
            QTimer.singleShot(500, msg_box.close)
            
            # Verificar si hay datos disponibles
            if node_id in self.node_configs and 'dataframe' in self.node_configs[node_id]:
                df = self.node_configs[node_id]['dataframe']
                
                # Siempre crear una nueva tabla de previsualización
                data_preview = QTableWidget()
                self.setup_data_preview(data_preview, df)
                # Buscar el layout del grupo de propiedades de destino
                for i in range(self.layout.count()):
                    item = self.layout.itemAt(i)
                    if isinstance(item.widget(), QGroupBox) and item.widget().title() == "Propiedades de Destino":
                        dest_group = item.widget()
                        dest_layout = dest_group.layout()
                        dest_layout.addRow("Vista previa:", data_preview)
                        break
                
                # Preparar información detallada sobre los datos
                info_text = f"Datos de entrada obtenidos con éxito:\n\n"
                info_text += f"• Número de filas: {len(df)}\n"
                info_text += f"• Número de columnas: {len(df.columns)}\n\n"
                info_text += "Columnas disponibles:\n"
                for i, col in enumerate(df.columns):
                    if i < 15:
                        info_text += f"- {col}\n"
                    elif i == 15:
                        info_text += f"- ... y {len(df.columns) - 15} columnas más\n"
                        break
                if len(df) > 0:
                    info_text += "\nPrimera fila de datos:\n"
                    try:
                        for i, col in enumerate(df.columns):
                            if i < 5:
                                info_text += f"- {col}: {df[col][0]}\n"
                            elif i == 5:
                                info_text += "- ...\n"
                                break
                    except:
                        info_text += "Error al mostrar la primera fila\n"
                detail_box = QMessageBox()
                detail_box.setWindowTitle("Datos obtenidos")
                detail_box.setText(info_text)
                detail_box.setIcon(QMessageBox.Icon.Information)
                detail_box.exec()
            else:
                QMessageBox.warning(self, "Sin datos", 
                                   "No se encontraron datos disponibles para este nodo destino.\n"
                                   "Asegúrese de que esté conectado a un nodo de origen o transformación que tenga datos.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Error", f"Error al obtener datos: {str(e)}")
        finally:
            if msg_box.isVisible():
                msg_box.close() 

    def _setup_column_selection_table(self, node_id, df, node_data):
        # Lógica para llenar la tabla de selección de columnas, con todas seleccionadas por defecto
        # Bloquear señales mientras se llena la tabla para evitar reentradas
        try:
            self.column_selection_table.blockSignals(True)
        except Exception:
            pass
        columns = list(df.columns)
        selected_cols = []
        renamed_cols = {}
        if 'output_cols' in node_data and node_data['output_cols']:
            # Aceptar nombres con prefijo OrigenX. y convertir a base
            raw_list = [col.strip() for col in node_data['output_cols'].split(',')]
            selected_cols = [c.split('.', 1)[1] if '.' in c else c for c in raw_list]
        if 'column_rename' in node_data and node_data['column_rename']:
            rename_pairs = node_data['column_rename'].split(',')
            for pair in rename_pairs:
                if ':' in pair:
                    old_name, new_name = pair.split(':', 1)
                    old_key = old_name.strip()
                    if '.' in old_key:
                        old_key = old_key.split('.', 1)[1]
                    renamed_cols[old_key] = new_name.strip()
        self.column_selection_table.setRowCount(len(columns))
        for i, col in enumerate(columns):
            checkbox = QTableWidgetItem()
            is_selected = col in selected_cols or not selected_cols  # Todas seleccionadas por defecto
            checkbox.setCheckState(Qt.CheckState.Checked if is_selected else Qt.CheckState.Unchecked)
            self.column_selection_table.setItem(i, 0, checkbox)
            self.column_checkboxes.append(checkbox)
            col_item = QTableWidgetItem(col)
            self.column_selection_table.setItem(i, 1, col_item)
            rename_field = QLineEdit()
            if col in renamed_cols:
                rename_field.setText(renamed_cols[col])
            else:
                rename_field.setPlaceholderText(col)
            self.column_selection_table.setCellWidget(i, 2, rename_field)
            self.column_rename_fields.append(rename_field)
            # Tipo (solo lectura)
            try:
                dtype_item = QTableWidgetItem(self._dtype_str(df, col))
                dtype_item.setFlags(dtype_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.column_selection_table.setItem(i, 3, dtype_item)
            except Exception:
                pass
        # Desbloquear señales al finalizar
        try:
            self.column_selection_table.blockSignals(False)
        except Exception:
            pass

    def _dtype_str(self, df, col_name: str) -> str:
        """Retorna el tipo de dato como texto para una columna."""
        try:
            if df is None:
                return ""
            if isinstance(df, pd.DataFrame):
                return str(df[col_name].dtype) if col_name in df.columns else ""
            # Polars
            if isinstance(df, pl.DataFrame):
                return str(df[col_name].dtype) if col_name in df.columns else ""
        except Exception:
            return ""
        return ""

    def _detect_join_keys(self, node_id):
        """Detecta automáticamente posibles claves de unión y actualiza los campos."""
        try:
            cfg = self.node_configs.get(node_id, {})
            df1 = cfg.get('dataframe')
            df2 = cfg.get('other_dataframe')
            if df1 is None or df2 is None:
                QMessageBox.information(self, "Datos insuficientes", "Conecte ambos orígenes y luego pulse 'Obtener datos'.")
                return
            if isinstance(df1, pd.DataFrame):
                df1 = pl.from_pandas(df1)
            if isinstance(df2, pd.DataFrame):
                df2 = pl.from_pandas(df2)
            cols1 = list(df1.columns)
            cols2 = list(df2.columns)
            exact = [c for c in cols1 if c in cols2]
            pairs = []
            if exact:
                try:
                    sample_n = min(len(df1), 5000)
                    df1s = df1.head(sample_n)
                    df2s = df2.head(sample_n)
                    scored = []
                    for c in exact:
                        try:
                            u1 = df1s[c].n_unique()
                            u2 = df2s[c].n_unique()
                            score = (float(u1) / max(1, sample_n)) + (float(u2) / max(1, sample_n))
                        except Exception:
                            score = 0.0
                        scored.append((score, c))
                    scored.sort(reverse=True)
                    top = [c for _, c in scored[:3]]
                    pairs = [(c, c) for c in top]
                except Exception:
                    pairs = [(c, c) for c in exact[:3]]
            else:
                def canon(s: str) -> str:
                    return re.sub(r"[^a-z0-9]+", "", s.lower())
                map1 = {}
                for c in cols1:
                    map1.setdefault(canon(c), []).append(c)
                used2 = set()
                for c2 in cols2:
                    key = canon(c2)
                    if key in map1 and map1[key]:
                        c1 = map1[key][0]
                        if c2 not in used2:
                            pairs.append((c1, c2))
                            used2.add(c2)
                    if len(pairs) >= 3:
                        break
            if not pairs:
                QMessageBox.information(self, "Sin coincidencias", "No se encontraron columnas coincidentes para la unión.")
                return
            if all(l == r for l, r in pairs):
                cols_text = ",".join(l for l, _ in pairs)
                if hasattr(self, 'join_fields') and 'join_cols' in self.join_fields:
                    self.join_fields['join_cols'].setText(cols_text)
                if hasattr(self, 'join_fields') and 'join_pairs' in self.join_fields:
                    self.join_fields['join_pairs'].setText("")
                cfg['join_cols'] = cols_text
                cfg.pop('join_pairs', None)
            else:
                pairs_text = ",".join(f"{l}:{r}" for l, r in pairs)
                if hasattr(self, 'join_fields') and 'join_pairs' in self.join_fields:
                    self.join_fields['join_pairs'].setText(pairs_text)
                if hasattr(self, 'join_fields') and 'join_cols' in self.join_fields:
                    self.join_fields['join_cols'].setText("")
                cfg['join_pairs'] = pairs_text
                cfg['join_cols'] = ''
            self.node_configs[node_id] = cfg
            self._schedule_autosave('transform', node_id)
        except Exception as e:
            QMessageBox.warning(self, "Error detectando claves", str(e))

    def _swap_join_inputs(self, node_id):
        """Intercambia Origen1 y Origen2, ajusta selección/renombres y pares de claves."""
        try:
            cfg = self.node_configs.get(node_id, {})
            df1 = cfg.get('dataframe')
            df2 = cfg.get('other_dataframe')
            if df1 is None or df2 is None:
                QMessageBox.information(self, "Datos insuficientes", "Se necesitan ambos orígenes conectados para intercambiar entradas.")
                return
            cfg['dataframe'], cfg['other_dataframe'] = df2, df1
            try:
                cfg['swap_inputs'] = not bool(cfg.get('swap_inputs'))
            except Exception:
                cfg['swap_inputs'] = True
            def swap_qual(name: str) -> str:
                n = name.strip()
                if n.startswith('Origen1.'):
                    return 'Origen2.' + n.split('.', 1)[1]
                if n.startswith('Origen2.'):
                    return 'Origen1.' + n.split('.', 1)[1]
                return n
            out_spec = cfg.get('output_cols') or ''
            if isinstance(out_spec, str) and out_spec.strip():
                parts = [p.strip() for p in out_spec.split(',') if p.strip()]
                parts = [swap_qual(p) for p in parts]
                cfg['output_cols'] = ','.join(parts)
            rn_spec = cfg.get('column_rename') or ''
            if isinstance(rn_spec, str) and rn_spec.strip():
                pairs = [p for p in rn_spec.split(',') if p.strip() and ':' in p]
                new_pairs = []
                for p in pairs:
                    old, new = p.split(':', 1)
                    new_pairs.append(f"{swap_qual(old.strip())}:{new.strip()}")
                cfg['column_rename'] = ','.join(new_pairs)
            jp = cfg.get('join_pairs') or ''
            if isinstance(jp, str) and jp.strip():
                pairs = [p for p in jp.split(',') if p.strip() and ':' in p]
                swapped = []
                for p in pairs:
                    l, r = p.split(':', 1)
                    swapped.append(f"{r.strip()}:{l.strip()}")
                cfg['join_pairs'] = ','.join(swapped)
            self.node_configs[node_id] = cfg
            self.show_node_properties(node_id, 'transform', cfg)
            self.node_config_changed.emit(node_id, cfg)
        except Exception as e:
            QMessageBox.warning(self, "Error al intercambiar", str(e))