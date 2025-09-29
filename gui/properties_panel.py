from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QLabel, QPushButton, 
                            QFileDialog, QTableWidget, QTableWidgetItem,
                            QComboBox, QLineEdit, QFormLayout, QGroupBox,
                            QHBoxLayout, QMessageBox, QTabWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
import polars as pl
import pandas as pd
import os

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
            self.column_selection_table.setColumnCount(3)
            self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como"])
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
        
        # Path del archivo y botón de carga
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
        transform_type.addItems(["Filtro", "Unión", "Agregación", "Mapeo"])
        
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
            filter_expr = QLineEdit()
            filter_expr.setText(node_data.get('filter_expr', ''))
            filter_expr.setPlaceholderText("Ejemplo: columna > 10")
            transform_layout.addRow("Expresión de filtro:", filter_expr)
            self.filter_expr_field = filter_expr
            # Auto-guardado
            self.filter_expr_field.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            
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
            
            join_config_group.setLayout(join_config_layout)
            transform_layout.addRow(join_config_group)
            # Auto-guardado de campos de unión
            join_type.currentTextChanged.connect(lambda *_: self._schedule_autosave('transform', node_id))
            join_cols.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            
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
            column_table.setColumnCount(3)
            column_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como"])
            
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
            group_by = QLineEdit()
            group_by.setText(node_data.get('group_by', ''))
            group_by.setPlaceholderText("columna1,columna2")
            transform_layout.addRow("Agrupar por:", group_by)
            
            agg_funcs = QLineEdit()
            agg_funcs.setText(node_data.get('agg_funcs', ''))
            agg_funcs.setPlaceholderText("sum(col1),avg(col2)")
            transform_layout.addRow("Funciones de agregación:", agg_funcs)
            
            self.agg_fields = {
                'group_by': group_by,
                'agg_funcs': agg_funcs
            }
            # Auto-guardado
            group_by.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            agg_funcs.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            
        elif subtype == 'map' or transform_type.currentText() == "Mapeo":
            map_expr = QLineEdit()
            map_expr.setText(node_data.get('map_expr', ''))
            map_expr.setPlaceholderText("nueva_col = col1 + col2")
            transform_layout.addRow("Expresión de mapeo:", map_expr)
            self.map_expr_field = map_expr
            # Auto-guardado
            self.map_expr_field.editingFinished.connect(lambda: self._schedule_autosave('transform', node_id))
            
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
                self.column_selection_table.setColumnCount(3)
                self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como"])
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
            self.column_selection_table.setColumnCount(3)
            self.column_selection_table.setHorizontalHeaderLabels(["Seleccionar", "Columna", "Renombrar como"])
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
            if hasattr(self, 'column_selection_table'):
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
        if hasattr(self, 'column_selection_table'):
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
                        is_selected = True if not selected_cols else (base_col in selected_cols)
                        checkbox.setCheckState(Qt.CheckState.Checked if is_selected else Qt.CheckState.Unchecked)
                        column_table.setItem(i, 0, checkbox)
                        self.column_checkboxes.append(checkbox)

                        # Columna 2: Nombre
                        column_name_item = QTableWidgetItem(col_name)
                        column_table.setItem(i, 1, column_name_item)

                        # Columna 3: Renombrar
                        rename_field = QLineEdit()
                        if base_col in renamed_cols:
                            rename_field.setText(renamed_cols[base_col])
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
        # Desbloquear señales
        try:
            self.column_selection_table.blockSignals(False)
        except Exception:
            pass
 