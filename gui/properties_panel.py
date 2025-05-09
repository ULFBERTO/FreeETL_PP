from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QLabel, QPushButton, 
                            QFileDialog, QTableWidget, QTableWidgetItem,
                            QComboBox, QLineEdit, QFormLayout, QGroupBox,
                            QHBoxLayout, QMessageBox, QTabWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
import polars as pl
import pandas as pd

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
        
    def setup_ui(self):
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)
        
        # Panel vacío inicial
        self.empty_label = QLabel("Seleccione un nodo para configurar")
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.layout.addWidget(self.empty_label)
        
    def show_node_properties(self, node_id, node_type, node_data):
        # Limpiar el panel actual
        for i in reversed(range(self.layout.count())): 
            self.layout.itemAt(i).widget().setParent(None)
            
        # Guardar el ID y tipo de nodo actual
        self.current_node_id = node_id
        self.current_node_type = node_type
        
        # Inicializar la configuración si no existe
        if node_id not in self.node_configs:
            self.node_configs[node_id] = node_data.copy() if node_data else {'subtype': node_data.get('subtype')}
            
        # Usar la configuración guardada
        self.current_node_data = self.node_configs[node_id]
            
        if node_type == 'source':
            self.show_source_properties(node_id, self.current_node_data)
        elif node_type == 'transform':
            self.show_transform_properties(node_id, self.current_node_data)
        elif node_type == 'destination':
            self.show_destination_properties(node_id, self.current_node_data)
            
    def show_source_properties(self, node_id, node_data):
        # Grupo para propiedades de fuente
        source_group = QGroupBox("Propiedades de Fuente")
        source_layout = QFormLayout()
        
        # Selector de tipo de fuente
        source_type = QComboBox()
        source_type.addItems(["CSV", "Excel", "Base de Datos", "API"])
        
        # Establecer el subtipo actual si existe
        subtype = node_data.get('subtype')
        if subtype == 'csv':
            source_type.setCurrentText("CSV")
        elif subtype == 'excel':
            source_type.setCurrentText("Excel")
        elif subtype == 'database':
            source_type.setCurrentText("Base de Datos")
        elif subtype == 'api':
            source_type.setCurrentText("API")
            
        source_layout.addRow("Tipo:", source_type)
        
        # Botones de Limpiar y Guardar
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        save_button = QPushButton("Guardar")
        save_button.clicked.connect(lambda: self.save_node_config(node_id, source_type.currentText()))
        button_layout.addWidget(save_button)
        
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
        
        # Path del archivo y botón de carga
        file_path = QLineEdit()
        file_path.setText(node_data.get('path', ''))
        source_layout.addRow("Ruta del archivo:", file_path)
        self.file_path_field = file_path
        
        if subtype == 'csv' or source_type.currentText() == "CSV":
            load_button = QPushButton("Cargar Archivo")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='csv'))
            source_layout.addRow(load_button)
        elif subtype == 'excel' or source_type.currentText() == "Excel":
            load_button = QPushButton("Cargar Archivo Excel")
            load_button.clicked.connect(lambda: self.load_file(node_id, file_type='excel'))
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
        
        # Botones de Limpiar y Guardar
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        save_button = QPushButton("Guardar")
        save_button.clicked.connect(lambda: self.save_transform_config(node_id, transform_type.currentText()))
        button_layout.addWidget(save_button)
        
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
                
            # Detectar columnas comunes (necesarias para la unión)
            common_columns = []
            if 'dataframe' in node_data and 'other_dataframe' in node_data:
                df1_cols = set(df1.columns)
                df2_cols = set(df2.columns)
                common_columns = list(df1_cols.intersection(df2_cols))
            
            # Extraer configuración guardada de columnas y renombrados
            selected_cols = []
            renamed_cols = {}
            
            if 'output_cols' in node_data and node_data['output_cols']:
                selected_cols = [col.strip().split('.', 1)[-1] for col in node_data['output_cols'].split(',')]
                # Solo el nombre base
            
            if 'column_rename' in node_data and node_data['column_rename']:
                rename_pairs = node_data['column_rename'].split(',')
                for pair in rename_pairs:
                    if ':' in pair:
                        old_name, new_name = pair.split(':', 1)
                        renamed_cols[old_name.strip().split('.', 1)[-1]] = new_name.strip()
            
            # Llenar la tabla con todas las columnas disponibles
            column_table.setRowCount(len(all_columns))
            
            for i, col_name in enumerate(all_columns):
                # Columna 1: Checkbox para seleccionar
                checkbox = QTableWidgetItem()
                base_col = col_name.split('.', 1)[-1]
                is_selected = base_col in selected_cols or base_col in common_columns
                if not selected_cols:
                    is_selected = True
                checkbox.setCheckState(Qt.CheckState.Checked if is_selected else Qt.CheckState.Unchecked)
                column_table.setItem(i, 0, checkbox)
                self.column_checkboxes.append(checkbox)
                
                # Columna 2: Nombre de la columna
                column_name_item = QTableWidgetItem(col_name)
                column_table.setItem(i, 1, column_name_item)
                
                # Columna 3: Campo para renombrar
                rename_field = QLineEdit()
                if base_col in renamed_cols:
                    rename_field.setText(renamed_cols[base_col])
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
            
        elif subtype == 'map' or transform_type.currentText() == "Mapeo":
            map_expr = QLineEdit()
            map_expr.setText(node_data.get('map_expr', ''))
            map_expr.setPlaceholderText("nueva_col = col1 + col2")
            transform_layout.addRow("Expresión de mapeo:", map_expr)
            self.map_expr_field = map_expr
            
        # --- Selección de columnas obligatoria para todos los nodos de transformación ---
        df = node_data.get('dataframe')
        if df is not None:
            self.data_table = QTableWidget()
            self.setup_data_preview(self.data_table, df)
            transform_layout.addRow("Vista previa:", self.data_table)
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
        dest_type.addItems(["CSV", "Excel", "Base de Datos", "API"])
        
        # Establecer el subtipo actual si existe
        subtype = node_data.get('subtype')
        if subtype == 'csv':
            dest_type.setCurrentText("CSV")
        elif subtype == 'excel':
            dest_type.setCurrentText("Excel")
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
        
        # Botones de Limpiar y Guardar
        button_layout = QHBoxLayout()
        
        clear_button = QPushButton("Limpiar")
        clear_button.clicked.connect(lambda: self.clear_node_config(node_id))
        button_layout.addWidget(clear_button)
        
        save_button = QPushButton("Guardar")
        save_button.clicked.connect(lambda: self.save_destination_config(node_id, dest_type.currentText()))
        button_layout.addWidget(save_button)
        
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
        elif subtype == 'csv' or dest_type.currentText() == "CSV":
            file_path = QLineEdit()
            file_path.setText(node_data.get('path', ''))
            dest_layout.addRow("Ruta del archivo:", file_path)
            self.dest_file_path = file_path
            select_path = QPushButton("Seleccionar ruta")
            select_path.clicked.connect(lambda: self.select_output_path('csv'))
            dest_layout.addRow(select_path)
            format_type = QComboBox()
            format_type.addItems(["CSV", "Excel", "Parquet", "JSON"])
            if 'format' in node_data:
                format_type.setCurrentText(node_data['format'])
            dest_layout.addRow("Formato:", format_type)
            self.dest_format = format_type
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
        dest_group.setLayout(dest_layout)
        self.layout.addWidget(dest_group)
        
    def update_source_type(self, node_id, new_type):
        """Actualiza el tipo de fuente y reconstruye el panel"""
        self.current_source_type = new_type
        # Convertir el tipo UI a subtipo interno
        subtype_map = {"CSV": "csv", "Excel": "excel", "Base de Datos": "database", "API": "api"}
        subtype = subtype_map.get(new_type, "csv")
        
        # Guardar el subtipo en la configuración del nodo
        self.node_configs[node_id]['subtype'] = subtype
        
        # Reconstruir el panel de propiedades
        self.show_node_properties(node_id, 'source', self.node_configs[node_id])
        
    def update_transform_type(self, node_id, new_type):
        """Actualiza el tipo de transformación y reconstruye el panel"""
        self.current_transform_type = new_type
        # Convertir el tipo UI a subtipo interno
        subtype_map = {"Filtro": "filter", "Unión": "join", "Agregación": "aggregate", "Mapeo": "map"}
        subtype = subtype_map.get(new_type, "filter")
        
        # Guardar el subtipo en la configuración del nodo
        self.node_configs[node_id]['subtype'] = subtype
        
        # Reconstruir el panel de propiedades
        self.show_node_properties(node_id, 'transform', self.node_configs[node_id])
        
    def update_destination_type(self, node_id, new_type):
        """Actualiza el tipo de destino y reconstruye el panel"""
        self.current_dest_type = new_type
        subtype_map = {"CSV": "csv", "Excel": "excel", "Base de Datos": "database", "API": "api"}
        subtype = subtype_map.get(new_type, "csv")
        self.node_configs[node_id]['subtype'] = subtype
        self.show_node_properties(node_id, 'destination', self.node_configs[node_id])
        
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
        config = self.node_configs.get(node_id, {})
        
        # Guardar configuración según el tipo
        if source_type == "CSV":
            if hasattr(self, 'file_path_field'):
                config['path'] = self.file_path_field.text()
            
            # Guardar mapeo de columnas si existe
            if hasattr(self, 'column_selection_table') and self.column_selection_table.rowCount() > 0:
                selected_columns = []
                rename_mappings = []
                for i in range(self.column_selection_table.rowCount()):
                    checkbox = self.column_selection_table.item(i, 0)
                    col_name = self.column_selection_table.item(i, 1).text()
                    rename_field = self.column_selection_table.cellWidget(i, 2)
                    if checkbox and checkbox.checkState() == Qt.CheckState.Checked:
                        selected_columns.append(col_name)
                        if rename_field and rename_field.text().strip():
                            rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
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
        
        # Guardar configuración
        self.node_configs[node_id] = config
        
        # Emitir señal de cambio de configuración
        self.node_config_changed.emit(node_id, config)
        
        QMessageBox.information(self, "Configuración guardada", "La configuración del nodo ha sido guardada")
        
    def save_transform_config(self, node_id, transform_type):
        """Guarda la configuración de un nodo de transformación"""
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
        
        # Guardar selección y renombrado de columnas
        if hasattr(self, 'column_selection_table') and self.column_selection_table.rowCount() > 0:
            selected_columns = []
            rename_mappings = []
            for i in range(self.column_selection_table.rowCount()):
                checkbox = self.column_selection_table.item(i, 0)
                col_name = self.column_selection_table.item(i, 1).text()
                rename_field = self.column_selection_table.cellWidget(i, 2)
                if checkbox and checkbox.checkState() == Qt.CheckState.Checked:
                    selected_columns.append(col_name)
                    if rename_field and rename_field.text().strip():
                        rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
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
        config = self.node_configs.get(node_id, {})
        
        # Guardar configuración según el tipo
        if dest_type == "CSV":
            if hasattr(self, 'dest_file_path'):
                config['path'] = self.dest_file_path.text()
            if hasattr(self, 'dest_format'):
                config['format'] = self.dest_format.currentText()
                
        elif dest_type == "Base de Datos":
            if hasattr(self, 'dest_db_fields'):
                for key, field in self.dest_db_fields.items():
                    if isinstance(field, QComboBox):
                        config[key] = field.currentText()
                    else:
                        config[key] = field.text()
                        
        elif dest_type == "API":
            if hasattr(self, 'dest_api_fields'):
                for key, field in self.dest_api_fields.items():
                    if isinstance(field, QComboBox):
                        config[key] = field.currentText()
                    else:
                        config[key] = field.text()
        
        # Guardar selección y renombrado de columnas
        if hasattr(self, 'column_selection_table') and self.column_selection_table.rowCount() > 0:
            selected_columns = []
            rename_mappings = []
            for i in range(self.column_selection_table.rowCount()):
                checkbox = self.column_selection_table.item(i, 0)
                col_name = self.column_selection_table.item(i, 1).text()
                rename_field = self.column_selection_table.cellWidget(i, 2)
                if checkbox and checkbox.checkState() == Qt.CheckState.Checked:
                    selected_columns.append(col_name)
                    if rename_field and rename_field.text().strip():
                        rename_mappings.append(f"{col_name}:{rename_field.text().strip()}")
            config['output_cols'] = ','.join(selected_columns)
            config['column_rename'] = ','.join(rename_mappings)
        
        # Guardar configuración
        self.node_configs[node_id] = config
        
        # Emitir señal de cambio de configuración
        self.node_config_changed.emit(node_id, config)
        
        QMessageBox.information(self, "Configuración guardada", "La configuración del nodo ha sido guardada")
        
    def select_output_path(self, file_type=None):
        if file_type == 'excel':
            format_filter = "Excel (*.xlsx)"
            extension = ".xlsx"
        else:
            format_filter = "CSV (*.csv)"
            extension = ".csv"
        directory = QFileDialog.getExistingDirectory(
            self,
            "Seleccionar carpeta para guardar archivo",
            "D:/"
        )
        if directory:
            file_name, _ = QFileDialog.getSaveFileName(
                self,
                "Guardar archivo",
                directory + "/output" + extension,
                "Excel (*.xlsx);;CSV (*.csv)",
                format_filter
            )
            if file_name:
                if not any(file_name.endswith(ext) for ext in ['.csv', '.xlsx']):
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
        else:
            file_name, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo",
                "",
                "Todos los archivos (*.*);;CSV (*.csv);;Excel (*.xlsx);;JSON (*.json)"
            )
        
        if file_name:
            try:
                if file_type == 'excel' or file_name.endswith('.xlsx'):
                    df = pl.read_excel(file_name)
                elif file_type == 'csv' or file_name.endswith('.csv'):
                    try:
                        df = pl.read_csv(file_name)
                    except:
                        try:
                            df = pl.read_csv(file_name, encoding='latin-1')
                        except:
                            df = pd.read_csv(file_name, encoding='latin-1')
                            df = pl.from_pandas(df)
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
                
                # Verificar si tenemos una tabla de columnas configurada
                if hasattr(self, 'column_checkboxes') and len(all_columns) > 0:
                    # Actualizar la tabla de columnas
                    column_table = self.join_fields['column_table']
                    column_table.setRowCount(len(all_columns))
                    
                    for i, col_name in enumerate(all_columns):
                        # Columna 1: Checkbox para seleccionar
                        checkbox = QTableWidgetItem()
                        checkbox.setCheckState(Qt.CheckState.Checked)
                        column_table.setItem(i, 0, checkbox)
                        self.column_checkboxes[i] = checkbox if i < len(self.column_checkboxes) else checkbox
                        
                        # Columna 2: Nombre de la columna
                        column_name_item = QTableWidgetItem(col_name)
                        column_table.setItem(i, 1, column_name_item)
                        
                        # Columna 3: Campo para renombrar
                        rename_field = QLineEdit()
                        rename_field.setPlaceholderText(col_name.split('.')[-1])
                        column_table.setCellWidget(i, 2, rename_field)
                        self.column_rename_fields[i] = rename_field if i < len(self.column_rename_fields) else rename_field
                    
                    # Ajustar tamaño
                    column_table.resizeColumnsToContents()
            
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
        columns = list(df.columns)
        selected_cols = []
        renamed_cols = {}
        if 'output_cols' in node_data and node_data['output_cols']:
            selected_cols = [col.strip() for col in node_data['output_cols'].split(',')]
        if 'column_rename' in node_data and node_data['column_rename']:
            rename_pairs = node_data['column_rename'].split(',')
            for pair in rename_pairs:
                if ':' in pair:
                    old_name, new_name = pair.split(':', 1)
                    renamed_cols[old_name.strip()] = new_name.strip()
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