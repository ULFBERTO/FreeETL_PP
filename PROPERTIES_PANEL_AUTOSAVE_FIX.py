# Corrección para auto-guardado en PropertiesPanel
# El problema es que la configuración no se guarda automáticamente cuando se cambian los campos

# SOLUCIÓN 1: Agregar auto-guardado a todos los campos de entrada
# Modificar el método que crea los campos de base de datos en properties_panel.py

def create_database_fields_with_autosave(self, layout, node_id):
    """Crear campos de base de datos con auto-guardado automático"""
    
    # Tipo de base de datos
    db_type = QComboBox()
    db_type.addItems(["MySQL", "PostgreSQL", "SQLite", "SQL Server"])
    db_type.currentTextChanged.connect(lambda value: self.update_node_config_field(node_id, 'db_type', value))
    layout.addRow("Tipo de BD:", db_type)
    
    # Host
    host_field = QLineEdit()
    host_field.setPlaceholderText("localhost")
    host_field.textChanged.connect(lambda value: self.update_node_config_field(node_id, 'host', value))
    layout.addRow("Host:", host_field)
    
    # Puerto
    port_field = QLineEdit()
    port_field.setPlaceholderText("3306")
    port_field.textChanged.connect(lambda value: self.update_node_config_field(node_id, 'port', value))
    layout.addRow("Puerto:", port_field)
    
    # Usuario
    user_field = QLineEdit()
    user_field.setPlaceholderText("usuario")
    user_field.textChanged.connect(lambda value: self.update_node_config_field(node_id, 'user', value))
    layout.addRow("Usuario:", user_field)
    
    # Contraseña
    password_field = QLineEdit()
    password_field.setEchoMode(QLineEdit.EchoMode.Password)
    password_field.setPlaceholderText("contraseña")
    password_field.textChanged.connect(lambda value: self.update_node_config_field(node_id, 'password', value))
    layout.addRow("Contraseña:", password_field)
    
    # Base de datos
    database_field = QLineEdit()
    database_field.setPlaceholderText("nombre_bd")
    database_field.textChanged.connect(lambda value: self.update_node_config_field(node_id, 'database', value))
    layout.addRow("Base de datos:", database_field)
    
    # Consulta SQL
    query_field = QTextEdit()
    query_field.setPlaceholderText("SELECT * FROM tabla LIMIT 100")
    query_field.setMaximumHeight(100)
    query_field.textChanged.connect(lambda: self.update_node_config_field(node_id, 'query', query_field.toPlainText()))
    layout.addRow("Consulta SQL:", query_field)

def update_node_config_field(self, node_id, field_name, value):
    """Actualiza un campo específico de la configuración del nodo"""
    try:
        # Asegurar que existe la configuración del nodo
        if node_id not in self.node_configs:
            self.node_configs[node_id] = {'subtype': 'database'}
        
        # Actualizar el campo específico
        self.node_configs[node_id][field_name] = value
        
        # Emitir señal de cambio de configuración
        self.node_config_changed.emit(node_id, self.node_configs[node_id])
        
        print(f"[DEBUG] Campo {field_name} actualizado para nodo {node_id}: {value}")
        
    except Exception as e:
        print(f"Error actualizando campo {field_name} del nodo {node_id}: {e}")

# SOLUCIÓN 2: Asegurar que el subtype se establece al crear el nodo
# Modificar el método show_node_properties para asegurar que el subtype se guarda

def ensure_subtype_is_saved(self, node_id, node_type, node_data):
    """Asegurar que el subtype se guarda correctamente"""
    
    # Inicializar la configuración si no existe
    if node_id not in self.node_configs:
        self.node_configs[node_id] = {}
    
    # Asegurar que el subtype está presente
    if 'subtype' not in self.node_configs[node_id] and 'subtype' in node_data:
        self.node_configs[node_id]['subtype'] = node_data['subtype']
    
    # Si es un nodo de origen y no tiene subtype, establecer uno por defecto
    if node_type == 'source' and not self.node_configs[node_id].get('subtype'):
        self.node_configs[node_id]['subtype'] = 'database'  # o el tipo apropiado
    
    # Emitir cambio de configuración para asegurar que se propaga
    self.node_config_changed.emit(node_id, self.node_configs[node_id])

# SOLUCIÓN 3: Modificar el método show_node_properties existente
# Agregar estas líneas al INICIO del método show_node_properties:

"""
# NUEVO: Asegurar que el subtype se establece correctamente
if node_id not in self.node_configs:
    self.node_configs[node_id] = node_data.copy() if isinstance(node_data, dict) else {}

# Asegurar que el subtype está presente
if 'subtype' not in self.node_configs[node_id] and 'subtype' in node_data:
    self.node_configs[node_id]['subtype'] = node_data['subtype']

# Si es un nodo de origen sin subtype, usar el del node_data o establecer por defecto
if node_type == 'source' and not self.node_configs[node_id].get('subtype'):
    if 'subtype' in node_data:
        self.node_configs[node_id]['subtype'] = node_data['subtype']
    else:
        # Establecer un subtype por defecto basado en el contexto
        self.node_configs[node_id]['subtype'] = 'database'

# Emitir cambio inmediatamente para asegurar propagación
self.node_config_changed.emit(node_id, self.node_configs[node_id])
"""

# SOLUCIÓN 4: Corrección rápida temporal
# Agregar al método execute_source en etl_engine.py, después de obtener la config:

"""
# CORRECCIÓN TEMPORAL: Si no hay configuración, intentar obtenerla del canvas
if not config or config.get('subtype') is None:
    self.execution_progress.emit(f"ADVERTENCIA: Nodo {node_id} sin configuración válida, intentando recuperar...")
    
    # Intentar obtener configuración del properties panel si está disponible
    try:
        # Esta línea requeriría acceso al properties panel desde el engine
        # Alternativa: usar configuración por defecto para nodos de base de datos
        if node_id == 0:  # Asumiendo que el nodo 0 es típicamente base de datos
            config = {
                'subtype': 'database',
                'db_type': 'MySQL',
                'host': 'localhost',
                'port': '3306',
                'user': '',
                'password': '',
                'database': '',
                'query': 'SELECT 1'
            }
            self.execution_progress.emit(f"Usando configuración por defecto para nodo {node_id}")
    except Exception as e:
        self.execution_progress.emit(f"No se pudo recuperar configuración para nodo {node_id}: {e}")
"""
