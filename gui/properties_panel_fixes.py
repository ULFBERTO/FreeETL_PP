# Métodos adicionales para PropertiesPanel para corregir persistencia de estado
# Agregar estos métodos a la clase PropertiesPanel en properties_panel.py

def _save_current_node_state(self):
    """Guarda el estado actual del nodo antes de cambiar a otro"""
    if not hasattr(self, 'current_node_id') or self.current_node_id is None:
        return
    
    try:
        # Guardar configuración actual en node_configs
        current_config = self.node_configs.get(self.current_node_id, {})
        
        # Guardar cualquier dataframe que esté en memoria
        if hasattr(self, 'current_dataframes') and self.current_node_id in self.current_dataframes:
            current_config['dataframe'] = self.current_dataframes[self.current_node_id]
        
        # Actualizar la configuración guardada
        self.node_configs[self.current_node_id] = current_config
        
    except Exception as e:
        print(f"Error guardando estado del nodo {self.current_node_id}: {e}")

def _restore_source_data_preview(self, node_id, df):
    """Restaura la vista previa de datos para un nodo de origen"""
    try:
        # Buscar el grupo de propiedades de origen
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if isinstance(item.widget(), QGroupBox):
                group_title = item.widget().title().lower()
                if any(word in group_title for word in ['origen', 'source', 'propiedades']):
                    source_group = item.widget()
                    source_layout = source_group.layout()
                    
                    # Verificar si ya existe una vista previa
                    preview_exists = False
                    for j in range(source_layout.rowCount()):
                        label_item = source_layout.itemAt(j, QFormLayout.ItemRole.LabelRole)
                        if label_item and label_item.widget():
                            label_text = label_item.widget().text().lower()
                            if 'vista previa' in label_text or 'preview' in label_text:
                                preview_exists = True
                                break
                    
                    if not preview_exists:
                        # Crear tabla de vista previa
                        data_preview = QTableWidget()
                        self.setup_data_preview(data_preview, df)
                        source_layout.addRow("Vista previa:", data_preview)
                        
                        # Crear tabla de selección de columnas si no existe
                        self._create_column_selection_table_if_needed(source_group, df, node_id)
                    break
    except Exception as e:
        print(f"Error restaurando vista previa de origen: {e}")

def _restore_destination_data_preview(self, node_id, df):
    """Restaura la vista previa de datos para un nodo de destino"""
    try:
        # Buscar el grupo de propiedades de destino
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if isinstance(item.widget(), QGroupBox):
                group_title = item.widget().title().lower()
                if any(word in group_title for word in ['destino', 'destination', 'propiedades']):
                    dest_group = item.widget()
                    dest_layout = dest_group.layout()
                    
                    # Verificar si ya existe una vista previa
                    preview_exists = False
                    for j in range(dest_layout.rowCount()):
                        label_item = dest_layout.itemAt(j, QFormLayout.ItemRole.LabelRole)
                        if label_item and label_item.widget():
                            label_text = label_item.widget().text().lower()
                            if 'vista previa' in label_text or 'preview' in label_text:
                                preview_exists = True
                                break
                    
                    if not preview_exists:
                        # Crear tabla de vista previa
                        data_preview = QTableWidget()
                        self.setup_data_preview(data_preview, df)
                        dest_layout.addRow("Vista previa:", data_preview)
                    break
    except Exception as e:
        print(f"Error restaurando vista previa de destino: {e}")

def _restore_transform_data_preview(self, node_id, df):
    """Restaura la vista previa de datos para un nodo de transformación"""
    try:
        # Buscar el grupo de propiedades de transformación
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if isinstance(item.widget(), QGroupBox):
                group_title = item.widget().title().lower()
                if any(word in group_title for word in ['transformación', 'transform', 'propiedades']):
                    transform_group = item.widget()
                    transform_layout = transform_group.layout()
                    
                    # Verificar si ya existe una vista previa
                    preview_exists = False
                    for j in range(transform_layout.rowCount()):
                        label_item = transform_layout.itemAt(j, QFormLayout.ItemRole.LabelRole)
                        if label_item and label_item.widget():
                            label_text = label_item.widget().text().lower()
                            if 'vista previa' in label_text or 'preview' in label_text:
                                preview_exists = True
                                break
                    
                    if not preview_exists:
                        # Crear tabla de vista previa
                        data_preview = QTableWidget()
                        self.setup_data_preview(data_preview, df)
                        transform_layout.addRow("Vista previa:", data_preview)
                    break
    except Exception as e:
        print(f"Error restaurando vista previa de transformación: {e}")

def _create_column_selection_table_if_needed(self, parent_group, df, node_id):
    """Crea tabla de selección de columnas si no existe"""
    try:
        parent_layout = parent_group.layout()
        
        # Verificar si ya existe tabla de selección
        selection_exists = False
        for j in range(parent_layout.rowCount()):
            label_item = parent_layout.itemAt(j, QFormLayout.ItemRole.LabelRole)
            if label_item and label_item.widget():
                label_text = label_item.widget().text().lower()
                if 'columnas' in label_text and 'seleccionar' in label_text:
                    selection_exists = True
                    break
        
        if not selection_exists and hasattr(self, '_create_column_selection_table'):
            self._create_column_selection_table(parent_group, df, node_id)
            
    except Exception as e:
        print(f"Error creando tabla de selección de columnas: {e}")

# Modificación para el método show_node_properties existente
# Agregar al FINAL del método show_node_properties (antes de self._ui_rebuilding = False):

"""
        # NUEVO: Restaurar datos previamente obtenidos si existen
        if 'dataframe' in self.current_node_data:
            df = self.current_node_data['dataframe']
            
            # Si es un nodo de origen, mostrar vista previa
            if node_type == 'source':
                self._restore_source_data_preview(node_id, df)
            
            # Si es un nodo de destino, mostrar vista previa
            elif node_type == 'destination':
                self._restore_destination_data_preview(node_id, df)
            
            # Si es un nodo de transformación, mostrar vista previa
            elif node_type == 'transform':
                self._restore_transform_data_preview(node_id, df)
        
        # Desactivar bandera de reconstrucción
        self._ui_rebuilding = False
"""

# Modificación para el INICIO del método show_node_properties
# Agregar después de la línea self._ui_rebuilding = True:

"""
        # NUEVO: Guardar estado del nodo anterior antes de cambiar
        if hasattr(self, 'current_node_id') and hasattr(self, 'current_node_id') and self.current_node_id is not None and self.current_node_id != node_id:
            self._save_current_node_state()
"""
