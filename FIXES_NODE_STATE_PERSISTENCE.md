# Correcciones para Persistencia del Estado de Nodos

## Problema Identificado
Cuando se hace clic en "Obtener Datos" en un nodo y luego se selecciona otro nodo, al volver al nodo original se pierde el estado (datos obtenidos, configuraciones, etc.).

## Causa del Problema
1. El método `show_node_properties` no está restaurando completamente el estado previo del nodo
2. Los datos obtenidos se guardan en `node_configs` pero no se restauran correctamente en la UI
3. Falta persistencia del estado al guardar/abrir archivos ETL

## Soluciones Requeridas

### 1. Mejorar `show_node_properties` en PropertiesPanel

Agregar al final del método `show_node_properties` (después de crear la UI):

```python
# Al final del método show_node_properties, después de crear toda la UI
# Restaurar datos previamente obtenidos si existen
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
```

### 2. Agregar Métodos de Restauración

Agregar estos métodos a la clase `PropertiesPanel`:

```python
def _restore_source_data_preview(self, node_id, df):
    """Restaura la vista previa de datos para un nodo de origen"""
    try:
        # Buscar el grupo de propiedades de origen
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if isinstance(item.widget(), QGroupBox) and "Origen" in item.widget().title():
                source_group = item.widget()
                source_layout = source_group.layout()
                
                # Crear tabla de vista previa
                data_preview = QTableWidget()
                self.setup_data_preview(data_preview, df)
                source_layout.addRow("Vista previa:", data_preview)
                
                # Crear tabla de selección de columnas
                self._create_column_selection_table(source_group, df, node_id)
                break
    except Exception as e:
        print(f"Error restaurando vista previa de origen: {e}")

def _restore_destination_data_preview(self, node_id, df):
    """Restaura la vista previa de datos para un nodo de destino"""
    try:
        # Buscar el grupo de propiedades de destino
        for i in range(self.layout.count()):
            item = self.layout.itemAt(i)
            if isinstance(item.widget(), QGroupBox) and "Destino" in item.widget().title():
                dest_group = item.widget()
                dest_layout = dest_group.layout()
                
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
            if isinstance(item.widget(), QGroupBox) and "Transformación" in item.widget().title():
                transform_group = item.widget()
                transform_layout = transform_group.layout()
                
                # Crear tabla de vista previa
                data_preview = QTableWidget()
                self.setup_data_preview(data_preview, df)
                transform_layout.addRow("Vista previa:", data_preview)
                break
    except Exception as e:
        print(f"Error restaurando vista previa de transformación: {e}")
```

### 3. Mejorar Persistencia al Cambiar de Nodo

Modificar el método `show_node_properties` para guardar el estado actual antes de cambiar:

```python
def show_node_properties(self, node_id, node_type, node_data):
    # NUEVO: Guardar estado del nodo anterior antes de cambiar
    if hasattr(self, 'current_node_id') and self.current_node_id != node_id:
        self._save_current_node_state()
    
    # ... resto del método existente ...
```

Y agregar el método para guardar estado:

```python
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
```

### 4. Mejorar Guardar/Abrir ETL

En `main_window.py`, modificar los métodos de guardar y abrir para incluir los datos:

```python
def save_pipeline(self):
    """Guarda el pipeline completo incluyendo configuraciones y datos"""
    file_path, _ = QFileDialog.getSaveFileName(self, "Guardar Pipeline", "", "JSON Files (*.json)")
    if file_path:
        try:
            # Obtener configuraciones de todos los nodos
            pipeline_data = {
                'nodes': {},
                'edges': []
            }
            
            # Guardar nodos con configuraciones
            for node_id in self.pipeline_canvas.graph.nodes:
                node_info = self.pipeline_canvas.graph.nodes[node_id]
                config = node_info.get('config', {})
                
                # Convertir DataFrame a dict para serialización
                if 'dataframe' in config:
                    try:
                        df = config['dataframe']
                        if hasattr(df, 'to_dict'):
                            config['dataframe_data'] = df.to_dict('records')
                            config['dataframe_columns'] = list(df.columns)
                            # Remover el DataFrame original para serialización
                            config_copy = config.copy()
                            del config_copy['dataframe']
                            config = config_copy
                    except Exception as e:
                        print(f"Error serializando DataFrame del nodo {node_id}: {e}")
                
                pipeline_data['nodes'][node_id] = {
                    'type': node_info['type'],
                    'config': config,
                    'position': node_info.get('position', {'x': 0, 'y': 0})
                }
            
            # Guardar conexiones
            for edge in self.pipeline_canvas.graph.edges:
                pipeline_data['edges'].append({
                    'source': edge[0],
                    'target': edge[1]
                })
            
            # Escribir archivo
            import json
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(pipeline_data, f, indent=2, ensure_ascii=False)
            
            self.log_message(f"Pipeline guardado en: {file_path}")
            QMessageBox.information(self, "Guardado", "Pipeline guardado exitosamente")
            
        except Exception as e:
            self.log_message(f"Error guardando pipeline: {e}")
            QMessageBox.critical(self, "Error", f"Error guardando pipeline: {e}")

def load_pipeline(self):
    """Carga un pipeline completo incluyendo configuraciones y datos"""
    file_path, _ = QFileDialog.getOpenFileName(self, "Abrir Pipeline", "", "JSON Files (*.json)")
    if file_path:
        try:
            import json
            with open(file_path, 'r', encoding='utf-8') as f:
                pipeline_data = json.load(f)
            
            # Limpiar canvas actual
            self.pipeline_canvas.clear_canvas()
            
            # Cargar nodos
            for node_id_str, node_info in pipeline_data.get('nodes', {}).items():
                node_id = int(node_id_str)
                node_type = node_info['type']
                config = node_info.get('config', {})
                position = node_info.get('position', {'x': 0, 'y': 0})
                
                # Restaurar DataFrame si existe
                if 'dataframe_data' in config and 'dataframe_columns' in config:
                    try:
                        import pandas as pd
                        df_data = config['dataframe_data']
                        df_columns = config['dataframe_columns']
                        df = pd.DataFrame(df_data, columns=df_columns)
                        config['dataframe'] = df
                        # Limpiar datos serializados
                        del config['dataframe_data']
                        del config['dataframe_columns']
                    except Exception as e:
                        print(f"Error restaurando DataFrame del nodo {node_id}: {e}")
                
                # Crear nodo en canvas
                scene_pos = QPointF(position['x'], position['y'])
                self.pipeline_canvas.add_node(node_type, scene_pos, config.get('subtype'))
                
                # Guardar configuración en properties panel
                self.properties_panel.node_configs[node_id] = config
            
            # Cargar conexiones
            for edge_info in pipeline_data.get('edges', []):
                source_id = edge_info['source']
                target_id = edge_info['target']
                self.pipeline_canvas.add_connection(source_id, target_id)
            
            self.log_message(f"Pipeline cargado desde: {file_path}")
            QMessageBox.information(self, "Cargado", "Pipeline cargado exitosamente")
            
        except Exception as e:
            self.log_message(f"Error cargando pipeline: {e}")
            QMessageBox.critical(self, "Error", f"Error cargando pipeline: {e}")
```

## Implementación Paso a Paso

1. **Modificar PropertiesPanel**: Agregar los métodos de restauración
2. **Actualizar show_node_properties**: Incluir restauración de estado
3. **Mejorar MainWindow**: Actualizar métodos de guardar/abrir
4. **Probar**: Verificar que el estado se mantiene al cambiar entre nodos

## Beneficios de las Correcciones

- ✅ Estado persistente entre cambios de nodo
- ✅ Datos obtenidos se mantienen al volver al nodo
- ✅ Configuraciones se preservan correctamente
- ✅ Guardar/abrir ETL incluye todos los datos
- ✅ Mejor experiencia de usuario
- ✅ No se pierden datos al trabajar con múltiples nodos
