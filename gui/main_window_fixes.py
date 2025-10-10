# Métodos mejorados para MainWindow para guardar/abrir ETL con persistencia de datos
# Reemplazar o agregar estos métodos en main_window.py

def save_pipeline(self):
    """Guarda el pipeline completo incluyendo configuraciones y datos"""
    file_path, _ = QFileDialog.getSaveFileName(self, "Guardar Pipeline", "", "JSON Files (*.json)")
    if file_path:
        try:
            # Obtener configuraciones de todos los nodos
            pipeline_data = {
                'nodes': {},
                'edges': [],
                'version': '1.0'
            }
            
            # Guardar nodos con configuraciones
            for node_id in self.pipeline_canvas.graph.nodes:
                node_info = self.pipeline_canvas.graph.nodes[node_id]
                config = node_info.get('config', {}).copy()
                
                # Convertir DataFrame a dict para serialización
                if 'dataframe' in config:
                    try:
                        df = config['dataframe']
                        if hasattr(df, 'to_dict'):
                            # Para Polars DataFrame
                            if hasattr(df, 'to_pandas'):
                                df_pandas = df.to_pandas()
                                config['dataframe_data'] = df_pandas.to_dict('records')
                                config['dataframe_columns'] = list(df_pandas.columns)
                                config['dataframe_dtypes'] = {col: str(dtype) for col, dtype in df_pandas.dtypes.items()}
                            # Para Pandas DataFrame
                            else:
                                config['dataframe_data'] = df.to_dict('records')
                                config['dataframe_columns'] = list(df.columns)
                                config['dataframe_dtypes'] = {col: str(dtype) for col, dtype in df.dtypes.items()}
                            
                            # Remover el DataFrame original para serialización
                            del config['dataframe']
                            
                            # Marcar que tenía datos
                            config['had_dataframe'] = True
                            
                    except Exception as e:
                        print(f"Error serializando DataFrame del nodo {node_id}: {e}")
                        config['had_dataframe'] = False
                
                # Obtener posición del nodo en el canvas
                position = {'x': 0, 'y': 0}
                try:
                    if hasattr(self.pipeline_canvas, 'node_items') and node_id in self.pipeline_canvas.node_items:
                        node_item = self.pipeline_canvas.node_items[node_id]
                        pos = node_item.pos()
                        position = {'x': pos.x(), 'y': pos.y()}
                except Exception:
                    pass
                
                pipeline_data['nodes'][str(node_id)] = {
                    'type': node_info['type'],
                    'config': config,
                    'position': position
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
            import pandas as pd
            import polars as pl
            
            with open(file_path, 'r', encoding='utf-8') as f:
                pipeline_data = json.load(f)
            
            # Limpiar canvas actual
            self.pipeline_canvas.clear_canvas()
            self.properties_panel.node_configs.clear()
            self.properties_panel.current_dataframes.clear()
            
            # Cargar nodos
            loaded_nodes = {}
            for node_id_str, node_info in pipeline_data.get('nodes', {}).items():
                node_id = int(node_id_str)
                node_type = node_info['type']
                config = node_info.get('config', {}).copy()
                position = node_info.get('position', {'x': 0, 'y': 0})
                
                # Restaurar DataFrame si existe
                if config.get('had_dataframe', False) and 'dataframe_data' in config and 'dataframe_columns' in config:
                    try:
                        df_data = config['dataframe_data']
                        df_columns = config['dataframe_columns']
                        df_dtypes = config.get('dataframe_dtypes', {})
                        
                        # Crear DataFrame pandas primero
                        df_pandas = pd.DataFrame(df_data, columns=df_columns)
                        
                        # Aplicar tipos de datos si están disponibles
                        for col, dtype_str in df_dtypes.items():
                            try:
                                if col in df_pandas.columns:
                                    if 'int' in dtype_str.lower():
                                        df_pandas[col] = pd.to_numeric(df_pandas[col], errors='coerce')
                                    elif 'float' in dtype_str.lower():
                                        df_pandas[col] = pd.to_numeric(df_pandas[col], errors='coerce')
                                    elif 'datetime' in dtype_str.lower():
                                        df_pandas[col] = pd.to_datetime(df_pandas[col], errors='coerce')
                            except Exception:
                                pass
                        
                        # Convertir a Polars si es necesario
                        try:
                            df_polars = pl.from_pandas(df_pandas)
                            config['dataframe'] = df_polars
                        except Exception:
                            # Fallback a pandas si Polars falla
                            config['dataframe'] = df_pandas
                        
                        # Limpiar datos serializados
                        for key in ['dataframe_data', 'dataframe_columns', 'dataframe_dtypes', 'had_dataframe']:
                            if key in config:
                                del config[key]
                                
                        self.log_message(f"Datos restaurados para nodo {node_id}: {len(df_pandas)} filas, {len(df_pandas.columns)} columnas")
                        
                    except Exception as e:
                        print(f"Error restaurando DataFrame del nodo {node_id}: {e}")
                        self.log_message(f"Advertencia: No se pudieron restaurar los datos del nodo {node_id}")
                
                # Crear nodo en canvas
                scene_pos = QPointF(position['x'], position['y'])
                created_node_id = self.pipeline_canvas.add_node(node_type, scene_pos, config.get('subtype'))
                
                # Asegurar que el ID coincida
                if created_node_id != node_id:
                    # Actualizar el mapeo si es necesario
                    loaded_nodes[node_id] = created_node_id
                else:
                    loaded_nodes[node_id] = node_id
                
                # Guardar configuración en properties panel
                actual_node_id = loaded_nodes[node_id]
                self.properties_panel.node_configs[actual_node_id] = config
                
                # Guardar dataframe en current_dataframes también
                if 'dataframe' in config:
                    self.properties_panel.current_dataframes[actual_node_id] = config['dataframe']
            
            # Cargar conexiones con IDs actualizados
            for edge_info in pipeline_data.get('edges', []):
                source_id = loaded_nodes.get(edge_info['source'], edge_info['source'])
                target_id = loaded_nodes.get(edge_info['target'], edge_info['target'])
                
                try:
                    self.pipeline_canvas.add_connection(source_id, target_id)
                except Exception as e:
                    print(f"Error creando conexión {source_id} -> {target_id}: {e}")
            
            self.log_message(f"Pipeline cargado desde: {file_path}")
            self.log_message(f"Nodos cargados: {len(loaded_nodes)}")
            QMessageBox.information(self, "Cargado", f"Pipeline cargado exitosamente\nNodos: {len(loaded_nodes)}\nConexiones: {len(pipeline_data.get('edges', []))}")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.log_message(f"Error cargando pipeline: {e}")
            QMessageBox.critical(self, "Error", f"Error cargando pipeline: {e}")

def create_menu_bar(self):
    """Crea la barra de menú con opciones mejoradas"""
    menubar = self.menuBar()
    
    # File menu
    file_menu = menubar.addMenu('Archivo')
    
    # Nuevo pipeline
    new_action = file_menu.addAction('Nuevo Pipeline')
    new_action.setShortcut('Ctrl+N')
    new_action.triggered.connect(self.new_pipeline)
    
    file_menu.addSeparator()
    
    # Guardar pipeline
    save_action = file_menu.addAction('Guardar Pipeline')
    save_action.setShortcut('Ctrl+S')
    save_action.triggered.connect(self.save_pipeline)
    
    # Abrir pipeline
    open_action = file_menu.addAction('Abrir Pipeline')
    open_action.setShortcut('Ctrl+O')
    open_action.triggered.connect(self.load_pipeline)
    
    file_menu.addSeparator()
    
    # Salir
    exit_action = file_menu.addAction('Salir')
    exit_action.setShortcut('Ctrl+Q')
    exit_action.triggered.connect(self.close)
    
    # Pipeline menu
    pipeline_menu = menubar.addMenu('Pipeline')
    
    # Ejecutar pipeline
    execute_action = pipeline_menu.addAction('Ejecutar Pipeline')
    execute_action.setShortcut('F5')
    execute_action.triggered.connect(self.execute_pipeline)
    
    # Limpiar canvas
    clear_action = pipeline_menu.addAction('Limpiar Canvas')
    clear_action.triggered.connect(self.clear_canvas)

def new_pipeline(self):
    """Crea un nuevo pipeline limpio"""
    reply = QMessageBox.question(self, 'Nuevo Pipeline', 
                                '¿Estás seguro de que quieres crear un nuevo pipeline?\nSe perderán todos los cambios no guardados.',
                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                QMessageBox.StandardButton.No)
    
    if reply == QMessageBox.StandardButton.Yes:
        self.pipeline_canvas.clear_canvas()
        self.properties_panel.node_configs.clear()
        self.properties_panel.current_dataframes.clear()
        self.properties_panel.clear_properties_panel()
        self.log_message("Nuevo pipeline creado")

def clear_canvas(self):
    """Limpia el canvas"""
    reply = QMessageBox.question(self, 'Limpiar Canvas', 
                                '¿Estás seguro de que quieres limpiar el canvas?\nSe perderán todos los nodos y conexiones.',
                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                QMessageBox.StandardButton.No)
    
    if reply == QMessageBox.StandardButton.Yes:
        self.pipeline_canvas.clear_canvas()
        self.properties_panel.node_configs.clear()
        self.properties_panel.current_dataframes.clear()
        self.properties_panel.clear_properties_panel()
        self.log_message("Canvas limpiado")
