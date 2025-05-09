import polars as pl
from typing import Dict, Any, List, Optional, Tuple
import networkx as nx
from PyQt6.QtCore import QObject, pyqtSignal
import os

class ETLEngine(QObject):
    # Señales
    execution_progress = pyqtSignal(str)  # Señal para informar del progreso
    execution_finished = pyqtSignal(bool, str)  # Señal para informar del resultado (éxito, mensaje)
    node_executed = pyqtSignal(int, object)  # Señal para informar que un nodo se ha ejecutado (id, dataframe)
    
    def __init__(self):
        super().__init__()
        self.pipeline = nx.DiGraph()
        self.node_dataframes = {}  # Almacena los dataframes de cada nodo
        
    def set_pipeline(self, pipeline: nx.DiGraph, node_configs: Dict[int, Dict[str, Any]]):
        """Establece el pipeline a partir del grafo visual y las configuraciones"""
        self.pipeline = pipeline.copy()
        
        # Añadir configuraciones a los nodos
        for node_id, config in node_configs.items():
            if node_id in self.pipeline.nodes:
                self.pipeline.nodes[node_id]['config'] = config
                
    def add_source(self, node_id: int, config: Dict[str, Any]):
        """Add a source node to the pipeline"""
        self.pipeline.add_node(node_id, type='source', config=config)
        
    def add_transform(self, node_id: int, config: Dict[str, Any]):
        """Add a transform node to the pipeline"""
        self.pipeline.add_node(node_id, type='transform', config=config)
        
    def add_destination(self, node_id: int, config: Dict[str, Any]):
        """Add a destination node to the pipeline"""
        self.pipeline.add_node(node_id, type='destination', config=config)
        
    def add_connection(self, source_id: int, target_id: int):
        """Add a connection between nodes"""
        self.pipeline.add_edge(source_id, target_id)
        
    def execute_source(self, node_id: int) -> pl.DataFrame:
        """Execute a source node and return the resulting DataFrame"""
        self.execution_progress.emit(f"Ejecutando nodo de origen {node_id}...")
        
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')
        
        # Si ya hay un dataframe en el nodo, usarlo
        if 'dataframe' in config:
            self.execution_progress.emit(f"Usando datos precargados en nodo {node_id}")
            return config['dataframe']
        
        # De lo contrario, cargar según el tipo
        if subtype == 'csv':
            if 'path' in config:
                try:
                    df = pl.read_csv(config['path'])
                    # Aplicar mapeo de columnas si existe
                    if 'column_mapping' in config:
                        column_map = config['column_mapping']
                        if column_map:
                            df = df.rename(column_map)
                    return df
                except Exception as e:
                    self.execution_progress.emit(f"Error al cargar CSV: {e}")
                    raise
            else:
                self.execution_progress.emit(f"Nodo {node_id}: No se especificó ruta de archivo")
                raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
                
        elif subtype == 'database':
            # TODO: Implementar conexión a base de datos
            self.execution_progress.emit(f"Conexión a base de datos no implementada para nodo {node_id}")
            raise NotImplementedError("Conexión a base de datos no implementada")
            
        elif subtype == 'api':
            # TODO: Implementar conexión a API
            self.execution_progress.emit(f"Conexión a API no implementada para nodo {node_id}")
            raise NotImplementedError("Conexión a API no implementada")
            
        self.execution_progress.emit(f"Tipo de origen desconocido para nodo {node_id}")
        raise ValueError(f"Tipo de origen desconocido para nodo {node_id}")
        
    def execute_transform(self, node_id: int, df: pl.DataFrame) -> pl.DataFrame:
        """Execute a transform node on the input DataFrame"""
        self.execution_progress.emit(f"Ejecutando transformación en nodo {node_id}...")
        
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')
        
        # Ejecutar transformación según el tipo
        if subtype == 'filter':
            filter_expr = config.get('filter_expr')
            if filter_expr:
                # Convertir expresión de texto a expresión de Polars
                try:
                    # Creamos una expresión simple para filtrar
                    # Ejemplo: "col('edad') > 25" 
                    expr_str = filter_expr.strip()
                    
                    # Evaluar la expresión (esto es simplificado, en producción sería más robusto)
                    if '>' in expr_str:
                        col, val = expr_str.split('>')
                        col = col.strip()
                        val = float(val.strip())
                        return df.filter(pl.col(col) > val)
                    elif '<' in expr_str:
                        col, val = expr_str.split('<')
                        col = col.strip()
                        val = float(val.strip())
                        return df.filter(pl.col(col) < val)
                    elif '==' in expr_str:
                        col, val = expr_str.split('==')
                        col = col.strip()
                        val = val.strip()
                        # Intentar convertir a número si es posible
                        try:
                            val = float(val)
                            return df.filter(pl.col(col) == val)
                        except:
                            # Es un string
                            return df.filter(pl.col(col) == val)
                    else:
                        self.execution_progress.emit(f"Expresión de filtro no soportada: {expr_str}")
                        return df
                except Exception as e:
                    self.execution_progress.emit(f"Error al aplicar filtro: {e}")
                    return df
            else:
                self.execution_progress.emit(f"No se especificó expresión de filtro para nodo {node_id}")
                return df
                
        elif subtype == 'join':
            # Para simplificar, asumimos que ya existen los datos de ambos lados en el nodo
            # En una implementación completa, habría que obtener los datos del otro nodo
            if 'join_cols' in config and 'other_dataframe' in config:
                join_cols = [col.strip() for col in config['join_cols'].split(',')]
                other_df = config['other_dataframe']
                join_type = config.get('join_type', 'Inner').lower()
                
                try:
                    # Realizar la unión según el tipo especificado
                    if join_type == 'inner':
                        result_df = df.join(other_df, on=join_cols, how='inner')
                    elif join_type == 'left':
                        result_df = df.join(other_df, on=join_cols, how='left')
                    elif join_type == 'right':
                        result_df = df.join(other_df, on=join_cols, how='right')
                    elif join_type == 'outer':
                        result_df = df.join(other_df, on=join_cols, how='outer')
                    else:
                        # Por defecto, inner join
                        result_df = df.join(other_df, on=join_cols, how='inner')
                    
                    # Aplicar selección de columnas de salida si está especificado
                    if 'output_cols' in config and config['output_cols'].strip():
                        output_cols = [col.strip() for col in config['output_cols'].split(',')]
                        
                        # Procesar los nombres de columnas que pueden tener prefijos de origen
                        processed_cols = []
                        for col in output_cols:
                            # Si el nombre tiene formato "OrigenX.nombre_columna", extraer solo el nombre
                            if col.startswith("Origen1.") or col.startswith("Origen2."):
                                col_name = col.split(".", 1)[1]
                                processed_cols.append(col_name)
                            else:
                                processed_cols.append(col)
                        
                        # Verificar que todas las columnas existen
                        valid_cols = [col for col in processed_cols if col in result_df.columns]
                        if valid_cols:
                            self.execution_progress.emit(f"Seleccionando columnas de salida: {', '.join(valid_cols)}")
                            result_df = result_df.select(valid_cols)
                        else:
                            self.execution_progress.emit(f"Ninguna columna válida encontrada en la selección")
                    
                    # Aplicar renombrado de columnas si está especificado
                    if 'column_rename' in config and config['column_rename'].strip():
                        rename_pairs = config['column_rename'].split(',')
                        rename_dict = {}
                        
                        for pair in rename_pairs:
                            if ':' in pair:
                                old_name, new_name = pair.split(':', 1)
                                old_name = old_name.strip()
                                new_name = new_name.strip()
                                
                                # Procesar nombres con prefijos de origen
                                if old_name.startswith("Origen1.") or old_name.startswith("Origen2."):
                                    old_name = old_name.split(".", 1)[1]
                                
                                if old_name in result_df.columns:
                                    rename_dict[old_name] = new_name
                        
                        if rename_dict:
                            self.execution_progress.emit(f"Renombrando columnas: {rename_dict}")
                            result_df = result_df.rename(rename_dict)
                    
                    return result_df
                except Exception as e:
                    self.execution_progress.emit(f"Error al realizar join: {e}")
                    import traceback
                    traceback.print_exc()
                    return df
            else:
                self.execution_progress.emit(f"Faltan parámetros para realizar join en nodo {node_id}")
                return df
                
        elif subtype == 'aggregate':
            # Implementación simplificada de agregación
            if 'group_by' in config and 'agg_funcs' in config:
                try:
                    group_cols = [col.strip() for col in config['group_by'].split(',')]
                    
                    # Parsear expresiones de agregación (simplificado)
                    agg_exprs = []
                    for agg_expr in config['agg_funcs'].split(','):
                        if '(' in agg_expr and ')' in agg_expr:
                            func, col = agg_expr.strip().split('(')
                            col = col.replace(')', '').strip()
                            
                            if func.lower() == 'sum':
                                agg_exprs.append(pl.sum(col))
                            elif func.lower() == 'avg' or func.lower() == 'mean':
                                agg_exprs.append(pl.mean(col))
                            elif func.lower() == 'min':
                                agg_exprs.append(pl.min(col))
                            elif func.lower() == 'max':
                                agg_exprs.append(pl.max(col))
                            elif func.lower() == 'count':
                                agg_exprs.append(pl.count(col))
                    
                    if group_cols and agg_exprs:
                        return df.group_by(group_cols).agg(agg_exprs)
                    else:
                        self.execution_progress.emit(f"Expresiones de agregación inválidas en nodo {node_id}")
                        return df
                except Exception as e:
                    self.execution_progress.emit(f"Error al realizar agregación: {e}")
                    return df
            else:
                self.execution_progress.emit(f"Faltan parámetros para agregación en nodo {node_id}")
                return df
                
        elif subtype == 'map':
            # Implementación simplificada de mapeo/columnas calculadas
            if 'map_expr' in config:
                map_expr = config['map_expr'].strip()
                try:
                    # Detectar formato "nueva_col = operación"
                    if '=' in map_expr:
                        new_col, expr = map_expr.split('=', 1)
                        new_col = new_col.strip()
                        expr = expr.strip()
                        
                        # Detectar operaciones aritméticas simples
                        if '+' in expr:
                            cols = [c.strip() for c in expr.split('+')]
                            # Implementación simplificada: sumamos las primeras dos columnas
                            if len(cols) >= 2:
                                return df.with_columns(
                                    (pl.col(cols[0]) + pl.col(cols[1])).alias(new_col)
                                )
                        elif '-' in expr:
                            cols = [c.strip() for c in expr.split('-')]
                            if len(cols) >= 2:
                                return df.with_columns(
                                    (pl.col(cols[0]) - pl.col(cols[1])).alias(new_col)
                                )
                        elif '*' in expr:
                            cols = [c.strip() for c in expr.split('*')]
                            if len(cols) >= 2:
                                return df.with_columns(
                                    (pl.col(cols[0]) * pl.col(cols[1])).alias(new_col)
                                )
                        elif '/' in expr:
                            cols = [c.strip() for c in expr.split('/')]
                            if len(cols) >= 2:
                                return df.with_columns(
                                    (pl.col(cols[0]) / pl.col(cols[1])).alias(new_col)
                                )
                    
                    self.execution_progress.emit(f"Expresión de mapeo no soportada en nodo {node_id}")
                    return df
                except Exception as e:
                    self.execution_progress.emit(f"Error al aplicar mapeo: {e}")
                    return df
            else:
                self.execution_progress.emit(f"No se especificó expresión de mapeo para nodo {node_id}")
                return df
                
        self.execution_progress.emit(f"Tipo de transformación desconocido para nodo {node_id}")
        return df
        
    def execute_destination(self, node_id: int, df: pl.DataFrame):
        """Execute a destination node to save the DataFrame"""
        self.execution_progress.emit(f"Ejecutando nodo de destino {node_id}...")
        
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')
        
        # Guardar según el tipo de destino
        if subtype == 'csv':
            if 'path' in config and config['path']:
                try:
                    path = config['path']
                    # Verificar que la ruta no es un directorio
                    if os.path.isdir(path):
                        self.execution_progress.emit(f"Error: La ruta especificada es un directorio: {path}")
                        raise ValueError(f"La ruta especificada es un directorio: {path}")
                        
                    format_type = config.get('format', 'CSV').lower()
                    
                    # Asegurarse de que el directorio existe
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    
                    self.execution_progress.emit(f"Guardando datos en {path}...")
                    
                    if format_type == 'csv':
                        df.write_csv(path)
                    elif format_type == 'excel':
                        df.write_excel(path)
                    elif format_type == 'parquet':
                        df.write_parquet(path)
                    elif format_type == 'json':
                        df.write_json(path)
                    else:
                        # Por defecto, CSV
                        df.write_csv(path)
                        
                    self.execution_progress.emit(f"Datos guardados en {path}")
                except Exception as e:
                    self.execution_progress.emit(f"Error al guardar datos: {e}")
                    import traceback
                    traceback.print_exc()
                    raise
            else:
                self.execution_progress.emit(f"No se especificó ruta de destino para nodo {node_id}")
                raise ValueError(f"No se especificó ruta de destino para nodo {node_id}")
        # SOPORTE PARA DESTINO EXCEL
        elif subtype == 'excel':
            if 'path' in config and config['path']:
                try:
                    path = config['path']
                    if os.path.isdir(path):
                        self.execution_progress.emit(f"Error: La ruta especificada es un directorio: {path}")
                        raise ValueError(f"La ruta especificada es un directorio: {path}")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    self.execution_progress.emit(f"Guardando datos en {path}...")
                    df.write_excel(path)
                    self.execution_progress.emit(f"Datos guardados en {path}")
                except Exception as e:
                    self.execution_progress.emit(f"Error al guardar datos: {e}")
                    import traceback
                    traceback.print_exc()
                    raise
            else:
                self.execution_progress.emit(f"No se especificó ruta de destino para nodo {node_id}")
                raise ValueError(f"No se especificó ruta de destino para nodo {node_id}")
                
        elif subtype == 'database':
            # TODO: Implementar escritura a base de datos
            self.execution_progress.emit(f"Escritura a base de datos no implementada para nodo {node_id}")
            
        elif subtype == 'api':
            # TODO: Implementar envío a API
            self.execution_progress.emit(f"Envío a API no implementada para nodo {node_id}")
            
        else:
            self.execution_progress.emit(f"Tipo de destino desconocido para nodo {node_id}")
            
    def execute_pipeline(self, node_configs=None):
        """Execute the entire pipeline"""
        try:
            self.execution_progress.emit("Iniciando ejecución del pipeline...")
            
            # Validar pipeline
            if not nx.is_directed_acyclic_graph(self.pipeline):
                self.execution_progress.emit("Error: El pipeline contiene ciclos")
                self.execution_finished.emit(False, "El pipeline contiene ciclos")
                return False
                
            # Si se proporcionaron configuraciones, actualizarlas
            if node_configs:
                for node_id, config in node_configs.items():
                    if node_id in self.pipeline.nodes:
                        self.pipeline.nodes[node_id]['config'] = config
                
            # Get topological sort of nodes
            sorted_nodes = list(nx.topological_sort(self.pipeline))
            
            # Verificar que hay al menos un nodo fuente
            has_source = False
            for node_id in sorted_nodes:
                if self.pipeline.nodes[node_id]['type'] == 'source':
                    has_source = True
                    break
                    
            if not has_source:
                self.execution_progress.emit("Error: El pipeline no tiene nodos de origen")
                self.execution_finished.emit(False, "El pipeline no tiene nodos de origen")
                return False
                
            # Execute pipeline
            node_results = {}
            for node_id in sorted_nodes:
                node_type = self.pipeline.nodes[node_id]['type']
                
                try:
                    if node_type == 'source':
                        df = self.execute_source(node_id)
                        node_results[node_id] = df
                        self.node_dataframes[node_id] = df
                        self.node_executed.emit(node_id, df)
                        
                    elif node_type == 'transform':
                        # Obtener dataframes de entrada
                        input_dfs = []
                        for pred in self.pipeline.predecessors(node_id):
                            if pred in node_results:
                                input_dfs.append(node_results[pred])
                                
                        if not input_dfs:
                            self.execution_progress.emit(f"Error: Nodo {node_id} no tiene entradas")
                            continue
                            
                        # Para simplificar, usamos solo el primer dataframe como entrada principal
                        input_df = input_dfs[0]
                        
                        # Si es un nodo de unión y hay más de una entrada, guardar la segunda como "other_dataframe"
                        config = self.pipeline.nodes[node_id]['config']
                        if config.get('subtype') == 'join' and len(input_dfs) > 1:
                            config['other_dataframe'] = input_dfs[1]
                            
                        # Ejecutar transformación
                        result_df = self.execute_transform(node_id, input_df)
                        
                        # Asegurarse que el resultado sea válido
                        if result_df is not None:
                            node_results[node_id] = result_df
                            self.node_dataframes[node_id] = result_df
                            self.node_executed.emit(node_id, result_df)
                        
                    elif node_type == 'destination':
                        # Obtener dataframe de entrada
                        preds = list(self.pipeline.predecessors(node_id))
                        if not preds or preds[0] not in node_results:
                            self.execution_progress.emit(f"Error: Nodo destino {node_id} no tiene entrada válida")
                            continue
                            
                        # Asegurar que usamos el dataframe actualizado más reciente
                        source_id = preds[0]
                        input_df = node_results[source_id]
                        
                        # Actualizamos la configuración del nodo destino con el dataframe actualizado
                        self.pipeline.nodes[node_id]['config']['dataframe'] = input_df
                        
                        # Ejecutar el nodo destino con el dataframe actualizado
                        self.execute_destination(node_id, input_df)
                        
                        # Guardar el dataframe en el nodo destino también
                        self.node_dataframes[node_id] = input_df
                        self.node_executed.emit(node_id, input_df)
                        
                except Exception as e:
                    self.execution_progress.emit(f"Error en nodo {node_id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    self.execution_finished.emit(False, f"Error en nodo {node_id}: {str(e)}")
                    return False
                    
            self.execution_progress.emit("Pipeline ejecutado correctamente")
            self.execution_finished.emit(True, "Pipeline ejecutado correctamente")
            return node_results
            
        except Exception as e:
            self.execution_progress.emit(f"Error al ejecutar pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            self.execution_finished.emit(False, f"Error al ejecutar pipeline: {str(e)}")
            return False 