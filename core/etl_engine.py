import polars as pl
from typing import Dict, Any, List, Optional, Tuple
import networkx as nx
from PyQt6.QtCore import QObject, pyqtSignal
import os
import pandas as pd
import json
import requests

class ETLEngine(QObject):
    # Señales
    execution_progress = pyqtSignal(str)  # Señal para informar del progreso
    execution_finished = pyqtSignal(bool, str)  # Señal para informar del resultado (éxito, mensaje)
    node_executed = pyqtSignal(int, object)  # Señal para informar que un nodo se ha ejecutado (id, dataframe)
    
    def __init__(self):
        super().__init__()
        self.pipeline = nx.DiGraph()
        self.node_dataframes = {}  # Almacena los dataframes de cada nodo
        self._stop_requested = False  # Bandera para detener ejecución
        
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
        """Ejecuta un nodo de origen y retorna un DataFrame de Polars."""
        self.execution_progress.emit(f"Ejecutando nodo de origen {node_id}...")
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')

        # Usar datos precargados si existen
        if 'dataframe' in config and isinstance(config['dataframe'], (pl.DataFrame, pd.DataFrame)):
            self.execution_progress.emit(f"Usando datos precargados en nodo {node_id}")
            df = config['dataframe']
            if isinstance(df, pd.DataFrame):
                df = pl.from_pandas(df)
            return self._apply_select_and_rename(df, config)

        try:
            if subtype == 'csv':
                path = config.get('path')
                if not path:
                    raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
                df = pl.read_csv(path)
                return self._apply_select_and_rename(df, config)

            elif subtype == 'excel':
                path = config.get('path')
                if not path:
                    raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
                # pl.read_excel puede no estar disponible en todas las versiones; fallback a pandas
                try:
                    df = pl.read_excel(path)
                except Exception:
                    pdf = pd.read_excel(path)
                    df = pl.from_pandas(pdf)
                return self._apply_select_and_rename(df, config)

            elif subtype == 'json':
                path = config.get('path')
                if not path:
                    raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Detectar estructura
                if isinstance(data, list):
                    df = pl.DataFrame(data)
                elif isinstance(data, dict):
                    # Si tiene 'data' o similar, intentar usarlo
                    key = 'data' if 'data' in data else None
                    if key:
                        df = pl.DataFrame(data[key])
                    else:
                        df = pl.DataFrame([data])
                else:
                    raise ValueError("Estructura JSON no soportada para conversión a DataFrame")
                return self._apply_select_and_rename(df, config)

            elif subtype == 'parquet':
                path = config.get('path')
                if not path:
                    raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
                df = pl.read_parquet(path)
                return self._apply_select_and_rename(df, config)

            elif subtype == 'database':
                db_type = config.get('db_type')
                host = config.get('host')
                port = config.get('port')
                user = config.get('user')
                password = config.get('password')
                database = config.get('database')
                query = config.get('query')
                if not query:
                    raise ValueError("Debe especificar una consulta SQL en la configuración del nodo de base de datos")

                conn_str = self._build_connection_string(db_type, host, port, user, password, database)
                self.execution_progress.emit(f"Leyendo desde base de datos ({db_type})...")
                df = self._read_sql(conn_str, query)
                return self._apply_select_and_rename(df, config)

            elif subtype == 'api':
                url = config.get('url')
                method = (config.get('method') or 'GET').upper()
                headers = self._parse_kv_string(config.get('headers')) if isinstance(config.get('headers'), str) else config.get('headers')
                params = self._parse_kv_string(config.get('params')) if isinstance(config.get('params'), str) else config.get('params')
                if not url:
                    raise ValueError("Debe especificar una URL para el origen API")
                self.execution_progress.emit(f"Llamando API {method} {url}...")
                resp = requests.request(method, url, headers=headers, params=params, timeout=60)
                resp.raise_for_status()
                try:
                    data = resp.json()
                except Exception:
                    # Intentar CSV si el contenido lo parece
                    try:
                        from io import BytesIO
                        df = pl.read_csv(BytesIO(resp.content))
                        return self._apply_select_and_rename(df, config)
                    except Exception:
                        raise ValueError("La respuesta de la API no es JSON ni CSV soportado")
                # Normalizar JSON
                if isinstance(data, list):
                    df = pl.DataFrame(data)
                elif isinstance(data, dict):
                    key = 'data' if 'data' in data else None
                    if key:
                        df = pl.DataFrame(data[key])
                    else:
                        df = pl.DataFrame([data])
                else:
                    raise ValueError("Estructura de respuesta de API no soportada")
                return self._apply_select_and_rename(df, config)

            else:
                raise ValueError(f"Tipo de origen desconocido o no soportado para nodo {node_id}")
        except Exception as e:
            self.execution_progress.emit(f"Error al ejecutar origen {node_id}: {e}")
            raise
        
    def execute_transform(self, node_id: int, df: pl.DataFrame) -> pl.DataFrame:
        """Ejecuta un nodo de transformación sobre el DataFrame de entrada."""
        self.execution_progress.emit(f"Ejecutando transformación en nodo {node_id}...")
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')

        result_df = df
        try:
            if subtype == 'filter':
                filter_expr = config.get('filter_expr')
                if filter_expr:
                    expr_str = filter_expr.strip()
                    if '>' in expr_str:
                        col, val = expr_str.split('>')
                        col = col.strip()
                        val = float(val.strip())
                        result_df = df.filter(pl.col(col) > val)
                    elif '<' in expr_str:
                        col, val = expr_str.split('<')
                        col = col.strip()
                        val = float(val.strip())
                        result_df = df.filter(pl.col(col) < val)
                    elif '==' in expr_str:
                        col, val = expr_str.split('==')
                        col = col.strip()
                        val = val.strip()
                        try:
                            val = float(val)
                            result_df = df.filter(pl.col(col) == val)
                        except Exception:
                            result_df = df.filter(pl.col(col) == val)
                    else:
                        self.execution_progress.emit(f"Expresión de filtro no soportada: {expr_str}")
                        result_df = df
                else:
                    self.execution_progress.emit(f"No se especificó expresión de filtro para nodo {node_id}")
                    result_df = df

            elif subtype == 'join':
                if 'join_cols' in config and 'other_dataframe' in config:
                    join_cols = [col.strip() for col in str(config['join_cols']).split(',') if col.strip()]
                    other_df = config['other_dataframe']
                    if isinstance(other_df, pd.DataFrame):
                        other_df = pl.from_pandas(other_df)
                    join_type = (config.get('join_type', 'Inner') or 'Inner').lower()

                    if join_type == 'inner':
                        result_df = df.join(other_df, on=join_cols, how='inner')
                    elif join_type == 'left':
                        result_df = df.join(other_df, on=join_cols, how='left')
                    elif join_type == 'right':
                        result_df = df.join(other_df, on=join_cols, how='right')
                    elif join_type == 'outer':
                        result_df = df.join(other_df, on=join_cols, how='outer')
                    else:
                        result_df = df.join(other_df, on=join_cols, how='inner')
                else:
                    self.execution_progress.emit(f"Faltan parámetros para realizar join en nodo {node_id}")
                    result_df = df

            elif subtype == 'aggregate':
                if 'group_by' in config and 'agg_funcs' in config:
                    group_cols = [col.strip() for col in str(config['group_by']).split(',') if col.strip()]
                    agg_exprs = []
                    for agg_expr in str(config['agg_funcs']).split(','):
                        if '(' in agg_expr and ')' in agg_expr:
                            func, col = agg_expr.strip().split('(')
                            col = col.replace(')', '').strip()
                            if func.lower() == 'sum':
                                agg_exprs.append(pl.sum(col))
                            elif func.lower() in ('avg', 'mean'):
                                agg_exprs.append(pl.mean(col))
                            elif func.lower() == 'min':
                                agg_exprs.append(pl.min(col))
                            elif func.lower() == 'max':
                                agg_exprs.append(pl.max(col))
                            elif func.lower() == 'count':
                                agg_exprs.append(pl.count())
                    if group_cols and agg_exprs:
                        result_df = df.group_by(group_cols).agg(agg_exprs)
                    else:
                        self.execution_progress.emit(f"Expresiones de agregación inválidas en nodo {node_id}")
                        result_df = df
                else:
                    self.execution_progress.emit(f"Faltan parámetros para agregación en nodo {node_id}")
                    result_df = df

            elif subtype == 'map':
                if 'map_expr' in config and config['map_expr']:
                    map_expr = str(config['map_expr']).strip()
                    if '=' in map_expr:
                        new_col, expr = map_expr.split('=', 1)
                        new_col = new_col.strip()
                        expr = expr.strip()
                        if '+' in expr:
                            cols = [c.strip() for c in expr.split('+')]
                            if len(cols) >= 2:
                                result_df = df.with_columns((pl.col(cols[0]) + pl.col(cols[1])).alias(new_col))
                        elif '-' in expr:
                            cols = [c.strip() for c in expr.split('-')]
                            if len(cols) >= 2:
                                result_df = df.with_columns((pl.col(cols[0]) - pl.col(cols[1])).alias(new_col))
                        elif '*' in expr:
                            cols = [c.strip() for c in expr.split('*')]
                            if len(cols) >= 2:
                                result_df = df.with_columns((pl.col(cols[0]) * pl.col(cols[1])).alias(new_col))
                        elif '/' in expr:
                            cols = [c.strip() for c in expr.split('/')]
                            if len(cols) >= 2:
                                result_df = df.with_columns((pl.col(cols[0]) / pl.col(cols[1])).alias(new_col))
                        else:
                            self.execution_progress.emit(f"Expresión de mapeo no soportada en nodo {node_id}")
                            result_df = df
                    else:
                        self.execution_progress.emit(f"Expresión de mapeo no soportada en nodo {node_id}")
                        result_df = df
                else:
                    self.execution_progress.emit(f"No se especificó expresión de mapeo para nodo {node_id}")
                    result_df = df
            else:
                self.execution_progress.emit(f"Tipo de transformación desconocido para nodo {node_id}")
                result_df = df

            # Post-procesamiento: selección y renombrado
            result_df = self._apply_select_and_rename(result_df, config)
            return result_df
        except Exception as e:
            self.execution_progress.emit(f"Error en transformación nodo {node_id}: {e}")
            import traceback
            traceback.print_exc()
            return df
        
    def execute_destination(self, node_id: int, df: pl.DataFrame):
        """Ejecuta un nodo de destino para guardar o enviar el DataFrame."""
        self.execution_progress.emit(f"Ejecutando nodo de destino {node_id}...")
        config = self.pipeline.nodes[node_id]['config']
        subtype = config.get('subtype')

        # Post-procesamiento opcional en destino (selección/renombrado)
        df_to_write = self._apply_select_and_rename(df, config)

        if subtype in ('csv', 'excel', 'json', 'parquet'):
            path = config.get('path')
            if not path:
                self.execution_progress.emit(f"No se especificó ruta de destino para nodo {node_id}")
                raise ValueError(f"No se especificó ruta de destino para nodo {node_id}")
            try:
                if os.path.isdir(path):
                    raise ValueError(f"La ruta especificada es un directorio: {path}")
                out_dir = os.path.dirname(path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)

                # Determinar formato: si hay 'format' úsalo, si no, según subtipo
                default_fmt = 'excel' if subtype == 'excel' else ('json' if subtype == 'json' else ('parquet' if subtype == 'parquet' else 'csv'))
                format_type = (config.get('format') or default_fmt).lower()
                self.execution_progress.emit(f"Guardando datos en {path} como {format_type.upper()}...")

                if format_type == 'csv':
                    df_to_write.write_csv(path)
                elif format_type == 'parquet':
                    df_to_write.write_parquet(path)
                elif format_type == 'json':
                    df_to_write.write_json(path)
                elif format_type == 'excel':
                    # Polars no tiene write_excel estable: usar pandas
                    pdf = df_to_write.to_pandas()
                    try:
                        pdf.to_excel(path, index=False)
                    except Exception as e:
                        raise ValueError(f"Error escribiendo Excel: {e}")
                else:
                    df_to_write.write_csv(path)

                self.execution_progress.emit(f"Datos guardados en {path}")
            except Exception as e:
                self.execution_progress.emit(f"Error al guardar datos: {e}")
                import traceback
                traceback.print_exc()
                raise

        elif subtype == 'database':
            # Escritura a base de datos con pandas + sqlalchemy
            db_type = config.get('db_type')
            host = config.get('host')
            port = config.get('port')
            user = config.get('user')
            password = config.get('password')
            database = config.get('database')
            table = config.get('table')
            if not table:
                raise ValueError("Debe especificar el nombre de la tabla de destino ('table')")
            conn_str = self._build_connection_string(db_type, host, port, user, password, database)
            self.execution_progress.emit(f"Escribiendo datos en base de datos tabla {table}...")
            pdf = df_to_write.to_pandas()
            try:
                from sqlalchemy import create_engine
                engine = create_engine(conn_str)
                if_exists = (config.get('if_exists') or 'replace').lower()
                pdf.to_sql(table, engine, if_exists=if_exists, index=False)
                self.execution_progress.emit(f"Datos escritos en la tabla {table}")
            except Exception as e:
                self.execution_progress.emit(f"Error al escribir en base de datos: {e}")
                import traceback
                traceback.print_exc()
                raise

        elif subtype == 'api':
            # Envío de datos a API en JSON (por lotes si es grande)
            url = config.get('url')
            method = (config.get('method') or 'POST').upper()
            headers = self._parse_kv_string(config.get('headers')) if isinstance(config.get('headers'), str) else config.get('headers')
            params = self._parse_kv_string(config.get('params')) if isinstance(config.get('params'), str) else config.get('params')
            if not url:
                raise ValueError("Debe especificar la URL para el destino API")
            self.execution_progress.emit(f"Enviando datos a API {method} {url}...")
            data_records = df_to_write.to_dicts()
            batch_size = int(config.get('batch_size') or 500)
            try:
                for i in range(0, len(data_records), batch_size):
                    if self._stop_requested:
                        raise KeyboardInterrupt("Ejecución detenida por el usuario")
                    batch = data_records[i:i+batch_size]
                    resp = requests.request(method, url, headers=headers, params=params, json=batch, timeout=60)
                    if not resp.ok:
                        raise ValueError(f"Error de API (status {resp.status_code}): {resp.text}")
                    self.execution_progress.emit(f"Lote {i//batch_size + 1} enviado ({len(batch)} registros)")
                self.execution_progress.emit("Envío a API completado")
            except Exception as e:
                self.execution_progress.emit(f"Error al enviar a API: {e}")
                raise

        else:
            self.execution_progress.emit(f"Tipo de destino desconocido para nodo {node_id}")
            
    def execute_pipeline(self, node_configs=None):
        """Execute the entire pipeline"""
        try:
            self.execution_progress.emit("Iniciando ejecución del pipeline...")
            self._stop_requested = False
            
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
                if self._stop_requested:
                    self.execution_progress.emit("Ejecución detenida por el usuario")
                    self.execution_finished.emit(False, "Ejecución detenida por el usuario")
                    return False
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

    # Utilidades
    def request_stop(self):
        """Solicita detener la ejecución del pipeline lo más pronto posible."""
        self._stop_requested = True

    def _parse_kv_string(self, s: Optional[str]) -> Optional[Dict[str, str]]:
        """Convierte un string tipo 'k1:v1,k2:v2' en dict. Ignora entradas malformadas."""
        if s is None:
            return None
        if isinstance(s, dict):
            return s  # ya es dict
        result: Dict[str, str] = {}
        try:
            pairs = [p for p in str(s).split(',') if p.strip()]
            for p in pairs:
                if ':' in p:
                    k, v = p.split(':', 1)
                    result[k.strip()] = v.strip()
        except Exception:
            pass
        return result

    def _apply_select_and_rename(self, df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
        """Aplica selección y renombrado de columnas según 'output_cols' y 'column_rename'."""
        result = df
        try:
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
        except Exception as e:
            self.execution_progress.emit(f"Aviso: error aplicando selección/renombrado: {e}")
        return result

    def _build_connection_string(self, db_type: Optional[str], host: Optional[str], port: Optional[str], user: Optional[str], password: Optional[str], database: Optional[str]) -> str:
        db_type = (db_type or '').lower()
        if db_type == 'sqlite':
            # Para SQLite el 'host' se usa como path del archivo si viene en 'database'
            if database and (database.endswith('.db') or database.endswith('.sqlite3')):
                return f"sqlite:///{database}"
            elif host and (host.endswith('.db') or host.endswith('.sqlite3')):
                return f"sqlite:///{host}"
            else:
                raise ValueError("Para SQLite especifique el path del archivo en 'database' o 'host'")
        elif db_type in ('postgresql', 'postgres'):
            return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        elif db_type == 'mysql':
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        elif db_type in ('mssql', 'sql server', 'sqlserver'):
            # Requiere mssql+pyodbc con DSN apropiado; se asume driver por defecto
            return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Tipo de base de datos no soportado: {db_type}")

    def _read_sql(self, conn_str: str, query: str) -> pl.DataFrame:
        """Lee datos SQL intentando con connectorx y haciendo fallback a pandas+sqlalchemy."""
        try:
            import connectorx as cx
            pdf = cx.read_sql(conn_str, query)
            # connectorx puede devolver pandas DataFrame
            if isinstance(pdf, pd.DataFrame):
                return pl.from_pandas(pdf)
            # Si devuelve un PyArrow Table
            try:
                import pyarrow as pa
                if isinstance(pdf, pa.Table):
                    return pl.from_arrow(pdf)
            except Exception:
                pass
            # Intentar crear polars directamente si fuese soportado
            return pl.DataFrame(pdf)
        except Exception:
            from sqlalchemy import create_engine
            engine = create_engine(conn_str)
            pdf = pd.read_sql_query(query, engine)
            return pl.from_pandas(pdf)