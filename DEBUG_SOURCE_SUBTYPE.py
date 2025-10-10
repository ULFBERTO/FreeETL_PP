# Corrección para debug del error de subtype en execute_source
# Reemplazar el método execute_source en etl_engine.py

def execute_source(self, node_id: int) -> pl.DataFrame:
    """Ejecuta un nodo de origen y retorna un DataFrame de Polars."""
    self.execution_progress.emit(f"Ejecutando nodo de origen {node_id}...")
    config = self.pipeline.nodes[node_id]['config']
    subtype = config.get('subtype')
    
    # DEBUG: Imprimir información del nodo para diagnosticar
    self.execution_progress.emit(f"DEBUG - Nodo {node_id} config: {config}")
    self.execution_progress.emit(f"DEBUG - Subtype detectado: '{subtype}' (tipo: {type(subtype)})")
    
    # Normalizar subtype para evitar problemas de mayúsculas/minúsculas o espacios
    if subtype:
        subtype = str(subtype).strip().lower()
        self.execution_progress.emit(f"DEBUG - Subtype normalizado: '{subtype}'")

    # Usar datos precargados si existen
    if 'dataframe' in config and isinstance(config['dataframe'], (pl.DataFrame, pd.DataFrame)):
        self.execution_progress.emit(f"Usando datos precargados en nodo {node_id}")
        df = config['dataframe']
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        res = self._apply_select_and_rename(df, config)
        try:
            self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
        except Exception:
            pass
        return res

    try:
        if subtype == 'csv':
            path = config.get('path')
            if not path:
                raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
            df = pl.read_csv(path)
            res = self._apply_select_and_rename(df, config)
            try:
                self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
            except Exception:
                pass
            return res

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
            res = self._apply_select_and_rename(df, config)
            try:
                self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
            except Exception:
                pass
            return res

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
            res = self._apply_select_and_rename(df, config)
            try:
                self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
            except Exception:
                pass
            return res

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
            try:
                if (db_type or '').lower() == 'mysql':
                    # Crear engine con (posible) SSL según config
                    engine = self._make_sqlalchemy_engine(db_type, conn_str, config)
                    pdf = pd.read_sql_query(query, engine)
                else:
                    # Camino estándar
                    df = self._read_sql(conn_str, query)
                    res = self._apply_select_and_rename(df, config)
                    try:
                        self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
                    except Exception:
                        pass
                    return res
            except Exception as e:
                # Reintentar con modo SSL opuesto si parece error de SSL y es MySQL
                if (db_type or '').lower() == 'mysql' and self._should_retry_ssl(e, config):
                    try:
                        retry_mode = 'DISABLED' if self._was_ssl_enabled(config) else 'REQUIRED'
                        engine = self._make_sqlalchemy_engine(db_type, conn_str, config, ssl_mode_override=retry_mode)
                        pdf = pd.read_sql_query(query, engine)
                        self.execution_progress.emit(f"Reintento MySQL con SSL='{retry_mode}' exitoso")
                    except Exception:
                        self.execution_progress.emit(f"Fallo reintento MySQL cambiando SSL: {e}")
                        raise
                else:
                    raise
            # Convertir a Polars y aplicar selección/renombrado
            df = pl.from_pandas(pdf)
            res = self._apply_select_and_rename(df, config)
            try:
                self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
            except Exception:
                pass
            return res

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
                    res = self._apply_select_and_rename(df, config)
                    try:
                        self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
                    except Exception:
                        pass
                    return res
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

        # NUEVO: Casos adicionales para manejar variaciones comunes
        elif subtype in ['archivo csv', 'csv file', 'csvfile']:
            # Tratar como CSV
            path = config.get('path')
            if not path:
                raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
            df = pl.read_csv(path)
            res = self._apply_select_and_rename(df, config)
            return res
            
        elif subtype in ['archivo excel', 'excel file', 'excelfile']:
            # Tratar como Excel
            path = config.get('path')
            if not path:
                raise ValueError(f"No se especificó ruta de archivo para el nodo {node_id}")
            try:
                df = pl.read_excel(path)
            except Exception:
                pdf = pd.read_excel(path)
                df = pl.from_pandas(pdf)
            res = self._apply_select_and_rename(df, config)
            return res
            
        elif subtype in ['base de datos', 'database connection', 'db']:
            # Tratar como base de datos
            return self._handle_database_source(node_id, config)

        else:
            # Información detallada del error
            available_subtypes = ['csv', 'excel', 'json', 'parquet', 'database', 'api']
            error_msg = f"Tipo de origen desconocido o no soportado para nodo {node_id}.\n"
            error_msg += f"Subtype recibido: '{subtype}' (tipo: {type(subtype)})\n"
            error_msg += f"Subtipos válidos: {available_subtypes}\n"
            error_msg += f"Configuración completa del nodo: {config}"
            
            self.execution_progress.emit(error_msg)
            raise ValueError(error_msg)
            
    except Exception as e:
        self.execution_progress.emit(f"Error al ejecutar origen {node_id}: {e}")
        raise

def _handle_database_source(self, node_id: int, config: dict):
    """Maneja fuentes de base de datos de forma separada"""
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
    
    try:
        if (db_type or '').lower() == 'mysql':
            engine = self._make_sqlalchemy_engine(db_type, conn_str, config)
            pdf = pd.read_sql_query(query, engine)
            df = pl.from_pandas(pdf)
        else:
            df = self._read_sql(conn_str, query)
        
        res = self._apply_select_and_rename(df, config)
        try:
            self.execution_progress.emit(f"Nodo origen {node_id} columnas: {list(res.columns)}")
        except Exception:
            pass
        return res
        
    except Exception as e:
        if (db_type or '').lower() == 'mysql' and self._should_retry_ssl(e, config):
            try:
                retry_mode = 'DISABLED' if self._was_ssl_enabled(config) else 'REQUIRED'
                engine = self._make_sqlalchemy_engine(db_type, conn_str, config, ssl_mode_override=retry_mode)
                pdf = pd.read_sql_query(query, engine)
                df = pl.from_pandas(pdf)
                res = self._apply_select_and_rename(df, config)
                self.execution_progress.emit(f"Reintento MySQL con SSL='{retry_mode}' exitoso")
                return res
            except Exception:
                self.execution_progress.emit(f"Fallo reintento MySQL cambiando SSL: {e}")
                raise
        else:
            raise
