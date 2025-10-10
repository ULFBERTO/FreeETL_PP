# Nuevos nodos de transformación para ETLEngine
# Agregar estos casos elif al método execute_transform() en etl_engine.py
# Insertar antes de la línea "else:" que maneja tipos desconocidos

"""
            elif subtype == 'sort':
                # Ordenamiento de datos
                sort_cols = config.get('sort_columns', [])
                if isinstance(sort_cols, str):
                    sort_cols = [c.strip() for c in sort_cols.split(',') if c.strip()]
                if sort_cols:
                    try:
                        result_df = result_df.sort(sort_cols)
                    except Exception as e:
                        self.execution_progress.emit(f"Error en ordenamiento: {e}")
                        result_df = df
                else:
                    self.execution_progress.emit(f"No se especificaron columnas para ordenar en nodo {node_id}")
                    result_df = df

            elif subtype == 'unique':
                # Eliminar duplicados
                subset_cols = config.get('subset_columns')
                keep = config.get('keep', 'first')
                try:
                    if subset_cols:
                        if isinstance(subset_cols, str):
                            subset_cols = [c.strip() for c in subset_cols.split(',') if c.strip()]
                        valid_cols = [c for c in subset_cols if c in result_df.columns]
                        if valid_cols:
                            result_df = result_df.unique(subset=valid_cols, keep=keep)
                        else:
                            result_df = result_df.unique(keep=keep)
                    else:
                        result_df = result_df.unique(keep=keep)
                except Exception as e:
                    self.execution_progress.emit(f"Error eliminando duplicados: {e}")
                    result_df = df

            elif subtype == 'limit':
                # Limitar número de filas
                n_rows = config.get('n_rows')
                offset = config.get('offset', 0)
                try:
                    if n_rows and isinstance(n_rows, (int, str)):
                        n = int(n_rows)
                        if offset > 0:
                            result_df = result_df.slice(offset, n)
                        else:
                            result_df = result_df.head(n)
                except Exception as e:
                    self.execution_progress.emit(f"Error limitando filas: {e}")
                    result_df = df

            elif subtype == 'fill_nulls':
                # Rellenar valores nulos
                fill_config = config.get('fill_config', {})
                try:
                    strategy = fill_config.get('strategy', 'value')
                    columns = fill_config.get('columns')
                    value = fill_config.get('value')
                    
                    if isinstance(columns, str):
                        columns = [c.strip() for c in columns.split(',') if c.strip()]
                    
                    if strategy == 'value' and value is not None:
                        if columns:
                            exprs = [pl.col(c).fill_null(value) for c in columns if c in result_df.columns]
                            if exprs:
                                result_df = result_df.with_columns(exprs)
                        else:
                            result_df = result_df.fill_null(value)
                    elif strategy == 'forward':
                        if columns:
                            exprs = [pl.col(c).forward_fill() for c in columns if c in result_df.columns]
                            if exprs:
                                result_df = result_df.with_columns(exprs)
                        else:
                            result_df = result_df.fill_null(strategy="forward")
                    elif strategy == 'backward':
                        if columns:
                            exprs = [pl.col(c).backward_fill() for c in columns if c in result_df.columns]
                            if exprs:
                                result_df = result_df.with_columns(exprs)
                        else:
                            result_df = result_df.fill_null(strategy="backward")
                except Exception as e:
                    self.execution_progress.emit(f"Error rellenando nulos: {e}")
                    result_df = df

            elif subtype == 'drop_nulls':
                # Eliminar filas con valores nulos
                subset_cols = config.get('subset_columns')
                try:
                    if subset_cols:
                        if isinstance(subset_cols, str):
                            subset_cols = [c.strip() for c in subset_cols.split(',') if c.strip()]
                        valid_cols = [c for c in subset_cols if c in result_df.columns]
                        if valid_cols:
                            result_df = result_df.drop_nulls(subset=valid_cols)
                        else:
                            result_df = result_df.drop_nulls()
                    else:
                        result_df = result_df.drop_nulls()
                except Exception as e:
                    self.execution_progress.emit(f"Error eliminando nulos: {e}")
                    result_df = df

            elif subtype == 'select_columns':
                # Seleccionar columnas específicas
                columns = config.get('columns')
                try:
                    if isinstance(columns, str):
                        columns = [c.strip() for c in columns.split(',') if c.strip()]
                    if isinstance(columns, list) and columns:
                        valid_cols = [c for c in columns if c in result_df.columns]
                        if valid_cols:
                            result_df = result_df.select(valid_cols)
                except Exception as e:
                    self.execution_progress.emit(f"Error seleccionando columnas: {e}")
                    result_df = df

            elif subtype == 'rename_columns':
                # Renombrar columnas
                rename_map = config.get('rename_map', {})
                try:
                    if isinstance(rename_map, dict) and rename_map:
                        valid_renames = {old: new for old, new in rename_map.items() 
                                       if old in result_df.columns and new}
                        if valid_renames:
                            result_df = result_df.rename(valid_renames)
                except Exception as e:
                    self.execution_progress.emit(f"Error renombrando columnas: {e}")
                    result_df = df

            elif subtype == 'compute_column':
                # Crear nueva columna con expresión
                new_col = config.get('new_column')
                expression = config.get('expression')
                try:
                    if new_col and expression:
                        # Soporte para expresiones simples
                        if '+' in expression:
                            parts = [p.strip() for p in expression.split('+')]
                            if len(parts) >= 2 and all(p in result_df.columns for p in parts):
                                expr = pl.col(parts[0])
                                for part in parts[1:]:
                                    expr = expr + pl.col(part)
                                result_df = result_df.with_columns(expr.alias(new_col))
                        elif '-' in expression:
                            parts = [p.strip() for p in expression.split('-')]
                            if len(parts) == 2 and all(p in result_df.columns for p in parts):
                                result_df = result_df.with_columns((pl.col(parts[0]) - pl.col(parts[1])).alias(new_col))
                        elif '*' in expression:
                            parts = [p.strip() for p in expression.split('*')]
                            if len(parts) == 2 and all(p in result_df.columns for p in parts):
                                result_df = result_df.with_columns((pl.col(parts[0]) * pl.col(parts[1])).alias(new_col))
                        elif '/' in expression:
                            parts = [p.strip() for p in expression.split('/')]
                            if len(parts) == 2 and all(p in result_df.columns for p in parts):
                                result_df = result_df.with_columns((pl.col(parts[0]) / pl.col(parts[1])).alias(new_col))
                        else:
                            # Expresión literal o copia de columna
                            if expression in result_df.columns:
                                result_df = result_df.with_columns(pl.col(expression).alias(new_col))
                            else:
                                # Intentar como valor literal
                                try:
                                    val = float(expression)
                                    result_df = result_df.with_columns(pl.lit(val).alias(new_col))
                                except:
                                    result_df = result_df.with_columns(pl.lit(expression).alias(new_col))
                except Exception as e:
                    self.execution_progress.emit(f"Error creando columna computada: {e}")
                    result_df = df
"""
