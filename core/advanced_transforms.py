# Transformaciones avanzadas adicionales para ETLEngine
# Agregar estos casos elif también al método execute_transform()

"""
            elif subtype == 'window':
                # Funciones de ventana
                window_config = config.get('window_config', {})
                try:
                    func = window_config.get('function')  # 'rank', 'row_number', 'lag', 'lead', 'sum', 'mean'
                    column = window_config.get('column')
                    partition_by = window_config.get('partition_by', [])
                    order_by = window_config.get('order_by', [])
                    new_col = window_config.get('new_column', f"{func}_{column}")
                    
                    if isinstance(partition_by, str):
                        partition_by = [c.strip() for c in partition_by.split(',') if c.strip()]
                    if isinstance(order_by, str):
                        order_by = [c.strip() for c in order_by.split(',') if c.strip()]
                    
                    if func and column and column in result_df.columns:
                        if func == 'rank':
                            expr = pl.col(column).rank().over(partition_by).alias(new_col)
                        elif func == 'row_number':
                            expr = pl.int_range(pl.len()).over(partition_by).alias(new_col)
                        elif func == 'lag':
                            offset = window_config.get('offset', 1)
                            expr = pl.col(column).shift(offset).over(partition_by).alias(new_col)
                        elif func == 'lead':
                            offset = window_config.get('offset', 1)
                            expr = pl.col(column).shift(-offset).over(partition_by).alias(new_col)
                        elif func == 'sum':
                            expr = pl.col(column).sum().over(partition_by).alias(new_col)
                        elif func == 'mean':
                            expr = pl.col(column).mean().over(partition_by).alias(new_col)
                        else:
                            expr = None
                        
                        if expr is not None:
                            result_df = result_df.with_columns(expr)
                except Exception as e:
                    self.execution_progress.emit(f"Error en función de ventana: {e}")
                    result_df = df

            elif subtype == 'pivot':
                # Transformar de largo a ancho
                pivot_config = config.get('pivot_config', {})
                try:
                    index = pivot_config.get('index', [])
                    columns = pivot_config.get('columns')
                    values = pivot_config.get('values')
                    
                    if isinstance(index, str):
                        index = [c.strip() for c in index.split(',') if c.strip()]
                    
                    if columns and values and all(c in result_df.columns for c in [columns, values]):
                        result_df = result_df.pivot(index=index, columns=columns, values=values)
                except Exception as e:
                    self.execution_progress.emit(f"Error en pivot: {e}")
                    result_df = df

            elif subtype == 'unpivot':
                # Transformar de ancho a largo (melt)
                unpivot_config = config.get('unpivot_config', {})
                try:
                    id_vars = unpivot_config.get('id_vars', [])
                    value_vars = unpivot_config.get('value_vars', [])
                    var_name = unpivot_config.get('var_name', 'variable')
                    value_name = unpivot_config.get('value_name', 'value')
                    
                    if isinstance(id_vars, str):
                        id_vars = [c.strip() for c in id_vars.split(',') if c.strip()]
                    if isinstance(value_vars, str):
                        value_vars = [c.strip() for c in value_vars.split(',') if c.strip()]
                    
                    if not value_vars:
                        value_vars = [c for c in result_df.columns if c not in id_vars]
                    
                    result_df = result_df.melt(id_vars=id_vars, value_vars=value_vars, 
                                             variable_name=var_name, value_name=value_name)
                except Exception as e:
                    self.execution_progress.emit(f"Error en unpivot: {e}")
                    result_df = df

            elif subtype == 'explode':
                # Expandir listas en filas
                column = config.get('column')
                try:
                    if column and column in result_df.columns:
                        result_df = result_df.explode(column)
                except Exception as e:
                    self.execution_progress.emit(f"Error en explode: {e}")
                    result_df = df

            elif subtype == 'text_transform':
                # Transformaciones de texto
                text_config = config.get('text_config', {})
                try:
                    operation = text_config.get('operation')  # 'upper', 'lower', 'strip', 'replace', 'extract'
                    columns = text_config.get('columns', [])
                    
                    if isinstance(columns, str):
                        columns = [c.strip() for c in columns.split(',') if c.strip()]
                    
                    if operation and columns:
                        exprs = []
                        for col in columns:
                            if col in result_df.columns:
                                if operation == 'upper':
                                    exprs.append(pl.col(col).str.to_uppercase().alias(col))
                                elif operation == 'lower':
                                    exprs.append(pl.col(col).str.to_lowercase().alias(col))
                                elif operation == 'strip':
                                    exprs.append(pl.col(col).str.strip_chars().alias(col))
                                elif operation == 'replace':
                                    old_val = text_config.get('old_value', '')
                                    new_val = text_config.get('new_value', '')
                                    exprs.append(pl.col(col).str.replace(old_val, new_val).alias(col))
                                elif operation == 'extract':
                                    pattern = text_config.get('pattern', '')
                                    if pattern:
                                        exprs.append(pl.col(col).str.extract(pattern, 0).alias(col))
                        
                        if exprs:
                            result_df = result_df.with_columns(exprs)
                except Exception as e:
                    self.execution_progress.emit(f"Error en transformación de texto: {e}")
                    result_df = df

            elif subtype == 'date_transform':
                # Transformaciones de fecha
                date_config = config.get('date_config', {})
                try:
                    operation = date_config.get('operation')  # 'extract', 'format', 'parse'
                    column = date_config.get('column')
                    
                    if operation and column and column in result_df.columns:
                        if operation == 'extract':
                            part = date_config.get('part')  # 'year', 'month', 'day', 'hour', 'minute'
                            new_col = date_config.get('new_column', f"{column}_{part}")
                            
                            if part == 'year':
                                expr = pl.col(column).dt.year().alias(new_col)
                            elif part == 'month':
                                expr = pl.col(column).dt.month().alias(new_col)
                            elif part == 'day':
                                expr = pl.col(column).dt.day().alias(new_col)
                            elif part == 'hour':
                                expr = pl.col(column).dt.hour().alias(new_col)
                            elif part == 'minute':
                                expr = pl.col(column).dt.minute().alias(new_col)
                            elif part == 'weekday':
                                expr = pl.col(column).dt.weekday().alias(new_col)
                            else:
                                expr = None
                            
                            if expr is not None:
                                result_df = result_df.with_columns(expr)
                        
                        elif operation == 'parse':
                            format_str = date_config.get('format', '%Y-%m-%d')
                            result_df = result_df.with_columns(
                                pl.col(column).str.strptime(pl.Datetime, format_str).alias(column)
                            )
                        
                        elif operation == 'format':
                            format_str = date_config.get('format', '%Y-%m-%d')
                            result_df = result_df.with_columns(
                                pl.col(column).dt.strftime(format_str).alias(column)
                            )
                except Exception as e:
                    self.execution_progress.emit(f"Error en transformación de fecha: {e}")
                    result_df = df

            elif subtype == 'rolling':
                # Ventanas deslizantes
                rolling_config = config.get('rolling_config', {})
                try:
                    column = rolling_config.get('column')
                    window_size = rolling_config.get('window_size', 3)
                    function = rolling_config.get('function', 'mean')  # 'mean', 'sum', 'min', 'max', 'std'
                    new_col = rolling_config.get('new_column', f"rolling_{function}_{column}")
                    
                    if column and column in result_df.columns:
                        if function == 'mean':
                            expr = pl.col(column).rolling_mean(window_size).alias(new_col)
                        elif function == 'sum':
                            expr = pl.col(column).rolling_sum(window_size).alias(new_col)
                        elif function == 'min':
                            expr = pl.col(column).rolling_min(window_size).alias(new_col)
                        elif function == 'max':
                            expr = pl.col(column).rolling_max(window_size).alias(new_col)
                        elif function == 'std':
                            expr = pl.col(column).rolling_std(window_size).alias(new_col)
                        else:
                            expr = None
                        
                        if expr is not None:
                            result_df = result_df.with_columns(expr)
                except Exception as e:
                    self.execution_progress.emit(f"Error en rolling: {e}")
                    result_df = df

            elif subtype == 'binning':
                # Discretización/binning
                binning_config = config.get('binning_config', {})
                try:
                    column = binning_config.get('column')
                    method = binning_config.get('method', 'equal_width')  # 'equal_width', 'quantile'
                    n_bins = binning_config.get('n_bins', 5)
                    new_col = binning_config.get('new_column', f"{column}_bin")
                    
                    if column and column in result_df.columns:
                        if method == 'equal_width':
                            # Usar cut para bins de igual ancho
                            result_df = result_df.with_columns(
                                pl.col(column).cut(n_bins).alias(new_col)
                            )
                        elif method == 'quantile':
                            # Usar qcut para bins por cuantiles
                            result_df = result_df.with_columns(
                                pl.col(column).qcut(n_bins).alias(new_col)
                            )
                except Exception as e:
                    self.execution_progress.emit(f"Error en binning: {e}")
                    result_df = df

            elif subtype == 'outliers':
                # Manejo de outliers
                outlier_config = config.get('outlier_config', {})
                try:
                    column = outlier_config.get('column')
                    method = outlier_config.get('method', 'iqr')  # 'iqr', 'zscore', 'clip'
                    action = outlier_config.get('action', 'remove')  # 'remove', 'clip', 'flag'
                    
                    if column and column in result_df.columns:
                        if method == 'iqr':
                            q1 = result_df.select(pl.col(column).quantile(0.25)).item()
                            q3 = result_df.select(pl.col(column).quantile(0.75)).item()
                            iqr = q3 - q1
                            lower_bound = q1 - 1.5 * iqr
                            upper_bound = q3 + 1.5 * iqr
                            
                            if action == 'remove':
                                result_df = result_df.filter(
                                    (pl.col(column) >= lower_bound) & (pl.col(column) <= upper_bound)
                                )
                            elif action == 'clip':
                                result_df = result_df.with_columns(
                                    pl.col(column).clip(lower_bound, upper_bound).alias(column)
                                )
                            elif action == 'flag':
                                result_df = result_df.with_columns(
                                    ((pl.col(column) < lower_bound) | (pl.col(column) > upper_bound)).alias(f"{column}_outlier")
                                )
                except Exception as e:
                    self.execution_progress.emit(f"Error manejando outliers: {e}")
                    result_df = df
"""
