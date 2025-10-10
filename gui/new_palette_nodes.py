# Nuevos nodos para agregar a node_palette.py
# Reemplazar la lista transform_buttons (líneas 53-59) con esta lista expandida:

"""
        transform_buttons = [
            # Nodos existentes
            ("Filtro", "transform", "filter"),
            ("Unión", "transform", "join"),
            ("Agregación", "transform", "aggregate"),
            ("Mapeo", "transform", "map"),
            ("Casteo", "transform", "cast"),
            
            # Nuevos nodos básicos
            ("Ordenar", "transform", "sort"),
            ("Eliminar Duplicados", "transform", "unique"),
            ("Limitar Filas", "transform", "limit"),
            ("Rellenar Nulos", "transform", "fill_nulls"),
            ("Eliminar Nulos", "transform", "drop_nulls"),
            
            # Nodos de columnas
            ("Seleccionar Columnas", "transform", "select_columns"),
            ("Renombrar Columnas", "transform", "rename_columns"),
            ("Columna Computada", "transform", "compute_column"),
            
            # Nodos avanzados
            ("Función Ventana", "transform", "window"),
            ("Pivot", "transform", "pivot"),
            ("Unpivot", "transform", "unpivot"),
            ("Explode", "transform", "explode"),
            
            # Nodos de texto y fechas
            ("Transformar Texto", "transform", "text_transform"),
            ("Transformar Fecha", "transform", "date_transform"),
            ("Rolling", "transform", "rolling"),
            
            # Nodos de calidad de datos
            ("Binning", "transform", "binning"),
            ("Outliers", "transform", "outliers")
        ]
"""

# Configuraciones de ejemplo para cada nuevo nodo:

EXAMPLE_CONFIGS = {
    'sort': {
        'sort_columns': ['column1', 'column2 DESC'],  # Lista de columnas, DESC para descendente
    },
    
    'unique': {
        'subset_columns': 'col1,col2',  # Columnas para considerar duplicados
        'keep': 'first'  # 'first', 'last', 'none'
    },
    
    'limit': {
        'n_rows': 100,
        'offset': 0
    },
    
    'fill_nulls': {
        'fill_config': {
            'strategy': 'value',  # 'value', 'forward', 'backward', 'mean', 'median'
            'columns': 'col1,col2',
            'value': 0
        }
    },
    
    'drop_nulls': {
        'subset_columns': 'col1,col2'  # Columnas a verificar por nulos
    },
    
    'select_columns': {
        'columns': 'col1,col2,col3'  # Columnas a seleccionar
    },
    
    'rename_columns': {
        'rename_map': {
            'old_name1': 'new_name1',
            'old_name2': 'new_name2'
        }
    },
    
    'compute_column': {
        'new_column': 'total',
        'expression': 'price + tax'  # Expresión simple
    },
    
    'window': {
        'window_config': {
            'function': 'rank',  # 'rank', 'row_number', 'lag', 'lead', 'sum', 'mean'
            'column': 'value',
            'partition_by': 'category',
            'new_column': 'rank_value'
        }
    },
    
    'pivot': {
        'pivot_config': {
            'index': 'date',
            'columns': 'category',
            'values': 'amount'
        }
    },
    
    'unpivot': {
        'unpivot_config': {
            'id_vars': 'id,date',
            'value_vars': 'col1,col2,col3',
            'var_name': 'variable',
            'value_name': 'value'
        }
    },
    
    'explode': {
        'column': 'list_column'  # Columna que contiene listas
    },
    
    'text_transform': {
        'text_config': {
            'operation': 'upper',  # 'upper', 'lower', 'strip', 'replace', 'extract'
            'columns': 'name,description',
            'old_value': 'old',  # Para replace
            'new_value': 'new',  # Para replace
            'pattern': r'(\d+)'  # Para extract
        }
    },
    
    'date_transform': {
        'date_config': {
            'operation': 'extract',  # 'extract', 'format', 'parse'
            'column': 'date_column',
            'part': 'year',  # 'year', 'month', 'day', 'hour', 'minute', 'weekday'
            'new_column': 'year_extracted',
            'format': '%Y-%m-%d'  # Para parse/format
        }
    },
    
    'rolling': {
        'rolling_config': {
            'column': 'value',
            'window_size': 3,
            'function': 'mean',  # 'mean', 'sum', 'min', 'max', 'std'
            'new_column': 'rolling_mean_3'
        }
    },
    
    'binning': {
        'binning_config': {
            'column': 'age',
            'method': 'equal_width',  # 'equal_width', 'quantile'
            'n_bins': 5,
            'new_column': 'age_bin'
        }
    },
    
    'outliers': {
        'outlier_config': {
            'column': 'value',
            'method': 'iqr',  # 'iqr', 'zscore', 'clip'
            'action': 'remove'  # 'remove', 'clip', 'flag'
        }
    }
}
