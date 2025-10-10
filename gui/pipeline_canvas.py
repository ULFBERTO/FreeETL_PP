from PyQt6.QtWidgets import (QGraphicsView, QGraphicsScene, QGraphicsEllipseItem, 
                            QGraphicsTextItem, QGraphicsLineItem, QGraphicsPathItem,
                            QGraphicsItem, QMenu, QMessageBox)
from PyQt6.QtCore import Qt, QPointF, pyqtSignal, QRectF, QLineF
from PyQt6.QtGui import QPen, QBrush, QColor, QPainter, QPainterPath, QPolygonF
import networkx as nx
import polars as pl
import math

class ArrowItem(QGraphicsPathItem):
    """Clase para representar las flechas entre nodos"""
    def __init__(self, start_point, end_point, source_id, target_id):
        super().__init__()
        self.source_id = source_id
        self.target_id = target_id
        self.start_point = start_point
        self.end_point = end_point
        self.setPen(QPen(Qt.GlobalColor.black, 2))
        self.setZValue(-1)  # Poner líneas detrás de los nodos
        self.update_path()
        
    def update_path(self):
        """Actualiza el camino de la flecha"""
        path = QPainterPath()
        path.moveTo(self.start_point)
        
        # Crear una curva entre los puntos
        dx = self.end_point.x() - self.start_point.x()
        dy = self.end_point.y() - self.start_point.y()
        ctrl1 = QPointF(self.start_point.x() + dx * 0.5, self.start_point.y())
        ctrl2 = QPointF(self.end_point.x() - dx * 0.5, self.end_point.y())
        path.cubicTo(ctrl1, ctrl2, self.end_point)
        
        # Añadir la punta de flecha
        angle = math.atan2(self.end_point.y() - ctrl2.y(), self.end_point.x() - ctrl2.x())
        arrow_size = 10
        arrow_p1 = QPointF(self.end_point.x() - arrow_size * math.cos(angle - math.pi/6),
                          self.end_point.y() - arrow_size * math.sin(angle - math.pi/6))
        arrow_p2 = QPointF(self.end_point.x() - arrow_size * math.cos(angle + math.pi/6),
                          self.end_point.y() - arrow_size * math.sin(angle + math.pi/6))
        
        arrow_head = QPolygonF()
        arrow_head.append(self.end_point)
        arrow_head.append(arrow_p1)
        arrow_head.append(arrow_p2)
        
        path.addPolygon(arrow_head)
        self.setPath(path)

class ConnectionPoint(QGraphicsEllipseItem):
    """Punto de conexión para entradas o salidas de nodos"""
    def __init__(self, x, y, parent_node, is_input=False):
        size = 10
        super().__init__(x - size/2, y - size/2, size, size)
        self.parent_node = parent_node
        self.is_input = is_input
        
        # Color según tipo (entrada/salida)
        if is_input:
            self.setBrush(QBrush(QColor(100, 100, 200)))  # Azul para entradas
        else:
            self.setBrush(QBrush(QColor(200, 100, 100)))  # Rojo para salidas
            
        self.setPen(QPen(Qt.GlobalColor.black, 1))
        self.setParentItem(parent_node)
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, False)
        
    def get_scene_pos(self):
        """Obtener la posición en coordenadas de la escena"""
        return self.mapToScene(self.rect().center())

class NodeItem(QGraphicsEllipseItem):
    """Clase personalizada para representar nodos en el canvas"""
    def __init__(self, x, y, width, height, node_id, node_type, subtype=None):
        super().__init__(x, y, width, height)
        self.node_id = node_id
        self.node_type = node_type
        self.subtype = subtype
        self.setData(0, node_id)
        self.setFlag(QGraphicsEllipseItem.GraphicsItemFlag.ItemIsMovable)
        self.setFlag(QGraphicsEllipseItem.GraphicsItemFlag.ItemIsSelectable)
        
        # Puntos de conexión
        self.input_points = []
        self.output_points = []
        
        # Número de entradas y salidas según el tipo
        self.num_inputs = 0
        self.num_outputs = 0
        
        # Determinar número de entradas/salidas según el tipo
        if node_type == 'source':
            self.num_inputs = 0
            self.num_outputs = 1
        elif node_type == 'transform':
            if subtype == 'join' or subtype == 'aggregate':
                self.num_inputs = 2  # Estos transformadores permiten 2 entradas
            else:
                self.num_inputs = 1
            self.num_outputs = 1
        elif node_type == 'destination':
            self.num_inputs = 1
            self.num_outputs = 0
            
    def setup_connection_points(self):
        """Configurar los puntos de conexión"""
        # Limpiar puntos existentes
        self.input_points.clear()
        self.output_points.clear()
        
        # Calcular posiciones
        width = self.rect().width()
        height = self.rect().height()
        center_x = self.rect().x() + width/2
        center_y = self.rect().y() + height/2
        
        # Crear puntos de entrada en la parte superior del nodo
        if self.num_inputs > 0:
            spacing = width / (self.num_inputs + 1)
            for i in range(self.num_inputs):
                x = self.rect().x() + spacing * (i + 1)
                y = self.rect().y()
                self.input_points.append(ConnectionPoint(x, y, self, True))
        
        # Crear puntos de salida en la parte inferior del nodo
        if self.num_outputs > 0:
            spacing = width / (self.num_outputs + 1)
            for i in range(self.num_outputs):
                x = self.rect().x() + spacing * (i + 1)
                y = self.rect().y() + height
                self.output_points.append(ConnectionPoint(x, y, self, False))
    
    def get_node_data(self):
        return self.node_id, self.node_type
        
    def contextMenuEvent(self, event):
        """Menú contextual para el nodo"""
        menu = QMenu()
        
        # Acción de cambiar nombre
        rename_action = menu.addAction("Cambiar Nombre")
        
        # Acción de duplicar nodo
        duplicate_action = menu.addAction("Duplicar Nodo")
        
        # Separador
        menu.addSeparator()
        
        # Acción de eliminar nodo
        delete_action = menu.addAction("Eliminar Nodo")
        
        # Mostrar el menú
        action = menu.exec(event.screenPos())
        
        if action == rename_action:
            # Emitir señal para cambiar nombre del nodo
            if self.scene():
                canvas = self.scene().views()[0]
                canvas.rename_node(self.node_id)
        elif action == duplicate_action:
            # Emitir señal para duplicar el nodo
            if self.scene():
                canvas = self.scene().views()[0]
                canvas.duplicate_node(self.node_id)
        elif action == delete_action:
            # Emitir señal para eliminar el nodo
            if self.scene():
                canvas = self.scene().views()[0]
                canvas.delete_node(self.node_id)

class PipelineCanvas(QGraphicsView):
    node_dropped = pyqtSignal(str, QPointF)  # Señal cuando se suelta un nodo
    node_selected = pyqtSignal(int, str, dict)  # Señal cuando se selecciona un nodo
    connection_created = pyqtSignal(str)  # Señal para notificar conexiones establecidas
    background_clicked = pyqtSignal()  # Señal cuando se hace clic en el fondo (sin nodo)
    
    def __init__(self):
        super().__init__()
        self.scene = QGraphicsScene()
        self.setScene(self.scene)
        
        # Set up the view
        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.setViewportUpdateMode(QGraphicsView.ViewportUpdateMode.FullViewportUpdate)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        
        # Initialize the graph
        self.graph = nx.DiGraph()
        
        # Set up the background
        self.setBackgroundBrush(QBrush(QColor(250, 250, 250)))
        
        # Grid configuration
        self.grid_size = 20  # Size of grid squares
        self.grid_color = QColor(220, 220, 220)  # Light gray for grid lines
        
        # Variables para la interacción
        self.dragging = False
        self.current_node = None
        self.connection_start = None
        self.connection_end = None
        self.temp_line = None
        self.temp_connection_point = None
        self.arrows = {}  # Diccionario de flechas por (source_id, target_id)
        
        # Variables para panning
        self.panning = False
        self.last_pan_point = QPointF()
        
        # Nombres descriptivos para tipos de nodos
        self.node_type_names = {
            'source': 'Fuente',
            'transform': 'Transformación',
            'destination': 'Destino',
            'CSV File': 'Archivo CSV',
            'Database': 'Base de Datos',
            'API': 'API',
            'Filter': 'Filtro',
            'Join': 'Unión',
            'Aggregate': 'Agregación',
            'Map': 'Mapeo'
        }

        # Nombres visibles por subtipo (es-ES)
        self.subtype_display_map = {
            'csv': 'CSV',
            'excel': 'Excel',
            'json': 'JSON',
            'parquet': 'Parquet',
            'database': 'Base de Datos',
            'api': 'API',
            'filter': 'Filtro',
            'join': 'Unión',
            'aggregate': 'Agregación',
            'map': 'Mapeo',
            'cast': 'Casteo',
        }

    def drawBackground(self, painter, rect):
        """Draw a grid background"""
        # Fill background with base color
        painter.fillRect(rect, QBrush(QColor(250, 250, 250)))
        
        # Set up pen for grid lines
        pen = QPen(self.grid_color, 1)
        painter.setPen(pen)
        
        # Calculate grid boundaries
        left = int(rect.left()) - (int(rect.left()) % self.grid_size)
        top = int(rect.top()) - (int(rect.top()) % self.grid_size)
        
        # Draw vertical lines
        x = left
        while x < rect.right():
            painter.drawLine(x, int(rect.top()), x, int(rect.bottom()))
            x += self.grid_size
            
        # Draw horizontal lines
        y = top
        while y < rect.bottom():
            painter.drawLine(int(rect.left()), y, int(rect.right()), y)
            y += self.grid_size

    def _format_node_name(self, node_type, subtype, config=None):
        # Si hay un nombre personalizado, usarlo
        if config and 'custom_name' in config:
            return config['custom_name']
        
        # Usar el formato estándar
        node_name = self.node_type_names.get(node_type, node_type)
        if subtype:
            display = self.subtype_display_map.get(subtype, None)
            if display is None:
                try:
                    display = subtype.capitalize()
                except Exception:
                    display = str(subtype)
            node_name += f": {display}"
        return node_name
        
    def add_node(self, node_type, position, subtype=None):
        """Add a new node to the pipeline"""
        # Usar el siguiente ID disponible para evitar colisiones tras cargar pipelines
        if self.graph.nodes:
            try:
                node_id = max(self.graph.nodes) + 1
            except Exception:
                node_id = len(self.graph.nodes)
        else:
            node_id = 0
        config = {'subtype': subtype} if subtype else {}
        self.graph.add_node(node_id, type=node_type, position=position, config=config)
        self.draw_node(node_id, position, subtype)
        return node_id
    
    def add_node_with_id(self, node_id, node_type, position, subtype=None):
        """Add a node with an explicit ID (useful when loading a saved pipeline)."""
        if node_id in self.graph.nodes:
            # Avoid duplicates
            return
        config = {'subtype': subtype} if subtype else {}
        self.graph.add_node(node_id, type=node_type, position=position, config=config)
        self.draw_node(node_id, position, subtype)
        
    def add_connection(self, source_id, source_point, target_id, target_point):
        """Add a connection between nodes"""
        # Verificar si la conexión ya existe
        if self.graph.has_edge(source_id, target_id):
            return
            
        # Agregar la conexión al grafo
        self.graph.add_edge(source_id, target_id)
        
        # Dibujar la conexión visual
        self.draw_connection(source_id, source_point, target_id, target_point)
        
        # Transferir datos del origen al destino si corresponde
        source_node = self.find_node_by_id(source_id)
        target_node = self.find_node_by_id(target_id)
        
        if source_node and target_node:
            source_type = source_node.node_type
            target_type = target_node.node_type
            source_subtype = source_node.subtype
            target_subtype = target_node.subtype
            
            # Propagar datos del origen al destino cuando sea un nodo de transformación
            self.propagate_data_to_target(source_id, target_id)
            
            # Mostrar mensaje de conexión establecida
            message = f"Conexión establecida: {self.node_type_names.get(source_type, source_type)} "
            if source_subtype:
                message += f"({source_subtype}) "
            message += f"→ {self.node_type_names.get(target_type, target_type)}"
            if target_subtype:
                message += f" ({target_subtype})"
                
            # Emitir señal con el mensaje de conexión
            self.connection_created.emit(message)
            
            # Emitir señal para actualizar las propiedades del nodo destino
            self.node_selected.emit(target_id, target_type, self.graph.nodes[target_id].get('config', {}))
            
    def propagate_data_to_target(self, source_id, target_id):
        """Propaga los datos desde un nodo origen a un nodo destino"""
        try:
            source_config = self.graph.nodes[source_id].get('config', {})
            target_config = self.graph.nodes[target_id].get('config', {})
            target_type = self.graph.nodes[target_id]['type']
            target_subtype = target_config.get('subtype')
            
            # Verificar si el origen tiene datos
            if 'dataframe' in source_config:
                # Verificar que el dataframe no sea None
                if source_config['dataframe'] is None:
                    print(f"Error: Dataframe nulo en nodo origen {source_id}")
                    return
                    
                try:
                    # Preparar dataframe del ORIGEN aplicando la transformación si el origen es un nodo de transformación
                    source_node_type = self.graph.nodes[source_id]['type'] if source_id in self.graph.nodes else None
                    prepared_df = None
                    if source_node_type == 'transform':
                        st = source_config.get('subtype')
                        if st == 'join' and source_config.get('other_dataframe') is not None:
                            # El preview de unión ya respeta selección/renombre
                            prepared_df = self._build_join_selected_df(source_config)
                        else:
                            # Aplicar transformación de preview y luego selección/renombrado
                            base_df = source_config['dataframe']
                            if st == 'filter':
                                base_df = self._apply_filter_rules_preview(base_df, source_config)
                            elif st == 'map':
                                base_df = self._apply_map_ops_preview(base_df, source_config)
                            elif st == 'aggregate':
                                base_df = self._apply_aggregate_preview(base_df, source_config)
                            elif st == 'cast':
                                base_df = self._apply_cast_preview(base_df, source_config)
                            prepared_df = self._apply_select_and_rename(base_df, source_config)
                    else:
                        # Origen es un nodo de fuente o destino: solo aplicar selección/renombrado del origen
                        if source_config.get('subtype') == 'join' and source_config.get('other_dataframe') is not None:
                            prepared_df = self._build_join_selected_df(source_config)
                        else:
                            prepared_df = self._apply_select_and_rename(source_config['dataframe'], source_config)
                    # Si el destino es un nodo de transformación
                    if target_type == 'transform':
                        # Para nodos de unión, verificar si ya tiene un dataframe
                        if target_subtype == 'join':
                            # Determinar la posición del source entre las entradas actuales
                            incoming = list(self.graph.in_edges(target_id))
                            pos = None
                            for i, (src, _) in enumerate(incoming):
                                if src == source_id:
                                    pos = i
                                    break
                            if pos is None:
                                # Fallback al conteo anterior
                                if len(incoming) <= 1:
                                    target_config['dataframe'] = prepared_df
                                    print(f"Datos copiados de {source_id} a {target_id} como dataframe principal")
                                else:
                                    target_config['other_dataframe'] = prepared_df
                                    print(f"Datos copiados de {source_id} a {target_id} como dataframe secundario")
                            else:
                                if pos == 0:
                                    target_config['dataframe'] = prepared_df
                                    print(f"Datos copiados de {source_id} a {target_id} como dataframe principal (pos 0)")
                                else:
                                    target_config['other_dataframe'] = prepared_df
                                    print(f"Datos copiados de {source_id} a {target_id} como dataframe secundario (pos {pos})")
                        else:
                            # Para otros tipos de transformaciones, simplemente asignar el dataframe
                            target_config['dataframe'] = prepared_df
                            print(f"Datos copiados de {source_id} a {target_id}")
                    # Si el destino es un nodo de destino
                    elif target_type == 'destination':
                        # Asignar el dataframe preparado
                        target_config['dataframe'] = prepared_df
                        print(f"Datos copiados de {source_id} a {target_id} (destino)")
                except Exception as e:
                    print(f"Error al copiar dataframe: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    
                # Actualizar la configuración del nodo
                self.graph.nodes[target_id]['config'] = target_config
        except Exception as e:
            print(f"Error en propagate_data_to_target: {str(e)}")
            import traceback
            traceback.print_exc()

    def _apply_select_and_rename(self, df, config):
        """Aplica la selección y renombrado definidos en el nodo origen."""
        try:
            # Asegurar Polars
            if not isinstance(df, pl.DataFrame):
                try:
                    import pandas as pd
                    if isinstance(df, pd.DataFrame):
                        df = pl.from_pandas(df)
                    else:
                        df = pl.DataFrame(df)
                except Exception:
                    df = pl.DataFrame(df)

            result = df
            # Selección de columnas
            output_cols = config.get('output_cols')
            if output_cols and isinstance(output_cols, str):
                cols = [c.strip() for c in output_cols.split(',') if c.strip()]
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
            return result
        except Exception:
            pass
        return df

    def _to_pl(self, df):
        """Convierte df a polars DataFrame si es posible."""
        if isinstance(df, pl.DataFrame):
            return df
        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                return pl.from_pandas(df)
        except Exception:
            pass
        try:
            return pl.DataFrame(df)
        except Exception:
            return pl.DataFrame()

    def _apply_filter_rules_preview(self, df, config):
        """Aplica reglas de filtro (preview) leyendo filter_rules/filter_mode."""
        try:
            df = self._to_pl(df)
            rules = config.get('filter_rules')
            mode = (config.get('filter_mode') or 'all').lower()
            if not isinstance(rules, list) or not rules:
                return df
            exprs = []
            for r in rules:
                col = r.get('column')
                op = str(r.get('op', '')).lower()
                val = r.get('value')
                if not col or not op:
                    continue
                e = None
                if op == '>':
                    e = pl.col(col) > val
                elif op == '<':
                    e = pl.col(col) < val
                elif op == '==':
                    e = pl.col(col) == val
                elif op == '!=':
                    e = pl.col(col) != val
                elif op == '>=':
                    e = pl.col(col) >= val
                elif op == '<=':
                    e = pl.col(col) <= val
                elif op == 'contains' and isinstance(val, str):
                    e = pl.col(col).cast(pl.Utf8).str.contains(val)
                elif op == 'in':
                    try:
                        seq = list(val) if isinstance(val, (list, tuple, set)) else [v.strip() for v in str(val).split(',') if v.strip()]
                    except Exception:
                        seq = [val]
                    e = pl.col(col).is_in(seq)
                elif op == 'isnull':
                    e = pl.col(col).is_null()
                elif op == 'notnull':
                    e = pl.col(col).is_not_null()
                if e is not None:
                    exprs.append(e)
            if not exprs:
                return df
            final = exprs[0]
            for e in exprs[1:]:
                final = (final | e) if mode == 'any' else (final & e)
            return df.filter(final)
        except Exception:
            return df

    def _apply_map_ops_preview(self, df, config):
        """Aplica operaciones de mapeo (preview) leyendo map_ops."""
        try:
            df = self._to_pl(df)
            ops = config.get('map_ops')
            if not isinstance(ops, list) or not ops:
                return df
            exprs = []
            for op in ops:
                new_col = op.get('new_col')
                op_type = str(op.get('op_type', '')).lower()
                a = op.get('a')
                b = op.get('b')
                val = op.get('value')
                if not new_col:
                    continue
                expr = None
                if op_type == 'add' and a and b:
                    expr = (pl.col(a) + pl.col(b)).alias(new_col)
                elif op_type == 'sub' and a and b:
                    expr = (pl.col(a) - pl.col(b)).alias(new_col)
                elif op_type == 'mul' and a and b:
                    expr = (pl.col(a) * pl.col(b)).alias(new_col)
                elif op_type == 'div' and a and b:
                    expr = (pl.col(a) / pl.col(b)).alias(new_col)
                elif op_type == 'concat' and a and b:
                    expr = (pl.col(a).cast(pl.Utf8) + pl.col(b).cast(pl.Utf8)).alias(new_col)
                elif op_type == 'literal' and val is not None:
                    expr = pl.lit(val).alias(new_col)
                elif op_type == 'copy' and a:
                    expr = pl.col(a).alias(new_col)
                elif op_type == 'upper' and a:
                    expr = pl.col(a).cast(pl.Utf8).str.to_uppercase().alias(new_col)
                elif op_type == 'lower' and a:
                    expr = pl.col(a).cast(pl.Utf8).str.to_lowercase().alias(new_col)
                elif op_type == 'length' and a:
                    expr = pl.col(a).cast(pl.Utf8).str.len_chars().alias(new_col)
                if expr is not None:
                    exprs.append(expr)
            return df.with_columns(exprs) if exprs else df
        except Exception:
            return df

    def _apply_aggregate_preview(self, df, config):
        """Aplica agregación (preview) leyendo group_by_list y aggs."""
        try:
            df = self._to_pl(df)
            group_by = config.get('group_by_list')
            aggs = config.get('aggs')
            if not isinstance(group_by, list) or not isinstance(aggs, list):
                return df
            agg_exprs = []
            for agg in aggs:
                func = str(agg.get('func', '')).lower()
                col = agg.get('col')
                alias = agg.get('as') or None
                if not col:
                    continue
                expr = None
                if func == 'sum':
                    expr = pl.sum(col)
                elif func in ('avg', 'mean'):
                    expr = pl.mean(col)
                elif func == 'min':
                    expr = pl.min(col)
                elif func == 'max':
                    expr = pl.max(col)
                elif func == 'count':
                    expr = pl.count()
                if expr is not None and alias:
                    expr = expr.alias(alias)
                if expr is not None:
                    agg_exprs.append(expr)
            group_cols = [c for c in group_by if isinstance(c, str) and c]
            if group_cols and agg_exprs:
                return df.group_by(group_cols).agg(agg_exprs)
            return df
        except Exception:
            return df

    def _apply_cast_preview(self, df, config):
        """Aplica casteo de tipos (preview) leyendo cast_ops."""
        try:
            df = self._to_pl(df)
            ops = config.get('cast_ops')
            if not isinstance(ops, list) or not ops:
                return df
            def map_dtype(name: str):
                n = (name or '').strip().lower()
                if n == 'int64':
                    return pl.Int64
                if n == 'int32':
                    return pl.Int32
                if n == 'float64':
                    return pl.Float64
                if n == 'float32':
                    return pl.Float32
                if n in ('utf8', 'string', 'str', 'texto'):
                    return pl.Utf8
                if n in ('bool', 'boolean'):
                    return pl.Boolean
                if n == 'date':
                    return pl.Date
                if n == 'datetime':
                    return pl.Datetime
                return None
            exprs = []
            for op in ops:
                col = op.get('col')
                to = op.get('to')
                dtype = map_dtype(to)
                if col and dtype is not None and col in df.columns:
                    fmt = (op.get('fmt') or '').strip()
                    if dtype == pl.Date:
                        if fmt:
                            exprs.append(pl.col(col).cast(pl.Utf8).str.strptime(pl.Date, fmt=fmt, strict=False).alias(col))
                        else:
                            exprs.append(pl.col(col).cast(pl.Utf8).str.to_date(strict=False).alias(col))
                    elif dtype == pl.Datetime:
                        if fmt:
                            exprs.append(pl.col(col).cast(pl.Utf8).str.strptime(pl.Datetime, fmt=fmt, strict=False).alias(col))
                        else:
                            exprs.append(pl.col(col).cast(pl.Utf8).str.to_datetime(strict=False).alias(col))
                    else:
                        exprs.append(pl.col(col).cast(dtype).alias(col))
            return df.with_columns(exprs) if exprs else df
        except Exception:
            return df

    def update_node_visual(self, node_id):
        """Actualiza el título y puntos de conexión del nodo tras cambio de subtipo."""
        try:
            node_item = self.find_node_by_id(node_id)
            if not node_item:
                return
            node_data = self.graph.nodes[node_id]
            node_type = node_data['type']
            config = node_data.get('config', {})
            subtype = config.get('subtype')
            # Actualizar subtipo
            node_item.subtype = subtype

            # Actualizar entradas/salidas según subtipo
            if node_type == 'source':
                node_item.num_inputs = 0
                node_item.num_outputs = 1
            elif node_type == 'transform':
                if subtype in ['join', 'aggregate']:
                    node_item.num_inputs = 2
                else:
                    node_item.num_inputs = 1
                node_item.num_outputs = 1
            elif node_type == 'destination':
                node_item.num_inputs = 1
                node_item.num_outputs = 0

            # Eliminar puntos de conexión existentes (hijos) antes de recrearlos
            for child in list(node_item.childItems()):
                if isinstance(child, ConnectionPoint):
                    # Quitar del parent y de la escena
                    child.setParentItem(None)
                    if self.scene:
                        try:
                            self.scene.removeItem(child)
                        except Exception:
                            pass

            # Volver a crear puntos de conexión
            node_item.setup_connection_points()

            # Actualizar el texto del nodo (buscar el QGraphicsTextItem hijo)
            node_name = self._format_node_name(node_type, subtype, config)
            for child in node_item.childItems():
                if isinstance(child, QGraphicsTextItem):
                    child.setPlainText(node_name)
                    break

            # Actualizar flechas/conexiones visuales
            self.update_node_connections(node_id)
        except Exception as e:
            print(f"Error actualizando visual del nodo {node_id}: {e}")

    def _build_join_selected_df(self, config):
        """Construye un dataframe combinado para un nodo de unión, respetando output_cols y renombres.
        Si no hay selección, combina todas las columnas de Origen1 y Origen2.
        """
        try:
            df1 = config.get('dataframe')
            df2 = config.get('other_dataframe')
            # Normalizar a Polars
            def to_pl(df):
                if isinstance(df, pl.DataFrame):
                    return df
                try:
                    import pandas as pd
                    if isinstance(df, pd.DataFrame):
                        return pl.from_pandas(df)
                except Exception:
                    pass
                return pl.DataFrame(df)
            if df1 is not None:
                df1 = to_pl(df1)
            if df2 is not None:
                df2 = to_pl(df2)

            # Parse selección y renombres
            selections = []
            out_spec = config.get('output_cols')
            if out_spec and isinstance(out_spec, str) and out_spec.strip():
                selections = [c.strip() for c in out_spec.split(',') if c.strip()]
            else:
                # Por defecto: todas las columnas de ambos orígenes
                if df1 is not None:
                    selections.extend([f"Origen1.{c}" for c in df1.columns])
                if df2 is not None:
                    selections.extend([f"Origen2.{c}" for c in df2.columns])
            rename_dict = {}
            rn_spec = config.get('column_rename')
            if rn_spec and isinstance(rn_spec, str):
                for pair in rn_spec.split(','):
                    if ':' in pair:
                        old_name, new_name = pair.split(':', 1)
                        rename_dict[old_name.strip()] = new_name.strip()

            # Determinar número de filas para el preview combinado
            n1 = len(df1) if df1 is not None else 0
            n2 = len(df2) if df2 is not None else 0
            n = max(n1, n2, 0)
            # Evitar 0 filas para preview: usa min 1 (polars requiere longitud consistente)
            if n == 0:
                n = 0

            data_cols = {}
            for sel in selections:
                base = sel
                src = None
                if sel.startswith('Origen1.'):
                    src = 1
                    base = sel.split('.', 1)[1]
                elif sel.startswith('Origen2.'):
                    src = 2
                    base = sel.split('.', 1)[1]
                # Nombre final (renombrado si aplica)
                out_name = rename_dict.get(sel, base)
                series = None
                if src == 1 and df1 is not None and base in df1.columns:
                    series = df1[base]
                elif src == 2 and df2 is not None and base in df2.columns:
                    series = df2[base]
                # Rellenar o truncar a longitud n
                if series is None:
                    data_cols[out_name] = [None] * n
                else:
                    try:
                        values = series.to_list()
                    except Exception:
                        values = list(series)
                    if len(values) < n:
                        values = values + [None] * (n - len(values))
                    elif len(values) > n:
                        values = values[:n]
                    data_cols[out_name] = values
            # Construir DataFrame
            return pl.DataFrame(data_cols) if data_cols else (df1 if df1 is not None else df2)
        except Exception as e:
            print(f"Error construyendo preview de unión: {e}")
            return config.get('dataframe')
        
    def draw_node(self, node_id, position, subtype=None):
        """Draw a node on the canvas"""
        node = self.graph.nodes[node_id]
        node_type = node['type']
        config = node.get('config', {})
        
        # Create node visual representation
        if node_type == 'source':
            color = QColor(100, 200, 100)  # Green for sources
            node_name = self._format_node_name(node_type, subtype, config)
        elif node_type == 'transform':
            color = QColor(200, 200, 100)  # Yellow for transforms
            node_name = self._format_node_name(node_type, subtype, config)
        elif node_type == 'destination':
            color = QColor(200, 100, 100)  # Red for destinations
            node_name = self._format_node_name(node_type, subtype, config)
            
        # Create the node item
        node_item = NodeItem(position.x() - 40, position.y() - 40, 80, 80, 
                             node_id, node_type, subtype)
        node_item.setPen(QPen(Qt.GlobalColor.black, 2))
        node_item.setBrush(QBrush(color))
        
        # Add text label
        text_item = QGraphicsTextItem(node_name)
        text_item.setPos(position.x() - 35, position.y() - 10)
        text_item.setParentItem(node_item)
        
        # Configurar puntos de conexión
        node_item.setup_connection_points()
        
        self.scene.addItem(node_item)
        
    def draw_connection(self, source_id, source_point, target_id, target_point):
        """Draw a connection between specific points of two nodes"""
        # Obtener las posiciones de los puntos
        start_pos = source_point.get_scene_pos()
        end_pos = target_point.get_scene_pos()
        
        # Crear la flecha
        arrow = ArrowItem(start_pos, end_pos, source_id, target_id)
        self.scene.addItem(arrow)
        
        # Guardar referencia a la flecha
        key = (source_id, target_id)
        self.arrows[key] = arrow
    
    def add_edge_simple(self, source_id, target_id):
        """Add an edge between two nodes and draw a default arrow using available points."""
        # Evitar conexiones duplicadas
        if self.graph.has_edge(source_id, target_id):
            return
        self.graph.add_edge(source_id, target_id)
        source_node = self.find_node_by_id(source_id)
        target_node = self.find_node_by_id(target_id)
        if not source_node or not target_node:
            return
        if not source_node.output_points or not target_node.input_points:
            return
        source_point = source_node.output_points[0]
        # Determinar entrada a usar
        input_idx = 0
        if target_node.node_type == 'transform' and target_node.subtype in ['join', 'aggregate']:
            incoming_count = len(list(self.graph.in_edges(target_id))) - 1  # ya añadimos este edge
            input_idx = min(incoming_count, len(target_node.input_points) - 1)
        target_point = target_node.input_points[input_idx]
        self.draw_connection(source_id, source_point, target_id, target_point)
        
    def find_node_by_id(self, node_id):
        """Encuentra un nodo por su ID"""
        for item in self.scene.items():
            if isinstance(item, NodeItem) and item.node_id == node_id:
                return item
        return None
        
    def find_connection_point(self, pos):
        """Encuentra un punto de conexión cercano a la posición dada"""
        for item in self.scene.items(pos):
            if isinstance(item, ConnectionPoint):
                return item
        return None
        
    def mousePressEvent(self, event):
        """Handle mouse press events"""
        if event.button() == Qt.MouseButton.LeftButton:
            # Get the position in scene coordinates
            pos = self.mapToScene(event.pos())
            
            # Verificar si se hizo clic en un punto de conexión
            connection_point = self.find_connection_point(pos)
            if connection_point:
                # Solo permitir conexiones desde puntos de salida
                if not connection_point.is_input:
                    self.temp_connection_point = connection_point
                    self.connection_start = connection_point.parent_node
                    
                    # Crear una línea temporal para mostrar mientras se arrastra
                    start_pos = connection_point.get_scene_pos()
                    self.temp_line = QGraphicsLineItem(start_pos.x(), start_pos.y(), pos.x(), pos.y())
                    self.temp_line.setPen(QPen(Qt.GlobalColor.black, 2, Qt.PenStyle.DashLine))
                    self.scene.addItem(self.temp_line)
                return
                
            # Verificar si el item o alguno de sus padres es un NodeItem
            node_item = self.find_node_item(self.scene.itemAt(pos, self.transform()))
            
            if node_item:
                self.dragging = True
                self.current_node = node_item
                node_item.setSelected(True)
                
                # Emit node selected signal
                node_id = node_item.node_id
                node_type = node_item.node_type
                node_data = self.graph.nodes[node_id].get('config', {})
                self.node_selected.emit(node_id, node_type, node_data)
            else:
                self.connection_start = None
                
                # Deselect all nodes
                for item in self.scene.selectedItems():
                    item.setSelected(False)
                # Emitir señal de clic en el fondo para vaciar el panel de propiedades
                self.background_clicked.emit()
                
                # Iniciar panning si no hay nodo seleccionado
                self.panning = True
                self.last_pan_point = event.pos()
                self.setCursor(Qt.CursorShape.ClosedHandCursor)
                
        super().mousePressEvent(event)
        
    def find_node_item(self, item):
        """Find a NodeItem from any item or its parents"""
        current = item
        while current:
            if isinstance(current, NodeItem):
                return current
            # Check if item has a parent
            if hasattr(current, 'parentItem'):
                current = current.parentItem()
            else:
                break
        return None
        
    def mouseMoveEvent(self, event):
        """Handle mouse move events"""
        pos = self.mapToScene(event.pos())
        
        # Si estamos arrastrando una conexión
        if self.temp_line and self.temp_connection_point:
            start_pos = self.temp_connection_point.get_scene_pos()
            self.temp_line.setLine(QLineF(start_pos, pos))
            return
            
        # Si estamos haciendo panning
        if self.panning:
            # Calcular el desplazamiento
            delta = event.pos() - self.last_pan_point
            self.last_pan_point = event.pos()
            
            # Aplicar el desplazamiento a la vista
            self.horizontalScrollBar().setValue(self.horizontalScrollBar().value() - delta.x())
            self.verticalScrollBar().setValue(self.verticalScrollBar().value() - delta.y())
            return
            
        # Si estamos arrastrando un nodo
        if self.dragging and self.current_node:
            # Update node position in the graph
            node_id = self.current_node.node_id
            self.graph.nodes[node_id]['position'] = pos
            
            # Actualizar las conexiones asociadas a este nodo
            self.update_node_connections(node_id)
            
        super().mouseMoveEvent(event)
        
    def update_node_connections(self, node_id):
        """Actualiza las conexiones de un nodo cuando se mueve"""
        # Actualizar conexiones donde este nodo es fuente
        for edge in list(self.graph.out_edges(node_id)):
            _, target_id = edge
            key = (node_id, target_id)
            if key in self.arrows:
                # Eliminar la flecha antigua
                self.scene.removeItem(self.arrows[key])
                del self.arrows[key]
                
                # Buscar los nodos
                source_node = self.find_node_by_id(node_id)
                target_node = self.find_node_by_id(target_id)
                
                if source_node and target_node and source_node.output_points and target_node.input_points:
                    # Crear nueva flecha
                    source_point = source_node.output_points[0]
                    
                    # Determinar qué punto de entrada usar en el nodo destino
                    input_idx = 0
                    
                    # Para nodos de unión (join), averiguar qué punto de entrada usar
                    if target_node.node_type == 'transform' and target_node.subtype in ['join', 'aggregate']:
                        # Contar cuántos nodos ya están conectados a este nodo
                        incoming_edges = list(self.graph.in_edges(target_id))
                        
                        # Obtener el índice del origen en la lista de edges
                        for i, (src, dst) in enumerate(incoming_edges):
                            if src == node_id:
                                input_idx = min(i, len(target_node.input_points)-1)
                                break
                                
                    # Asegurarse que el índice esté dentro del rango
                    if input_idx < len(target_node.input_points):
                        target_point = target_node.input_points[input_idx]
                        self.draw_connection(node_id, source_point, target_id, target_point)
                    
        # Actualizar conexiones donde este nodo es destino
        for edge in list(self.graph.in_edges(node_id)):
            source_id, _ = edge
            key = (source_id, node_id)
            if key in self.arrows:
                # Eliminar la flecha antigua
                self.scene.removeItem(self.arrows[key])
                del self.arrows[key]
                
                # Buscar los nodos
                source_node = self.find_node_by_id(source_id)
                target_node = self.find_node_by_id(node_id)
                
                if source_node and target_node and source_node.output_points and target_node.input_points:
                    # Crear nueva flecha
                    source_point = source_node.output_points[0]
                    
                    # Determinar qué punto de entrada usar
                    input_idx = 0
                    
                    # Para nodos de unión (join), averiguar qué punto de entrada usar
                    if target_node.node_type == 'transform' and target_node.subtype in ['join', 'aggregate']:
                        # Contar cuántos nodos ya están conectados a este nodo
                        incoming_edges = list(self.graph.in_edges(node_id))
                        
                        # Obtener el índice del origen en la lista de edges
                        for i, (src, dst) in enumerate(incoming_edges):
                            if src == source_id:
                                input_idx = min(i, len(target_node.input_points)-1)
                                break
                    
                    # Asegurarse que el índice esté dentro del rango
                    if input_idx < len(target_node.input_points):
                        target_point = target_node.input_points[input_idx]
                        self.draw_connection(source_id, source_point, node_id, target_point)
        
    def mouseReleaseEvent(self, event):
        """Handle mouse release events"""
        if event.button() == Qt.MouseButton.LeftButton:
            # Si estamos conectando nodos
            if self.temp_line and self.temp_connection_point:
                pos = self.mapToScene(event.pos())
                target_point = self.find_connection_point(pos)
                
                if target_point and target_point.is_input:
                    source_node = self.temp_connection_point.parent_node
                    target_node = target_point.parent_node
                    
                    # Verificar reglas de conexión
                    if source_node != target_node:  # No autoconexiones
                        source_type = source_node.node_type
                        target_type = target_node.node_type
                        source_subtype = source_node.subtype
                        target_subtype = target_node.subtype
                        
                        # Reglas de conexión
                        valid_connection = True
                        error_msg = ""
                        
                        # Regla 1: Los nodos de destino no pueden ser origen de conexiones
                        if source_type == 'destination':
                            valid_connection = False
                            error_msg = "Los nodos de destino no pueden ser origen de conexiones"
                        
                        # Regla 2: Los nodos de origen no pueden ser destino de conexiones
                        elif target_type == 'source':
                            valid_connection = False
                            error_msg = "Los nodos de origen no pueden ser destino de conexiones"
                        
                        # Regla 3: Verificar conexiones múltiples para nodos normales
                        elif target_type == 'transform' and target_subtype != 'join' and target_subtype != 'aggregate':
                            # Si el nodo destino no es de unión y ya tiene conexiones de entrada
                            if list(self.graph.in_edges(target_node.node_id)):
                                valid_connection = False
                                error_msg = "Este nodo ya tiene una conexión de entrada"
                                
                        # Regla 4: Validar número máximo de conexiones para uniones y agregaciones
                        elif target_type == 'transform' and (target_subtype == 'join' or target_subtype == 'aggregate'):
                            # Máximo 2 conexiones para uniones y agregaciones
                            if len(list(self.graph.in_edges(target_node.node_id))) >= 2:
                                valid_connection = False
                                error_msg = "Los nodos de unión y agregación pueden tener máximo 2 entradas"
                                
                            # Determinar a qué punto de entrada conectar
                            if valid_connection:
                                # Marcar el punto específico de entrada al que se conectará
                                incoming_count = len(list(self.graph.in_edges(target_node.node_id)))
                                # Asegurarse de que el punto de entrada está dentro del rango disponible
                                input_idx = min(incoming_count, len(target_node.input_points)-1)
                                target_point = target_node.input_points[input_idx]
                        
                        # Regla 5: Los nodos de destino solo pueden tener una conexión de entrada
                        elif target_type == 'destination':
                            # Verificar si el destino ya tiene una conexión de entrada
                            if list(self.graph.in_edges(target_node.node_id)):
                                valid_connection = False
                                error_msg = "Los nodos de destino solo pueden tener una conexión de entrada"
                        
                        if valid_connection:
                            self.add_connection(source_node.node_id, self.temp_connection_point, 
                                               target_node.node_id, target_point)
                        else:
                            QMessageBox.warning(self, "Error de conexión", error_msg)
                
                # Eliminar la línea temporal
                self.scene.removeItem(self.temp_line)
                self.temp_line = None
                self.temp_connection_point = None
                return
                
            # Si estamos arrastrando un nodo
            if self.dragging and self.current_node:
                self.dragging = False
                self.current_node = None
                
            # Si estamos haciendo panning
            if self.panning:
                self.panning = False
                self.setCursor(Qt.CursorShape.ArrowCursor)
                
        super().mouseReleaseEvent(event)
        
    def delete_node(self, node_id):
        """Elimina un nodo y sus conexiones"""
        # Eliminar flechas asociadas
        for edge in list(self.graph.edges()):
            source, target = edge
            if source == node_id or target == node_id:
                key = (source, target)
                if key in self.arrows:
                    self.scene.removeItem(self.arrows[key])
                    del self.arrows[key]
        
        # Eliminar el nodo del grafo
        if node_id in self.graph:
            self.graph.remove_node(node_id)
            
        # Eliminar el nodo visualmente
        node_item = self.find_node_by_id(node_id)
        if node_item:
            self.scene.removeItem(node_item)
    
    def clear_all(self):
        """Clear the entire canvas, graph, and arrows."""
        # Eliminar todos los items de la escena
        for item in list(self.scene.items()):
            self.scene.removeItem(item)
        # Reiniciar estructuras
        self.graph.clear()
        self.arrows.clear()
            
    def wheelEvent(self, event):
        """Handle zooming with mouse wheel"""
        factor = 1.2
        if event.angleDelta().y() < 0:
            factor = 1.0 / factor
        self.scale(factor, factor)
    
    def rename_node(self, node_id):
        """Permite cambiar el nombre personalizado de un nodo"""
        from PyQt6.QtWidgets import QInputDialog
        
        try:
            # Obtener el nodo
            node_item = self.find_node_by_id(node_id)
            if not node_item:
                return
            
            # Obtener nombre actual
            current_name = ""
            node_data = self.graph.nodes[node_id]
            config = node_data.get('config', {})
            
            # Verificar si ya tiene un nombre personalizado
            if 'custom_name' in config:
                current_name = config['custom_name']
            else:
                # Usar el nombre por defecto basado en tipo/subtipo
                current_name = self._format_node_name(node_data['type'], config.get('subtype'))
            
            # Mostrar diálogo para cambiar nombre
            new_name, ok = QInputDialog.getText(
                None, 
                "Cambiar Nombre del Nodo", 
                f"Nuevo nombre para el nodo {node_id}:",
                text=current_name
            )
            
            if ok and new_name.strip():
                # Guardar el nombre personalizado en la configuración
                config['custom_name'] = new_name.strip()
                self.graph.nodes[node_id]['config'] = config
                
                # Actualizar el texto visual del nodo
                for child in node_item.childItems():
                    if isinstance(child, QGraphicsTextItem):
                        child.setPlainText(new_name.strip())
                        break
                
                print(f"Nodo {node_id} renombrado a: {new_name.strip()}")
                
        except Exception as e:
            print(f"Error renombrando nodo {node_id}: {e}")
    
    def duplicate_node(self, node_id):
        """Duplicar un nodo con su configuración"""
        try:
            # Obtener el nodo original
            if node_id not in self.graph.nodes:
                return
            
            original_node = self.graph.nodes[node_id]
            original_item = self.find_node_by_id(node_id)
            
            if not original_item:
                return
            
            # Obtener datos del nodo original
            node_type = original_node['type']
            original_config = original_node.get('config', {}).copy()
            subtype = original_config.get('subtype')
            
            # Limpiar datos específicos que no deben duplicarse
            if 'dataframe' in original_config:
                del original_config['dataframe']
            if 'other_dataframe' in original_config:
                del original_config['other_dataframe']
            
            # Calcular nueva posición (desplazada)
            original_pos = original_item.pos()
            new_position = QPointF(original_pos.x() + 100, original_pos.y() + 50)
            
            # Crear el nuevo nodo
            new_node_id = self.add_node(node_type, new_position, subtype)
            
            # Copiar la configuración (excepto datos)
            if new_node_id in self.graph.nodes:
                self.graph.nodes[new_node_id]['config'].update(original_config)
                
                # Si tenía nombre personalizado, agregar "(Copia)"
                if 'custom_name' in original_config:
                    original_name = original_config['custom_name']
                    new_name = f"{original_name} (Copia)"
                    self.graph.nodes[new_node_id]['config']['custom_name'] = new_name
                    
                    # Actualizar el texto visual
                    new_item = self.find_node_by_id(new_node_id)
                    if new_item:
                        for child in new_item.childItems():
                            if isinstance(child, QGraphicsTextItem):
                                child.setPlainText(new_name)
                                break
            
            print(f"Nodo {node_id} duplicado como nodo {new_node_id}")
            
        except Exception as e:
            print(f"Error duplicando nodo {node_id}: {e}")