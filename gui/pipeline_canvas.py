from PyQt6.QtWidgets import (QGraphicsView, QGraphicsScene, QGraphicsEllipseItem, 
                            QGraphicsTextItem, QGraphicsLineItem, QGraphicsPathItem,
                            QGraphicsItem, QMenu, QMessageBox)
from PyQt6.QtCore import Qt, QPointF, pyqtSignal, QRectF, QLineF
from PyQt6.QtGui import QPen, QBrush, QColor, QPainter, QPainterPath, QPolygonF
import networkx as nx
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
        
        # Acción de eliminar nodo
        delete_action = menu.addAction("Eliminar nodo")
        
        # Mostrar el menú
        action = menu.exec(event.screenPos())
        
        if action == delete_action:
            # Emitir señal para eliminar el nodo
            if self.scene():
                canvas = self.scene().views()[0]
                canvas.delete_node(self.node_id)

class PipelineCanvas(QGraphicsView):
    node_dropped = pyqtSignal(str, QPointF)  # Señal cuando se suelta un nodo
    node_selected = pyqtSignal(int, str, dict)  # Señal cuando se selecciona un nodo
    connection_created = pyqtSignal(str)  # Señal para notificar conexiones establecidas
    
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
        self.setBackgroundBrush(QBrush(QColor(240, 240, 240)))
        
        # Variables para la interacción
        self.dragging = False
        self.current_node = None
        self.connection_start = None
        self.connection_end = None
        self.temp_line = None
        self.temp_connection_point = None
        self.arrows = {}  # Diccionario de flechas por (source_id, target_id)
        
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
        
    def add_node(self, node_type, position, subtype=None):
        """Add a new node to the pipeline"""
        node_id = len(self.graph.nodes)
        config = {'subtype': subtype} if subtype else {}
        self.graph.add_node(node_id, type=node_type, position=position, config=config)
        self.draw_node(node_id, position, subtype)
        return node_id
        
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
                    # Si el destino es un nodo de transformación
                    if target_type == 'transform':
                        # Para nodos de unión, verificar si ya tiene un dataframe
                        if target_subtype == 'join':
                            # Contar cuántas entradas ya tiene
                            incoming = list(self.graph.in_edges(target_id))
                            
                            # Si es la primera conexión, guardar como dataframe principal
                            if len(incoming) <= 1:
                                # Asignar el dataframe directamente
                                target_config['dataframe'] = source_config['dataframe']
                                print(f"Datos copiados de {source_id} a {target_id} como dataframe principal")
                            # Si es la segunda conexión, guardar como dataframe secundario
                            elif len(incoming) == 2:
                                # Asignar el dataframe directamente
                                target_config['other_dataframe'] = source_config['dataframe']
                                print(f"Datos copiados de {source_id} a {target_id} como dataframe secundario")
                        else:
                            # Para otros tipos de transformaciones, simplemente asignar el dataframe
                            target_config['dataframe'] = source_config['dataframe']
                            print(f"Datos copiados de {source_id} a {target_id}")
                    # Si el destino es un nodo de destino
                    elif target_type == 'destination':
                        # Asignar el dataframe directamente
                        target_config['dataframe'] = source_config['dataframe']
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
        
    def draw_node(self, node_id, position, subtype=None):
        """Draw a node on the canvas"""
        node = self.graph.nodes[node_id]
        node_type = node['type']
        
        # Create node visual representation
        if node_type == 'source':
            color = QColor(100, 200, 100)  # Green for sources
            node_name = self.node_type_names.get(node_type, node_type)
            if subtype:
                node_name += f": {subtype.capitalize()}"
        elif node_type == 'transform':
            color = QColor(200, 200, 100)  # Yellow for transforms
            node_name = self.node_type_names.get(node_type, node_type)
            if subtype:
                node_name += f": {subtype.capitalize()}"
        elif node_type == 'destination':
            color = QColor(200, 100, 100)  # Red for destinations
            node_name = self.node_type_names.get(node_type, node_type)
            if subtype:
                node_name += f": {subtype.capitalize()}"
            
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
            
    def wheelEvent(self, event):
        """Handle zooming with mouse wheel"""
        factor = 1.2
        if event.angleDelta().y() < 0:
            factor = 1.0 / factor
        self.scale(factor, factor) 