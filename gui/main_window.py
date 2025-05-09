from PyQt6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                            QPushButton, QLabel, QMenuBar, QMenu, QStatusBar,
                            QTextEdit, QSplitter, QMessageBox)
from PyQt6.QtCore import Qt, QTimer
from .pipeline_canvas import PipelineCanvas
from .node_palette import NodePalette
from .properties_panel import PropertiesPanel
from core.etl_engine import ETLEngine

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Pipeline Builder")
        self.setGeometry(100, 100, 1500, 800)  # Ventana más ancha
        
        # Create central widget and layout
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Usar un QSplitter para dividir la ventana
        self.main_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Widget superior con layout horizontal
        self.top_widget = QWidget()
        main_layout = QHBoxLayout(self.top_widget)
        
        # Create node palette
        self.node_palette = NodePalette()
        main_layout.addWidget(self.node_palette)
        
        # Create pipeline canvas
        self.pipeline_canvas = PipelineCanvas()
        main_layout.addWidget(self.pipeline_canvas)
        
        # Create properties panel
        self.properties_panel = PropertiesPanel()
        main_layout.addWidget(self.properties_panel)
        
        # Agregar widget superior al splitter
        self.main_splitter.addWidget(self.top_widget)
        
        # Create log panel
        self.log_panel = QTextEdit()
        self.log_panel.setReadOnly(True)
        self.log_panel.setMaximumHeight(150)
        self.log_panel.append("Bienvenido a ETL Pipeline Builder")
        
        # Agregar log panel al splitter
        self.main_splitter.addWidget(self.log_panel)
        
        # Set sizes for splitter
        self.main_splitter.setSizes([600, 150])
        
        # Agregar el splitter al layout principal
        main_layout_vertical = QVBoxLayout(self.central_widget)
        main_layout_vertical.addWidget(self.main_splitter)
        
        # Create ETL engine
        self.etl_engine = ETLEngine()
        
        # Connect signals
        self.node_palette.node_selected.connect(self.handle_node_selected)
        self.pipeline_canvas.node_selected.connect(self.properties_panel.show_node_properties)
        self.properties_panel.node_config_changed.connect(self.handle_node_config_changed)
        self.pipeline_canvas.connection_created.connect(self.log_message)
        self.properties_panel.fetch_connected_data.connect(self.fetch_data_from_connected_nodes)
        
        # Connect ETL engine signals
        self.etl_engine.execution_progress.connect(self.log_message)
        self.etl_engine.execution_finished.connect(self.handle_execution_finished)
        self.etl_engine.node_executed.connect(self.handle_node_executed)
        
        # Create menu bar
        self.create_menu_bar()
        
        # Create status bar
        self.statusBar().showMessage("Ready")
        
    def handle_node_selected(self, node_type, subtype=None):
        """Handle when a node type is selected from the palette"""
        # Get the center of the canvas
        center = self.pipeline_canvas.viewport().rect().center()
        scene_pos = self.pipeline_canvas.mapToScene(center)
        
        # Add the node to the canvas
        node_id = self.pipeline_canvas.add_node(node_type, scene_pos, subtype)
        
        # Show properties for the new node
        self.properties_panel.show_node_properties(node_id, node_type, {'subtype': subtype})
        
        # Log node creation
        self.log_message(f"Nodo {node_id} creado de tipo {node_type}")
        
    def handle_node_config_changed(self, node_id, config):
        """Maneja el cambio de configuración de un nodo"""
        # Actualizar la configuración del nodo en el pipeline
        if node_id in self.pipeline_canvas.graph.nodes:
            # Guardar la configuración actualizada
            self.pipeline_canvas.graph.nodes[node_id]['config'] = config
            self.log_message(f"Configuración actualizada para nodo {node_id}")
            
            # Obtener tipo de nodo
            node_type = self.pipeline_canvas.graph.nodes[node_id]['type']
            
            # Si hay dataframes en la configuración, verificar si hay nodos conectados
            # que necesiten recibir estos datos
            if 'dataframe' in config:
                # Propagar datos a los nodos conectados
                for target_id in self.pipeline_canvas.graph.successors(node_id):
                    # Propagar los datos actualizados
                    self.pipeline_canvas.propagate_data_to_target(node_id, target_id)
                    
                    # Actualizar el panel de propiedades si es el nodo actualmente seleccionado
                    if hasattr(self.properties_panel, 'current_node_id') and self.properties_panel.current_node_id == target_id:
                        target_type = self.pipeline_canvas.graph.nodes[target_id]['type']
                        
                        # Actualizar también el dataframe en el panel de propiedades
                        target_config = self.pipeline_canvas.graph.nodes[target_id].get('config', {})
                        if 'dataframe' in target_config:
                            self.properties_panel.set_node_dataframe(target_id, target_config['dataframe'])
                            
                        # Mostrar propiedades actualizadas
                        self.properties_panel.show_node_properties(
                            target_id, 
                            target_type, 
                            self.pipeline_canvas.graph.nodes[target_id].get('config', {})
                        )
                        
                # Si es un nodo de transformación, también debemos propagar a sus nodos destino
                if node_type == 'transform':
                    # Verificar si hay nodos destino indirectamente conectados
                    for target_id in list(self.pipeline_canvas.graph.successors(node_id)):
                        # Propagar a los nodos conectados al nodo objetivo (si es una transformación)
                        target_type = self.pipeline_canvas.graph.nodes[target_id]['type']
                        if target_type == 'transform':
                            for second_target_id in self.pipeline_canvas.graph.successors(target_id):
                                # Asegurar que la transformación se propague correctamente
                                self.pipeline_canvas.propagate_data_to_target(target_id, second_target_id)
        
    def handle_node_executed(self, node_id, dataframe):
        """Maneja el evento de nodo ejecutado"""
        # Actualizar el dataframe en el panel de propiedades
        self.properties_panel.set_node_dataframe(node_id, dataframe)
        self.log_message(f"Nodo {node_id} ejecutado con éxito - {len(dataframe)} filas")
        
    def handle_execution_finished(self, success, message):
        """Maneja el final de la ejecución del pipeline"""
        if success:
            QMessageBox.information(self, "Ejecución completada", message)
        else:
            QMessageBox.critical(self, "Error en ejecución", message)
            
    def log_message(self, message):
        """Añade un mensaje al panel de log"""
        self.log_panel.append(message)
        # Scroll to bottom
        scrollbar = self.log_panel.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
    def create_menu_bar(self):
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu("Archivo")
        new_action = file_menu.addAction("Nuevo Pipeline")
        open_action = file_menu.addAction("Abrir Pipeline")
        save_action = file_menu.addAction("Guardar Pipeline")
        file_menu.addSeparator()
        exit_action = file_menu.addAction("Salir")
        
        # Edit menu
        edit_menu = menubar.addMenu("Editar")
        undo_action = edit_menu.addAction("Deshacer")
        redo_action = edit_menu.addAction("Rehacer")
        
        # Run menu
        run_menu = menubar.addMenu("Ejecutar")
        run_pipeline_action = run_menu.addAction("Ejecutar Pipeline")
        stop_pipeline_action = run_menu.addAction("Detener Pipeline")
        
        # Connect actions
        exit_action.triggered.connect(self.close)
        run_pipeline_action.triggered.connect(self.run_pipeline)
        stop_pipeline_action.triggered.connect(self.stop_pipeline)
        
    def run_pipeline(self):
        """Ejecuta la pipeline actual"""
        self.statusBar().showMessage("Ejecutando pipeline...")
        self.log_message("Iniciando ejecución de pipeline...")
        
        # Verificar que la pipeline tenga nodos
        if not self.pipeline_canvas.graph.nodes:
            QMessageBox.warning(self, "Pipeline vacío", "No hay nodos en el pipeline para ejecutar")
            return
            
        # Obtener configuraciones de nodos
        node_configs = {}
        for node_id in self.pipeline_canvas.graph.nodes:
            node_configs[node_id] = self.properties_panel.get_node_config(node_id)
            
        # Configurar el motor ETL
        self.etl_engine.set_pipeline(self.pipeline_canvas.graph, node_configs)
        
        # Ejecutar el pipeline
        self.etl_engine.execute_pipeline()
        
    def stop_pipeline(self):
        """Detiene la ejecución de la pipeline"""
        self.statusBar().showMessage("Deteniendo pipeline...")
        self.log_message("Deteniendo ejecución de pipeline...")
        # TODO: Implementar mecanismo para detener la ejecución
        QMessageBox.information(self, "No implementado", "Detener pipeline no implementado aún")
        
    def fetch_data_from_connected_nodes(self, node_id):
        """Busca datos en los nodos conectados al nodo especificado"""
        self.log_message(f"Obteniendo datos para el nodo {node_id}...")
        
        # Verificar que el nodo existe
        if node_id not in self.pipeline_canvas.graph.nodes:
            self.log_message(f"Error: Nodo {node_id} no encontrado")
            return
            
        node_type = self.pipeline_canvas.graph.nodes[node_id]['type']
        node_config = self.pipeline_canvas.graph.nodes[node_id].get('config', {})
        
        # Si es un nodo de transformación, buscar los nodos conectados a sus entradas
        if node_type == 'transform':
            # Obtener nodos de entrada
            incoming_edges = list(self.pipeline_canvas.graph.in_edges(node_id))
            
            if not incoming_edges:
                self.log_message("No hay nodos conectados como entrada")
                QMessageBox.warning(self, "Sin fuentes de datos", "No hay nodos conectados como entrada. Primero conecte este nodo a una fuente de datos.")
                return
                
            # Actualizar los datos del nodo actual con los datos de los nodos de entrada
            updated_config = node_config.copy()
            data_updated = False
            
            # Para cada conexión de entrada
            for idx, (source_id, _) in enumerate(incoming_edges):
                source_config = self.pipeline_canvas.graph.nodes[source_id].get('config', {})
                
                # Verificar si el nodo fuente tiene datos
                if 'dataframe' in source_config:
                    data_updated = True
                    if idx == 0 or len(incoming_edges) == 1:
                        # Si es la primera entrada o la única, asignar como dataframe principal
                        updated_config['dataframe'] = source_config['dataframe']
                        self.log_message(f"Datos obtenidos del nodo {source_id} como fuente principal")
                    elif idx == 1 and node_config.get('subtype') in ['join', 'aggregate']:
                        # Si es la segunda entrada y el nodo es de unión o agregación
                        updated_config['other_dataframe'] = source_config['dataframe']
                        self.log_message(f"Datos obtenidos del nodo {source_id} como fuente secundaria")
                else:
                    self.log_message(f"El nodo fuente {source_id} no tiene datos disponibles")
            
            if not data_updated:
                self.log_message("No se encontraron datos en los nodos conectados")
                QMessageBox.warning(self, "Sin datos", "Los nodos conectados no contienen datos. Asegúrese de cargar datos en los nodos de origen.")
                return
                
            # Actualizar la configuración del nodo
            self.pipeline_canvas.graph.nodes[node_id]['config'] = updated_config
            
            # Asegurarse de que la actualización se registre para este nodo específico
            self.log_message(f"Configuración de nodo {node_id} actualizada con datos de entrada")
            
            # Para nodos de unión, verificar que se tienen ambos dataframes necesarios
            if node_config.get('subtype') == 'join':
                if 'dataframe' not in updated_config or 'other_dataframe' not in updated_config:
                    self.log_message(f"Faltan datos para la unión en el nodo {node_id}")
                    if 'dataframe' not in updated_config:
                        QMessageBox.warning(self, "Datos incompletos", "Falta el primer origen de datos para la unión.")
                    elif 'other_dataframe' not in updated_config:
                        QMessageBox.warning(self, "Datos incompletos", "Falta el segundo origen de datos para la unión.")
                    
            # Actualizar el panel de propiedades
            if hasattr(self.properties_panel, 'update_with_fetched_data'):
                self.properties_panel.update_with_fetched_data(node_id, updated_config)
                self.log_message(f"Panel de propiedades actualizado para el nodo {node_id}")
            else:
                # Si no existe el método, recargar todo el panel
                self.properties_panel.show_node_properties(node_id, node_type, updated_config)
                self.log_message(f"Panel de propiedades recargado para el nodo {node_id}")
                
            self.log_message(f"Datos actualizados para el nodo {node_id}")
            
        # Si es un nodo de destino, buscar datos del nodo de entrada conectado
        elif node_type == 'destination':
            # Obtener nodos de entrada
            incoming_edges = list(self.pipeline_canvas.graph.in_edges(node_id))
            
            if not incoming_edges:
                self.log_message("El nodo destino no tiene conexiones de entrada")
                QMessageBox.warning(self, "Sin fuentes de datos", "Este nodo destino no está conectado a ninguna fuente. Primero conecte este nodo a una fuente o transformación.")
                return
                
            # Obtener el primer nodo de entrada (los nodos destino solo tienen una entrada)
            source_id, _ = incoming_edges[0]
            source_config = self.pipeline_canvas.graph.nodes[source_id].get('config', {})
            
            # Verificar si el nodo fuente tiene datos
            if 'dataframe' in source_config:
                # Actualizar la configuración del nodo destino con los datos del nodo fuente
                updated_config = node_config.copy()
                updated_config['dataframe'] = source_config['dataframe']
                
                # Guardar la configuración actualizada
                self.pipeline_canvas.graph.nodes[node_id]['config'] = updated_config
                
                # Actualizar el panel de propiedades
                if hasattr(self.properties_panel, 'set_node_dataframe'):
                    self.properties_panel.set_node_dataframe(node_id, source_config['dataframe'])
                    self.log_message(f"Dataframe actualizado para nodo destino {node_id}")
                
                # Asegurar que los datos se propaguen correctamente
                self.pipeline_canvas.propagate_data_to_target(source_id, node_id)
                
                self.log_message(f"Datos obtenidos del nodo {source_id} para el nodo destino {node_id}")
            else:
                self.log_message(f"El nodo fuente {source_id} no tiene datos disponibles")
                QMessageBox.warning(self, "Sin datos", "El nodo conectado no contiene datos. Asegúrese de cargar o procesar datos primero.")
                
        else:
            self.log_message(f"El nodo {node_id} no es un nodo de transformación o destino")
            QMessageBox.warning(self, "Operación no válida", "Solo los nodos de transformación y destino pueden obtener datos de otros nodos") 