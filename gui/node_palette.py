from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QFrame
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QColor, QPalette

class NodePalette(QWidget):
    node_selected = pyqtSignal(str, str)  # Señal emitida cuando se selecciona un tipo de nodo (tipo, subtipo)
    
    def __init__(self):
        super().__init__()
        self.setFixedWidth(200)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        # Title
        title = QLabel("Tipos de Nodos")
        title.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(title)
        
        # Separator
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(separator)
        
        # Source nodes
        source_label = QLabel("Orígenes de Datos")
        source_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(source_label)
        
        source_buttons = [
            ("Archivo CSV", "source", "csv"),
            ("Archivo Excel", "source", "excel"),
            ("Archivo JSON", "source", "json"),
            ("Archivo Parquet", "source", "parquet"),
            ("Base de Datos", "source", "database"),
            ("API", "source", "api")
        ]
        
        for name, node_type, subtype in source_buttons:
            btn = QPushButton(name)
            btn.setStyleSheet("background-color: rgb(100, 200, 100);")
            btn.clicked.connect(lambda checked, t=node_type, s=subtype: self.node_selected.emit(t, s))
            layout.addWidget(btn)
            
        # Transform nodes
        transform_label = QLabel("Transformaciones")
        transform_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(transform_label)
        
        transform_buttons = [
            ("Filtro", "transform", "filter"),
            ("Unión", "transform", "join"),
            ("Agregación", "transform", "aggregate"),
            ("Mapeo", "transform", "map"),
            ("Casteo", "transform", "cast")
        ]
        
        for name, node_type, subtype in transform_buttons:
            btn = QPushButton(name)
            btn.setStyleSheet("background-color: rgb(200, 200, 100);")
            btn.clicked.connect(lambda checked, t=node_type, s=subtype: self.node_selected.emit(t, s))
            layout.addWidget(btn)
            
        # Destination nodes
        dest_label = QLabel("Destinos")
        dest_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(dest_label)
        
        dest_buttons = [
            ("Archivo CSV", "destination", "csv"),
            ("Archivo Excel", "destination", "excel"),
            ("Archivo JSON", "destination", "json"),
            ("Archivo Parquet", "destination", "parquet"),
            ("Base de Datos", "destination", "database"),
            ("API", "destination", "api")
        ]
        
        for name, node_type, subtype in dest_buttons:
            btn = QPushButton(name)
            btn.setStyleSheet("background-color: rgb(200, 100, 100);")
            btn.clicked.connect(lambda checked, t=node_type, s=subtype: self.node_selected.emit(t, s))
            layout.addWidget(btn)
            
        # Add stretch to push everything to the top
        layout.addStretch()
        
        self.setLayout(layout) 