from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QSpinBox, QLineEdit,
    QDialogButtonBox, QLabel, QComboBox
)


class ProjectSettingsDialog(QDialog):
    def __init__(self, parent, project_manager):
        super().__init__(parent)
        self.setWindowTitle("Ajustes del Proyecto")
        self.pm = project_manager

        self.layout = QVBoxLayout(self)
        form = QFormLayout()

        # Defaults object
        d = (self.pm.project.get('defaults') if isinstance(self.pm.project, dict) else {}) or {}
        services = d.get('services') or {}
        mcp = d.get('mcp') or {}

        # max_parallel
        self.max_parallel = QSpinBox()
        self.max_parallel.setRange(1, 512)
        try:
            self.max_parallel.setValue(int(d.get('max_parallel') or 2))
        except Exception:
            self.max_parallel.setValue(2)
        form.addRow(QLabel("Máximo paralelismo (CPU cores):"), self.max_parallel)

        # default service port
        self.service_port = QSpinBox()
        self.service_port.setRange(1, 65535)
        try:
            self.service_port.setValue(int(services.get('port') or 8080))
        except Exception:
            self.service_port.setValue(8080)
        form.addRow(QLabel("Puerto por defecto (Servicios):"), self.service_port)

        # MCP lang
        self.mcp_lang = QComboBox()
        self.mcp_lang.addItems(["auto", "es", "en"])
        cur_lang = str(mcp.get('lang') or 'es')
        if cur_lang not in ["auto", "es", "en"]:
            self.mcp_lang.addItem(cur_lang)
        self.mcp_lang.setCurrentText(cur_lang)
        form.addRow(QLabel("Idioma MCP por defecto:"), self.mcp_lang)

        # Ollama base URL
        self.ollama_base_url = QLineEdit()
        self.ollama_base_url.setPlaceholderText("http://localhost:11434")
        self.ollama_base_url.setText(str(mcp.get('ollama_base_url') or 'http://localhost:11434'))
        form.addRow(QLabel("Ollama Base URL:"), self.ollama_base_url)

        self.layout.addLayout(form)

        # Buttons
        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(self._save_and_close)
        btns.rejected.connect(self.reject)
        self.layout.addWidget(btns)

        self.resize(420, 220)

    def _save_and_close(self):
        # Ensure defaults structure exists
        proj = self.pm.project if isinstance(self.pm.project, dict) else {}
        proj.setdefault('defaults', {})
        d = proj['defaults']
        d['max_parallel'] = int(self.max_parallel.value())
        d.setdefault('services', {})
        d['services']['port'] = int(self.service_port.value())
        d.setdefault('mcp', {})
        d['mcp']['lang'] = self.mcp_lang.currentText()
        d['mcp']['ollama_base_url'] = self.ollama_base_url.text().strip() or 'http://localhost:11434'
        # Save in-memory; caller may persist to disk
        self.accept()
