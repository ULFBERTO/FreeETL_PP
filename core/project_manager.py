import json
import os
from datetime import datetime
from typing import Any, Dict, Optional


class ProjectManager:
    """Manages FreeETL single-file .fetl projects.

    Responsibilities:
    - Create a new project (.fetl) with sensible defaults.
    - Open existing .fetl and normalize schema (ensure defaults/keys present).
    - Save project back to disk.
    - Provide computed paths (e.g., sidecar logs folder: <project>.fetl.logs).
    """

    def __init__(self):
        self.project: Dict[str, Any] = {}
        self.path: Optional[str] = None

    # ---------------------- Public API ----------------------
    def create_new(self, path: str) -> Dict[str, Any]:
        path = self._ensure_fetl_extension(path)
        name = os.path.splitext(os.path.basename(path))[0]
        defaults = self._make_defaults()
        proj = {
            "version": "1.0",
            "name": name,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "defaults": defaults,
            "etls": [],
            "jobs": [],
            "services": [],
            "runs": [],
        }
        self._write_json(path, proj)
        self.path = path
        self.project = proj
        # Ensure sidecar logs folder exists
        self.ensure_logs_root()
        return proj

    def open(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            proj = json.load(f)
        self.path = self._ensure_fetl_extension(path)
        self.project = self._normalize_project(proj)
        # Ensure sidecar logs folder exists
        self.ensure_logs_root()
        return self.project

    def save(self) -> None:
        if not self.path:
            raise ValueError("No project path set")
        if not self.project:
            raise ValueError("No project content to save")
        self._write_json(self.path, self.project)

    def save_as(self, path: str) -> None:
        path = self._ensure_fetl_extension(path)
        if not self.project:
            raise ValueError("No project content to save")
        self._write_json(path, self.project)
        self.path = path
        # Sidecar folder name may change if filename changed
        self.ensure_logs_root()

    # ---------------------- Helpers ----------------------
    def logs_root(self) -> str:
        if not self.path:
            raise ValueError("Project path not set")
        return f"{self.path}.logs"

    def ensure_logs_root(self) -> str:
        root = self.logs_root()
        try:
            os.makedirs(root, exist_ok=True)
            os.makedirs(os.path.join(root, "jobs"), exist_ok=True)
            os.makedirs(os.path.join(root, "services"), exist_ok=True)
            os.makedirs(os.path.join(root, "etls"), exist_ok=True)
        except Exception:
            # Non-fatal: UI should continue even if logs dir cannot be created
            pass
        return root

    # ---------------------- Internal ----------------------
    def _ensure_fetl_extension(self, path: str) -> str:
        if not path.lower().endswith(".fetl"):
            path = f"{path}.fetl"
        return path

    def _write_json(self, path: str, data: Dict[str, Any]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _make_defaults(self) -> Dict[str, Any]:
        try:
            cores = os.cpu_count() or 2
        except Exception:
            cores = 2
        return {
            "max_parallel": cores,
            "services": {"port": 8080},
            # MCP defaults can be overridden per project/service or per request (?lang=)
            "mcp": {"lang": "es", "ollama_base_url": "http://localhost:11434"},
        }

    def _normalize_project(self, proj: Dict[str, Any]) -> Dict[str, Any]:
        proj = dict(proj or {})
        proj.setdefault("version", "1.0")
        proj.setdefault("name", os.path.splitext(os.path.basename(self.path or "Proyecto"))[0])
        proj.setdefault("created_at", datetime.now().isoformat(timespec="seconds"))
        proj.setdefault("defaults", {})
        d = proj["defaults"]
        if not isinstance(d, dict):
            d = {}
            proj["defaults"] = d
        d.setdefault("max_parallel", (os.cpu_count() or 2))
        d.setdefault("services", {})
        if not isinstance(d["services"], dict):
            d["services"] = {}
        d["services"].setdefault("port", 8080)
        d.setdefault("mcp", {})
        if not isinstance(d["mcp"], dict):
            d["mcp"] = {}
        d["mcp"].setdefault("lang", "es")
        d["mcp"].setdefault("ollama_base_url", "http://localhost:11434")

        proj.setdefault("etls", [])
        proj.setdefault("jobs", [])
        proj.setdefault("services", [])
        proj.setdefault("runs", [])
        return proj
