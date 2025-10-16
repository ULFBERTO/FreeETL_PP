from __future__ import annotations

import json
import os
from typing import Any, Dict

from fastapi.testclient import TestClient

from core.service_runner import ServiceRunner


def make_project_with_etl(tmp_path) -> Dict[str, Any]:
    # Create input JSON
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    in_path = os.path.join(tmp_path, "input.json")
    with open(in_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    out_path = os.path.join(tmp_path, "out.json")

    etl = {
        "id": "etl1",
        "name": "etl1",
        "content": {
            "nodes": [
                {"id": 1, "type": "source", "config": {"subtype": "json", "path": in_path}},
                {"id": 2, "type": "destination", "config": {"subtype": "json", "path": out_path}},
            ],
            "edges": [
                {"source": 1, "target": 2}
            ]
        }
    }
    project = {
        "defaults": {
            "mcp": {"lang": "es", "ollama_base_url": "http://localhost:11434"},
            "services": {"port": 8080},
            "max_parallel": 2,
        },
        "etls": [etl],
        "jobs": [
            {
                "id": "job1",
                "name": "job1",
                "on_error": "stop",
                "stages": [
                    {"parallel": False, "steps": [{"etl_id": "etl1"}]}
                ]
            }
        ],
        "services": []
    }
    return project


def test_etl_run_endpoint_success(tmp_path):
    project = make_project_with_etl(tmp_path)
    service = {"id": "svc", "name": "svc"}
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r = client.post("/etl/run", json={"etl_id": "etl1"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("ok") is True
        log_path = data.get("log_path")
        assert log_path and os.path.exists(log_path)


def test_job_run_endpoint_success(tmp_path):
    project = make_project_with_etl(tmp_path)
    service = {"id": "svc", "name": "svc"}
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r = client.post("/job/run", json={"job_id": "job1"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("ok") is True
        log_path = data.get("log_path")
        assert log_path and os.path.exists(log_path)


def test_etl_run_not_found(tmp_path):
    project = make_project_with_etl(tmp_path)
    service = {"id": "svc", "name": "svc"}
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r = client.post("/etl/run", json={"etl_id": "unknown"})
        assert r.status_code == 404
