from __future__ import annotations

import json
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from core.service_runner import ServiceRunner


def _project_defaults(lang: str = 'es') -> Dict[str, Any]:
    return {
        'defaults': {
            'mcp': {'lang': lang, 'ollama_base_url': 'http://localhost:11434'},
            'services': {'port': 8080},
            'max_parallel': 2,
        }
    }


def test_health_no_auth(tmp_path):
    project = _project_defaults()
    service = {'id': 'svc1', 'name': 'svc1'}  # sin auth -> libre
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r = client.get('/health')
        assert r.status_code == 200
        data = r.json()
        assert data.get('status') == 'ok'
        assert data.get('service') == 'svc1'


def test_echo_default_lang_and_override(tmp_path):
    project = _project_defaults(lang='es')
    service = {'id': 'svc2', 'name': 'svc2'}  # sin auth
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r1 = client.get('/echo')
        assert r1.status_code == 200
        assert r1.json().get('lang') == 'es'

        r2 = client.get('/echo?lang=en')
        assert r2.status_code == 200
        assert r2.json().get('lang') == 'en'


def test_token_and_echo_with_bearer(tmp_path):
    project = _project_defaults()
    service = {
        'id': 'svc3',
        'name': 'svc3',
        'auth': {'basic_user': 'u', 'basic_pass': 'p', 'jwt_secret': 'secret'}
    }
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        # Obtener token con Basic
        r_tok = client.post('/token', auth=('u', 'p'))
        assert r_tok.status_code == 200
        token = r_tok.json().get('access_token')
        assert token

        # Acceder a /echo con Bearer
        r_echo = client.get('/echo', headers={'Authorization': f'Bearer {token}'})
        assert r_echo.status_code == 200
        assert r_echo.json().get('ok') is True

        # Token inválido debe fallar
        r_bad = client.get('/echo', headers={'Authorization': 'Bearer BADTOKEN'})
        assert r_bad.status_code == 401


def test_mcp_chat_missing_prompt_returns_400(tmp_path):
    project = _project_defaults()
    service = {'id': 'svc4', 'name': 'svc4'}
    runner = ServiceRunner(project, service, str(tmp_path))
    app = runner._make_app()  # type: ignore[attr-defined]

    with TestClient(app) as client:
        r = client.post('/mcp/chat', json={})
        assert r.status_code == 400
        assert 'prompt' in (r.json().get('detail') or '')
