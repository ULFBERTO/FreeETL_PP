from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Callable

from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPAuthorizationCredentials, HTTPBearer
import jwt
import uvicorn
import requests
import networkx as nx

from core.etl_engine import ETLEngine
from core.job_runner import JobRunner


class ServiceRunner:
    """Ejecuta un servicio FastAPI en un hilo, con Basic+JWT, logs sidecar y stop.

    service esperado (subset):
    {
      id, name, port?, auth: { basic_user?, basic_pass?, jwt_secret? }
    }
    """

    def __init__(self,
                 project: Dict[str, Any],
                 service: Dict[str, Any],
                 logs_root: str,
                 ui_writer: Optional[Callable[[str], None]] = None) -> None:
        self.project = project
        self.service = service
        self.logs_root = logs_root
        self.ui_writer = ui_writer
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[uvicorn.Server] = None
        self._should_stop = threading.Event()
        self._running = threading.Event()
        self._log_file_path = self._open_log_file()

    # ---- Logging ----
    def _open_log_file(self) -> str:
        name = str(self.service.get('name') or self.service.get('id') or 'service')
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        dir_path = os.path.join(self.logs_root, 'services', name)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f'{ts}.log')
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat(timespec='seconds')} | [SERVICE] init {name}\n")
        except Exception:
            pass
        return file_path

    def _log(self, msg: str) -> None:
        try:
            with open(self._log_file_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().isoformat(timespec='seconds')} | {msg}\n")
        except Exception:
            pass
        if self.ui_writer:
            try:
                self.ui_writer(msg)
            except Exception:
                pass

    # ---- Auth helpers ----
    def _make_app(self) -> FastAPI:
        app = FastAPI(title=str(self.service.get('name') or self.service.get('id') or 'service'))

        basic = HTTPBasic(auto_error=False)
        bearer = HTTPBearer(auto_error=False)
        svc_auth = (self.service.get('auth') or {}) if isinstance(self.service, dict) else {}
        basic_user = svc_auth.get('basic_user')
        basic_pass = svc_auth.get('basic_pass')
        jwt_secret = svc_auth.get('jwt_secret')
        defaults = (self.project.get('defaults') or {}) if isinstance(self.project, dict) else {}
        default_lang = ((defaults.get('mcp') or {}).get('lang')) or 'es'
        ollama_url = ((defaults.get('mcp') or {}).get('ollama_base_url')) or 'http://localhost:11434'

        def auth_dependency(credentials_basic: Optional[HTTPBasicCredentials] = Depends(basic),
                           credentials_bearer: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> str:
            # Accept JWT if present and valid
            if credentials_bearer and credentials_bearer.scheme.lower() == 'bearer' and credentials_bearer.credentials:
                if not jwt_secret:
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='JWT disabled')
                try:
                    jwt.decode(credentials_bearer.credentials, jwt_secret, algorithms=['HS256'])
                    return 'jwt'
                except Exception:
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid token')
            # Accept Basic
            if credentials_basic and basic_user and basic_pass:
                if credentials_basic.username == basic_user and credentials_basic.password == basic_pass:
                    return 'basic'
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid basic credentials')
            # If no auth configured, allow
            if not basic_user and not basic_pass and not jwt_secret:
                return 'none'
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Auth required')

        @app.middleware('http')
        async def logging_middleware(request: Request, call_next):
            self._log(f"[REQ] {request.method} {request.url}")
            try:
                response = await call_next(request)
            except Exception as e:
                self._log(f"[ERROR] {e}")
                return JSONResponse(status_code=500, content={'error': str(e)})
            self._log(f"[RES] {response.status_code}")
            return response

        @app.get('/health')
        async def health():
            return {'status': 'ok', 'service': self.service.get('id')}

        @app.get('/echo')
        async def echo(request: Request, _auth=Depends(auth_dependency)):
            # lang overrides via query param
            lang = request.query_params.get('lang') or default_lang
            return {'ok': True, 'lang': lang, 'service': self.service.get('id')}

        @app.post('/token')
        async def token(_credentials: Optional[HTTPBasicCredentials] = Depends(basic)):
            if not jwt_secret:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='JWT disabled')
            if not _credentials or _credentials.username != basic_user or _credentials.password != basic_pass:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid basic credentials')
            exp = datetime.utcnow() + timedelta(hours=24)
            payload = {'sub': _credentials.username, 'exp': exp}
            tkn = jwt.encode(payload, jwt_secret, algorithm='HS256')
            return {'access_token': tkn, 'token_type': 'bearer', 'expires_in': 24 * 3600}

        @app.get('/mcp/models')
        async def mcp_models(_auth=Depends(auth_dependency)):
            try:
                url = f"{ollama_url.rstrip('/')}/api/tags"
                r = requests.get(url, timeout=2)
                r.raise_for_status()
                data = r.json() or {}
                items = data.get('models') or []
                names = []
                for m in items:
                    name = m.get('name') or m.get('model')
                    if name:
                        names.append(str(name))
                return {'ok': True, 'models': names}
            except Exception as e:
                self._log(f"[MCP] models error: {e}")
                return JSONResponse(status_code=502, content={'ok': False, 'error': str(e)})

        @app.post('/mcp/chat')
        async def mcp_chat(request: Request, _auth=Depends(auth_dependency)):
            try:
                body = await request.json()
                prompt = (body or {}).get('prompt')
                model = (body or {}).get('model')
                if not prompt:
                    raise HTTPException(status_code=400, detail='prompt requerido')
                # Lang override
                lang = request.query_params.get('lang') or default_lang
                suffix = 'Responde en español.' if str(lang).lower().startswith('es') else 'Answer in English.'
                prompt_final = f"{prompt}\n\n{suffix}"
                # Model autodetect
                if not model:
                    url_tags = f"{ollama_url.rstrip('/')}/api/tags"
                    rr = requests.get(url_tags, timeout=2)
                    rr.raise_for_status()
                    tags = rr.json() or {}
                    models = tags.get('models') or []
                    if not models:
                        raise HTTPException(status_code=502, detail='No hay modelos de Ollama disponibles')
                    model = models[0].get('name') or models[0].get('model')
                    if not model:
                        raise HTTPException(status_code=502, detail='Modelo de Ollama inválido')
                # Generate
                url_gen = f"{ollama_url.rstrip('/')}/api/generate"
                payload = {'model': model, 'prompt': prompt_final, 'stream': False}
                rg = requests.post(url_gen, json=payload, timeout=30)
                rg.raise_for_status()
                out = rg.json() or {}
                text = out.get('response') or ''
                return {'ok': True, 'model': model, 'lang': lang, 'text': text}
            except HTTPException:
                raise
            except Exception as e:
                self._log(f"[MCP] chat error: {e}")
                return JSONResponse(status_code=502, content={'ok': False, 'error': str(e)})

        # ------------- ETL/Job execution endpoints -------------
        def _build_graph_from_etl_content(etl_content: Dict[str, Any]):
            g = nx.DiGraph()
            node_configs: Dict[int, Dict[str, Any]] = {}
            for n in (etl_content.get('nodes') or []):
                try:
                    nid = int(n.get('id'))
                except Exception:
                    continue
                ntype = n.get('type')
                cfg = (n.get('config') or {})
                g.add_node(nid, type=ntype, config=cfg)
                node_configs[nid] = cfg
            for e in (etl_content.get('edges') or []):
                try:
                    g.add_edge(int(e.get('source')), int(e.get('target')))
                except Exception:
                    continue
            return g, node_configs

        def _apply_overrides(node_configs: Dict[int, Dict[str, Any]], overrides: Optional[Dict[str, Any]]) -> None:
            if not overrides:
                return
            for k, v in overrides.items():
                try:
                    node_str, key = str(k).split('.', 1)
                    nid = int(node_str)
                    if nid in node_configs:
                        node_configs[nid][key] = v
                except Exception:
                    continue

        def _open_etl_log(etl_name: str):
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            dir_path = os.path.join(self.logs_root, 'etls', etl_name or 'etl')
            os.makedirs(dir_path, exist_ok=True)
            path = os.path.join(dir_path, f'{ts}.log')
            f = open(path, 'a', encoding='utf-8')
            return path, f

        @app.post('/etl/run')
        async def etl_run(request: Request, _auth=Depends(auth_dependency)):
            body = await request.json()
            etl_id = (body or {}).get('etl_id')
            overrides = (body or {}).get('overrides') or {}
            if not etl_id:
                raise HTTPException(status_code=400, detail='etl_id requerido')
            # Buscar ETL
            etl_doc = None
            for e in (self.project.get('etls') or []):
                if str(e.get('id')) == str(etl_id):
                    etl_doc = e
                    break
            if not etl_doc:
                raise HTTPException(status_code=404, detail=f"ETL '{etl_id}' no encontrada")
            name = str(etl_doc.get('name') or etl_doc.get('id') or 'etl')
            log_path, fh = _open_etl_log(name)
            def write(msg: str):
                try:
                    fh.write(f"{datetime.now().isoformat(timespec='seconds')} | {msg}\n")
                    fh.flush()
                except Exception:
                    pass
                self._log(f"[ETL {etl_id}] {msg}")
            try:
                write(f"[ETL] inicio '{name}'")
                content = etl_doc.get('content') or {}
                g, node_cfgs = _build_graph_from_etl_content(content)
                _apply_overrides(node_cfgs, overrides)
                eng = ETLEngine()
                eng.set_pipeline(g, node_cfgs)
                res = eng.execute_pipeline()
                ok = (res is not False)
                write(f"[ETL] {'OK' if ok else 'FAILED'}")
                try:
                    runs = self.project.setdefault('runs', []) if isinstance(self.project, dict) else None
                    if isinstance(runs, list):
                        runs.append({'type': 'etl', 'id': etl_id, 'name': name, 'ok': bool(ok), 'log_path': log_path, 'ts': datetime.now().isoformat(timespec='seconds')})
                except Exception:
                    pass
                return {'ok': ok, 'log_path': log_path}
            except Exception as e:
                write(f"[ERROR] {e}")
                return JSONResponse(status_code=500, content={'ok': False, 'error': str(e), 'log_path': log_path})
            finally:
                try:
                    fh.close()
                except Exception:
                    pass

        @app.post('/job/run')
        async def job_run(request: Request, _auth=Depends(auth_dependency)):
            body = await request.json()
            job_id = (body or {}).get('job_id')
            if not job_id:
                raise HTTPException(status_code=400, detail='job_id requerido')
            job_doc = None
            for j in (self.project.get('jobs') or []):
                if str(j.get('id')) == str(job_id):
                    job_doc = j
                    break
            if not job_doc:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' no encontrado")
            jr = JobRunner(self.project, self.logs_root, ui_writer=lambda m: self._log(f"[JOB {job_id}] {m}"))
            try:
                result = jr.run_job(job_doc)
                try:
                    runs = self.project.setdefault('runs', []) if isinstance(self.project, dict) else None
                    if isinstance(runs, list):
                        runs.append({'type': 'job', 'id': job_id, 'name': job_doc.get('name') or job_id, 'ok': bool(result.get('success')), 'log_path': result.get('log_path'), 'ts': datetime.now().isoformat(timespec='seconds')})
                except Exception:
                    pass
                return {'ok': bool(result.get('success')), 'log_path': result.get('log_path'), 'errors': result.get('errors')}
            except Exception as e:
                return JSONResponse(status_code=500, content={'ok': False, 'error': str(e)})

        return app

    # ---- Control ----
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        port = int(self.service.get('port') or (self.project.get('defaults', {}).get('services', {}).get('port', 8080)))
        app = self._make_app()
        config = uvicorn.Config(app=app, host='127.0.0.1', port=port, log_level='info')
        server = uvicorn.Server(config)
        self._server = server

        def _run():
            try:
                self._running.set()
                self._log(f"[SERVICE] starting on 127.0.0.1:{port}")
                server.run()
            except Exception as e:
                self._log(f"[SERVICE] error: {e}")
            finally:
                self._running.clear()
                self._log("[SERVICE] stopped")

        t = threading.Thread(target=_run, daemon=True)
        self._thread = t
        t.start()
        # Small wait to give server a chance to boot
        time.sleep(0.2)

    def stop(self) -> None:
        if self._server is not None:
            try:
                self._server.should_exit = True
            except Exception:
                pass
        # Wait for thread to finish a bit
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def is_running(self) -> bool:
        return bool(self._running.is_set())
