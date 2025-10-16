import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Callable

import networkx as nx

from .etl_engine import ETLEngine


def _build_graph_from_etl_content(etl_content: Dict[str, Any]) -> Tuple[nx.DiGraph, Dict[int, Dict[str, Any]]]:
    """Construye el grafo y el dict de configuraciones desde un contenido de ETL embebido.
    Espera keys: 'nodes' (lista de {id, type, config, ...}), 'edges' (lista de {source, target}).
    """
    g = nx.DiGraph()
    node_configs: Dict[int, Dict[str, Any]] = {}
    for n in etl_content.get("nodes", []):
        try:
            nid = int(n.get("id"))
        except Exception:
            continue
        ntype = n.get("type")
        cfg = (n.get("config") or {})
        g.add_node(nid, type=ntype, config=cfg)
        node_configs[nid] = cfg
    for e in etl_content.get("edges", []):
        try:
            g.add_edge(int(e.get("source")), int(e.get("target")))
        except Exception:
            continue
    return g, node_configs


def _apply_overrides(node_configs: Dict[int, Dict[str, Any]], overrides: Optional[Dict[str, Any]]) -> None:
    """Aplica overrides del tipo "<nodeId>.<key>": value sobre node_configs (in-place)."""
    if not overrides:
        return
    for k, v in overrides.items():
        try:
            node_str, key = str(k).split(".", 1)
            nid = int(node_str)
            if nid in node_configs:
                node_configs[nid][key] = v
        except Exception:
            continue


class JobRunner:
    """Ejecuta Jobs definidos en el .fetl con etapas secuenciales/paralelas.

    Job esperado (subset):
    {
      id, name, max_parallel?, on_error: "stop"|"continue",
      stages: [ { parallel: bool, steps: [ { etl_id, overrides? } ] } ]
    }
    """

    def __init__(self, project: Dict[str, Any], logs_root: str, ui_writer: Optional[Callable[[str], None]] = None):
        self.project = project
        self.logs_root = logs_root
        self.ui_writer = ui_writer
        self._stop_event = threading.Event()
        self._active_lock = threading.Lock()
        self._active_engines: List[ETLEngine] = []
        # Runners de servicios iniciados por steps del Job (clave: service_id)
        self._service_runners: Dict[str, Any] = {}

    # ---- Control ----
    def request_stop(self):
        self._stop_event.set()
        with self._active_lock:
            for eng in list(self._active_engines):
                try:
                    eng.request_stop()
                except Exception:
                    pass

    # ---- Ejecución ----
    def run_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        name = job.get("name") or job.get("id") or "job"
        log_path, log_file = self._open_job_log(name)

        def write(msg: str):
            try:
                line = f"{datetime.now().isoformat(timespec='seconds')} | {msg}\n"
                log_file.write(line)
                log_file.flush()
            except Exception:
                pass
            if self.ui_writer:
                try:
                    self.ui_writer(msg)
                except Exception:
                    pass

        write(f"[JOB] Inicio '{name}'")
        success_overall = True
        errors: List[str] = []

        defaults = (self.project.get('defaults') or {}) if isinstance(self.project, dict) else {}
        default_parallel = int(defaults.get('max_parallel') or 1)
        on_error_default = str(job.get('on_error') or 'stop').lower()

        for stage_idx, stage in enumerate(job.get('stages') or []):
            if self._stop_event.is_set():
                write("[JOB] Detenido por el usuario")
                success_overall = False
                break

            parallel = bool(stage.get('parallel'))
            steps = stage.get('steps') or []
            effective_parallel = int(job.get('max_parallel') or default_parallel)
            write(f"[STAGE {stage_idx+1}] parallel={parallel} steps={len(steps)} max_workers={effective_parallel}")

            if not steps:
                continue

            if parallel:
                # Ejecutar todos los steps de la etapa en paralelo
                with ThreadPoolExecutor(max_workers=max(1, min(len(steps), effective_parallel))) as ex:
                    futures = []
                    for s in steps:
                        etl_id = s.get('etl_id')
                        svc_id = s.get('service_id')
                        if etl_id:
                            overrides = s.get('overrides') or {}
                            etl_doc = self._find_etl(etl_id)
                            if not etl_doc:
                                msg = f"ETL no encontrada: {etl_id}"
                                write(f"[ERROR] {msg}")
                                errors.append(msg)
                                success_overall = False
                                continue
                            futures.append(ex.submit(self._run_single_etl, etl_doc, overrides, write))
                        elif svc_id:
                            futures.append(ex.submit(self._run_service_action, s, write))
                        else:
                            write("[WARN] Step inválido (falta etl_id o service_id)")
                    for fut in as_completed(futures):
                        ok, info = fut.result()
                        if not ok:
                            success_overall = False
                            errors.append(info)
                            if on_error_default == 'stop':
                                write("[STAGE] Error y política stop -> solicitando stop")
                                self.request_stop()
                                break
                if self._stop_event.is_set():
                    break
            else:
                # Secuencial dentro de la etapa
                for s in steps:
                    if self._stop_event.is_set():
                        break
                    etl_id = s.get('etl_id')
                    svc_id = s.get('service_id')
                    if etl_id:
                        overrides = s.get('overrides') or {}
                        etl_doc = self._find_etl(etl_id)
                        if not etl_doc:
                            msg = f"ETL no encontrada: {etl_id}"
                            write(f"[ERROR] {msg}")
                            errors.append(msg)
                            success_overall = False
                            if on_error_default == 'stop':
                                self.request_stop()
                                break
                            continue
                        ok, info = self._run_single_etl(etl_doc, overrides, write)
                    elif svc_id:
                        ok, info = self._run_service_action(s, write)
                    else:
                        write("[WARN] Step inválido (falta etl_id o service_id)")
                        ok, info = False, "step invalido"
                    if not ok:
                        success_overall = False
                        errors.append(info)
                        if on_error_default == 'stop':
                            self.request_stop()
                            break
            if self._stop_event.is_set():
                break

        if self._stop_event.is_set():
            write("[JOB] Finalizado con STOP solicitado")
        else:
            write("[JOB] Finalizado")

        try:
            log_file.close()
        except Exception:
            pass
        return {"success": success_overall, "errors": errors, "log_path": log_path}

    # ---- Internos ----
    def _open_job_log(self, job_name: str) -> Tuple[str, Any]:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_dir = os.path.join(self.logs_root, "jobs", job_name or "job")
        os.makedirs(job_dir, exist_ok=True)
        log_path = os.path.join(job_dir, f"{ts}.log")
        f = open(log_path, "a", encoding="utf-8")
        return log_path, f

    def _find_etl(self, etl_id: str) -> Optional[Dict[str, Any]]:
        for e in (self.project.get('etls') or []):
            if str(e.get('id')) == str(etl_id):
                return e
        return None

    def _find_service(self, service_id: str) -> Optional[Dict[str, Any]]:
        for s in (self.project.get('services') or []):
            if str(s.get('id')) == str(service_id):
                return s
        return None

    def _run_service_action(self, step: Dict[str, Any], write: Callable[[str], None]) -> Tuple[bool, str]:
        """Ejecuta una acción de servicio: {'service_id': ID, 'action': 'start'|'stop'}.
        Devuelve (ok, info)."""
        try:
            svc_id = str(step.get('service_id') or '')
            action = str(step.get('action') or 'start').lower()
            if not svc_id:
                return False, 'service_id vacío'
            svc_doc = self._find_service(svc_id)
            if not svc_doc:
                write(f"[SERVICE] no encontrado: {svc_id}")
                return False, f"service {svc_id} not found"
            # Import dinámico para evitar ciclo
            try:
                from core.service_runner import ServiceRunner  # type: ignore
            except Exception as e:
                write(f"[SERVICE] import error: {e}")
                return False, str(e)
            if action == 'stop':
                runner = self._service_runners.get(svc_id)
                if not runner:
                    write(f"[SERVICE {svc_id}] no estaba iniciado")
                    return True, 'no-op'
                try:
                    runner.stop()
                    write(f"[SERVICE {svc_id}] stopped")
                    return True, 'stopped'
                except Exception as e:
                    write(f"[SERVICE {svc_id}] stop error: {e}")
                    return False, str(e)
            else:
                # start por defecto
                runner = self._service_runners.get(svc_id)
                if not runner:
                    runner = ServiceRunner(self.project, svc_doc, self.logs_root, ui_writer=write)
                    self._service_runners[svc_id] = runner
                try:
                    runner.start()
                    write(f"[SERVICE {svc_id}] started")
                    return True, 'started'
                except Exception as e:
                    write(f"[SERVICE {svc_id}] start error: {e}")
                    return False, str(e)
        except Exception as e:
            try:
                write(f"[SERVICE] action error: {e}")
            except Exception:
                pass
            return False, str(e)

    def _run_single_etl(self,
                        etl_doc: Dict[str, Any],
                        overrides: Optional[Dict[str, Any]],
                        write: Callable[[str], None]) -> Tuple[bool, str]:
        content = etl_doc.get('content') or {}
        g, node_cfgs = _build_graph_from_etl_content(content)
        _apply_overrides(node_cfgs, overrides)

        engine = ETLEngine()
        # Registrar engine para stop()
        with self._active_lock:
            self._active_engines.append(engine)
        try:
            # Logs básicos
            write(f"[ETL {etl_doc.get('id')}] inicio")
            engine.set_pipeline(g, node_cfgs)
            res = engine.execute_pipeline()
            ok = (res is not False)
            write(f"[ETL {etl_doc.get('id')}] {'OK' if ok else 'FAILED'}")
            return ok, ("OK" if ok else "FAILED")
        except Exception as e:
            write(f"[ETL {etl_doc.get('id')}] ERROR: {e}")
            return False, str(e)
        finally:
            with self._active_lock:
                try:
                    self._active_engines.remove(engine)
                except ValueError:
                    pass
