from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import shutil
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from apscheduler.executors.pool import ThreadPoolExecutor as APSchedulerThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

LOCAL_PG_HOST = "127.0.0.1:5433"
os.environ.setdefault("APP_DB_BACKEND", "postgres")
os.environ.setdefault("APP_DATABASE_URL", f"postgresql://maksimkiselev@{LOCAL_PG_HOST}/data_analytics_hot")
os.environ.setdefault("APP_SYSTEM_DATABASE_URL", f"postgresql://maksimkiselev@{LOCAL_PG_HOST}/data_analytics_system")
os.environ.setdefault("APP_HISTORY_DATABASE_URL", f"postgresql://maksimkiselev@{LOCAL_PG_HOST}/data_analytics_history")

from backend.routers import (
    data_sources,
    integrations as integrations_router,
    pricing as pricing_router,
    catalog as catalog_router,
    pricing_prices as pricing_prices_router,
    pricing_boost as pricing_boost_router,
    pricing_attractiveness as pricing_attractiveness_router,
    pricing_autopilot as pricing_autopilot_router,
    pricing_promos as pricing_promos_router,
    pricing_strategy as pricing_strategy_router,
    sales_coinvest as sales_coinvest_router,
    sales_elasticity as sales_elasticity_router,
    sales_overview as sales_overview_router,
)
from backend.services.db import init_db
from backend.services.store_data_model import abandon_incomplete_refresh_job_runs, init_store_data_model
from backend.services.source_tables import init_source_registry
from backend.services.storage import load_integrations, seed_sources_if_empty
from backend.services.pricing_autopilot_service import run_pricing_autopilot_simulation
from backend.services.pricing_catalog_tree_service import refresh_pricing_catalog_trees_from_sources
from backend.services.yandex_united_orders_report_service import refresh_sales_overview_cogs_sources
from backend.services.refresh_orchestrator_service import (
    bind_refresh_scheduler,
    configure_refresh_scheduler,
    ensure_refresh_jobs_defaults,
)

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"


def _env_bool(*keys: str, default: bool = False) -> bool:
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        return str(raw).strip() in {"1", "true", "TRUE", "yes", "YES"}
    return default


def _env_str(*keys: str, default: str = "") -> str:
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        return str(raw).strip()
    return default


FRONTEND_PORT = int(_env_str("FRONTEND_PORT", "NEXT_PORT", default="3000") or "3000")
FRONTEND_MODE = _env_str("FRONTEND_MODE", default="dev").lower()
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "8000"))
START_FRONTEND = _env_bool("START_FRONTEND", "NEXT_AUTOSTART", default=True)
BACKEND_RELOAD_ENABLED = _env_bool("BACKEND_RELOAD", "UVICORN_RELOAD", default=True)
ELASTICITY_SCHEDULER = BackgroundScheduler(
    executors={"default": APSchedulerThreadPoolExecutor(1)},
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 7200},
)


async def _run_startup_refreshes() -> None:
    try:
        await asyncio.to_thread(refresh_pricing_catalog_trees_from_sources)
    except Exception:
        pass
    try:
        await asyncio.to_thread(refresh_sales_overview_cogs_sources)
    except Exception:
        pass

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    init_store_data_model()
    try:
        abandon_incomplete_refresh_job_runs()
    except Exception:
        pass
    init_source_registry()
    seed_sources_if_empty()
    load_integrations()
    ensure_refresh_jobs_defaults()
    asyncio.create_task(_run_startup_refreshes())
    if not ELASTICITY_SCHEDULER.running:
        bind_refresh_scheduler(ELASTICITY_SCHEDULER)
        ELASTICITY_SCHEDULER.add_job(
            lambda: asyncio.run(run_pricing_autopilot_simulation()),
            trigger="cron",
            minute=5,
            id="pricing_autopilot_simulation",
            replace_existing=True,
        )
        configure_refresh_scheduler()
        ELASTICITY_SCHEDULER.start()
    yield
    if ELASTICITY_SCHEDULER.running:
        ELASTICITY_SCHEDULER.shutdown(wait=False)


app = FastAPI(title="Data Analytics Web API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if (BASE_DIR / "static").exists():
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

app.include_router(data_sources.router)
app.include_router(integrations_router.router)
app.include_router(pricing_router.router)
app.include_router(catalog_router.router)
app.include_router(pricing_prices_router.router)
app.include_router(pricing_boost_router.router)
app.include_router(pricing_attractiveness_router.router)
app.include_router(pricing_autopilot_router.router)
app.include_router(pricing_promos_router.router)
app.include_router(pricing_strategy_router.router)
app.include_router(sales_coinvest_router.router)
app.include_router(sales_elasticity_router.router)
app.include_router(sales_overview_router.router)

@app.get("/api/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/")
async def root_redirect():
    return RedirectResponse(url=f"http://127.0.0.1:{FRONTEND_PORT}", status_code=307)


def _start_frontend_process() -> subprocess.Popen[str] | None:
    if not START_FRONTEND:
        return None
    if not FRONTEND_DIR.exists():
        return None

    frontend_env = os.environ.copy()

    if FRONTEND_MODE == "prod":
        dist_dir = FRONTEND_DIR / "dist"
        required_build_files = [
            dist_dir / "index.html",
            dist_dir / "assets",
        ]
        has_valid_build = all(p.exists() for p in required_build_files)
        if not has_valid_build:
            if dist_dir.exists():
                shutil.rmtree(dist_dir, ignore_errors=True)
            subprocess.run(
                ["npm", "run", "build"],
                cwd=str(FRONTEND_DIR),
                env=frontend_env,
                check=True,
                text=True,
            )
        cmd = ["npm", "run", "preview", "--", "--host", "127.0.0.1", "--port", str(FRONTEND_PORT)]
    else:
        cmd = ["npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", str(FRONTEND_PORT)]

    return subprocess.Popen(
        cmd,
        cwd=str(FRONTEND_DIR),
        env=frontend_env,
        stdout=None,
        stderr=None,
        text=True,
    )


def _stop_process(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=4)
    except Exception:
        proc.kill()


def _run_dev() -> None:
    frontend_proc = _start_frontend_process()
    try:
        reload_enabled = BACKEND_RELOAD_ENABLED
        if reload_enabled:
            os.environ.setdefault("WATCHFILES_FORCE_POLLING", "true")
            use_watchfiles = importlib.util.find_spec("watchfiles") is not None
            uvicorn_kwargs = {
                "app": "main:app",
                "host": BACKEND_HOST,
                "port": BACKEND_PORT,
                "reload": True,
                "log_level": "info",
            }
            if use_watchfiles:
                uvicorn_kwargs["reload_dirs"] = [str(BASE_DIR)]
                uvicorn_kwargs["reload_includes"] = ["main.py", "backend/*"]
                uvicorn_kwargs["reload_excludes"] = ["frontend/*", "data/*", ".git/*", "__pycache__/*", "node_modules/*"]
            else:
                # Без watchfiles uvicorn использует statreload и не умеет корректно применять exclude/include.
                # Чтобы не трогать фронтенд и .next, наблюдаем только backend.
                uvicorn_kwargs["reload_dirs"] = [str(BASE_DIR / "backend")]
            uvicorn.run(**uvicorn_kwargs)
        else:
            uvicorn.run(
                app,
                host=BACKEND_HOST,
                port=BACKEND_PORT,
                reload=False,
                log_level="info",
            )
    finally:
        _stop_process(frontend_proc)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    _run_dev()
