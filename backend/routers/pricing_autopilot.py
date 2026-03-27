from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.services.pricing_autopilot_service import run_pricing_autopilot_simulation

router = APIRouter()


@router.post("/api/pricing/autopilot/simulate")
async def pricing_autopilot_simulate():
    try:
        return await run_pricing_autopilot_simulation()
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Не удалось выполнить simulate-run автопилота: {exc}"}, status_code=500)
