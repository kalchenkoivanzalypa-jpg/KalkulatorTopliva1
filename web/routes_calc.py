"""Публичный расчёт стоимости (как в боте)."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from bot.handlers import find_nearest_basises, MAX_AUTO_DISTANCE_KM, MAX_RAIL_DISTANCE_KM
from db.database import Product
from web.deps import DbSession, init_guest_session, optional_session_user, require_guest_user
from web.products_util import list_products_for_calc
from web.services.calc_service import (
    finalize_calculation,
    rebuild_selected,
    resolve_destination_to_id,
    serialize_basis_item,
)

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")


@router.get("/calc", response_class=HTMLResponse)
async def calc_start(request: Request, session: DbSession):
    init_guest_session(request)
    products = await list_products_for_calc(session)
    return templates.TemplateResponse(
        "calc_product.html",
        {"request": request, "products": products, "error": None},
    )


@router.post("/calc/product", response_class=HTMLResponse)
async def calc_product(
    request: Request,
    session: DbSession,
    product_id: int = Form(...),
):
    init_guest_session(request)
    request.session["calc"] = {"product_id": product_id}
    p = await session.get(Product, product_id)
    name = p.name if p else ""
    return templates.TemplateResponse(
        "calc_destination.html",
        {
            "request": request,
            "product_id": product_id,
            "product_name": name,
            "error": None,
        },
    )


@router.post("/calc/destination", response_class=HTMLResponse)
async def calc_destination(
    request: Request,
    session: DbSession,
    destination: str = Form(...),
):
    init_guest_session(request)
    calc = request.session.get("calc") or {}
    product_id = calc.get("product_id")
    if not product_id:
        return RedirectResponse("/calc", status_code=302)

    dest_id, dest_lat, dest_lon, dest_key = await resolve_destination_to_id(session, destination.strip())
    if dest_id is None:
        p = await session.get(Product, int(product_id))
        return templates.TemplateResponse(
            "calc_destination.html",
            {
                "request": request,
                "product_id": product_id,
                "product_name": p.name if p else "",
                "error": "Не удалось определить координаты. Уточните название населённого пункта или станции.",
            },
        )

    nearby = await find_nearest_basises(
        session,
        dest_lat,
        dest_lon,
        int(product_id),
        limit=10,
        max_distance_rail=MAX_RAIL_DISTANCE_KM,
        max_distance_auto=MAX_AUTO_DISTANCE_KM,
        destination_name_key=dest_key,
        destination_raw=destination.strip(),
    )
    if not nearby:
        p = await session.get(Product, int(product_id))
        return templates.TemplateResponse(
            "calc_destination.html",
            {
                "request": request,
                "product_id": product_id,
                "product_name": p.name if p else "",
                "error": "Не найдено базисов в допустимых радиусах для выбранного топлива и пункта.",
            },
        )

    request.session["calc"] = {
        "product_id": int(product_id),
        "destination_id": dest_id,
        "destination_name": destination.strip(),
        "dest_lat": dest_lat,
        "dest_lon": dest_lon,
        "dest_key": dest_key,
    }

    return templates.TemplateResponse(
        "calc_pick_basis.html",
        {
            "request": request,
            "destination": destination.strip(),
            "items": nearby,
            "product_id": product_id,
        },
    )


@router.post("/calc/volume", response_class=HTMLResponse)
async def calc_volume_form(
    request: Request,
    session: DbSession,
    basis_id: int = Form(...),
):
    init_guest_session(request)
    calc = request.session.get("calc") or {}
    product_id = calc.get("product_id")
    dest_lat = calc.get("dest_lat")
    dest_lon = calc.get("dest_lon")
    dest_key = calc.get("dest_key")
    destination_raw = calc.get("destination_name")
    if not product_id or dest_lat is None or dest_lon is None or not dest_key or not destination_raw:
        return RedirectResponse("/calc", status_code=302)

    nearby = await find_nearest_basises(
        session,
        float(dest_lat),
        float(dest_lon),
        int(product_id),
        limit=10,
        max_distance_rail=MAX_RAIL_DISTANCE_KM,
        max_distance_auto=MAX_AUTO_DISTANCE_KM,
        destination_name_key=str(dest_key),
        destination_raw=str(destination_raw),
    )
    selected_item = None
    for it in nearby:
        if int(it["basis"].id) == int(basis_id):
            selected_item = it
            break
    if not selected_item:
        raise HTTPException(status_code=400, detail="Базис не из списка")

    calc["selected"] = serialize_basis_item(selected_item)
    request.session["calc"] = calc

    return templates.TemplateResponse(
        "calc_volume.html",
        {
            "request": request,
            "basis_id": basis_id,
            "product_id": product_id,
            "destination_name": calc.get("destination_name", ""),
        },
    )


@router.post("/calc/result", response_class=HTMLResponse)
async def calc_result(
    request: Request,
    session: DbSession,
    volume: str = Form(...),
):
    init_guest_session(request)
    calc = request.session.get("calc") or {}
    product_id = calc.get("product_id")
    destination_id = calc.get("destination_id")
    selected_raw = calc.get("selected")
    if not product_id or not destination_id or not selected_raw:
        return RedirectResponse("/calc", status_code=302)

    try:
        vol = float(volume.replace(",", ".").strip())
        if vol <= 0:
            raise ValueError
    except ValueError:
        return templates.TemplateResponse(
            "calc_volume.html",
            {
                "request": request,
                "basis_id": selected_raw.get("basis_id"),
                "product_id": product_id,
                "destination_name": calc.get("destination_name", ""),
                "error": "Введите корректный объём (тонн).",
            },
        )

    selected = await rebuild_selected(session, int(product_id), selected_raw)
    if not selected:
        raise HTTPException(status_code=400, detail="Не удалось восстановить базис")

    guest_user = await require_guest_user(request, session)
    session_user = await optional_session_user(request, session)
    user = session_user if (session_user and session_user.email) else guest_user
    try:
        result = await finalize_calculation(
            session,
            user=user,
            product_id=int(product_id),
            destination_id=int(destination_id),
            selected=selected,
            volume=vol,
        )
    except Exception as e:
        return templates.TemplateResponse(
            "calc_volume.html",
            {
                "request": request,
                "basis_id": selected_raw.get("basis_id"),
                "product_id": product_id,
                "destination_name": calc.get("destination_name", ""),
                "error": f"Ошибка расчёта: {e}",
            },
        )

    request.session.pop("calc", None)

    return templates.TemplateResponse(
        "calc_result.html",
        {
            "request": request,
            "r": result,
            "logged_in": bool(session_user and session_user.email),
        },
    )
