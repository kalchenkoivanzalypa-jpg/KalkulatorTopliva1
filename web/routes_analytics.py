"""Публичная аналитика: тренд, сравнение и заявки."""
from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from bot.handlers import (
    MAX_AUTO_DISTANCE_KM,
    MAX_RAIL_DISTANCE_KM,
    find_nearest_basises,
    notify_managers_about_lead,
    send_order_to_email,
)
from db.database import Basis, CityDestination, Lead, Product, ProductBasisPrice, UserDestination, UserRequest
from utils import canonical_fuel_display_name
from utils.market_price_freshness import pick_best_product_basis_price_row
from utils.rail_logistics import find_rail_station_for_destination, is_sakhalin_destination
from web.deps import DbSession, optional_session_user, require_email_user
from web.products_util import list_products_for_basis, list_products_for_calc
from web.services.calc_service import finalize_calculation, resolve_destination_to_id
from web.jinja_env import templates
from web.services.analytics_service import (
    compute_compare_three,
    compute_matrix,
    compute_trend,
    search_basises,
)
from analytics.metrics import compute_final_score_breakdown, compute_final_score_and_signal, compute_metrics_30d, load_series_30d
from utils import normalize_city_name_key

router = APIRouter()

PAGE_SIZE = 12
MATRIX_MAX_BASISES = 5
MATRIX_MAX_DESTINATIONS = 5


def _a(request: Request) -> dict:
    return request.session.get("analytics") or {}


def _set_a(request: Request, data: dict) -> None:
    request.session["analytics"] = data


@router.get("/analytics", response_class=HTMLResponse)
async def analytics_menu(request: Request):
    return templates.TemplateResponse("analytics_menu.html", {"request": request})


@router.get("/analytics/rating", response_class=HTMLResponse)
async def rating_start(request: Request, session: DbSession):
    # Тот же список канонических марок и тот же representative product_id, что в /calc
    products = await list_products_for_calc(session)
    return templates.TemplateResponse(
        "analytics_rating.html",
        {"request": request, "products": products},
    )


@router.get("/analytics/rating/result", response_class=HTMLResponse)
async def rating_result_get(
    request: Request,
    session: DbSession,
    product_id: int,
    destination: str,
):
    """GET версия результата: нужна для возврата после подписок (PRG)."""
    session_user = await optional_session_user(request, session)
    logged_in = bool(session_user and session_user.email)
    t = (destination or "").strip()
    if not t:
        return templates.TemplateResponse(
            "analytics_rating_result.html",
            {"request": request, "items": [], "destination": "", "product_name": "—", "logged_in": logged_in},
        )

    product = await session.get(Product, int(product_id))
    product_name = canonical_fuel_display_name(product.name) if product else "—"

    dest_id, dest_lat, dest_lon, dest_key, dest_is_station = await resolve_destination_to_id(session, t)
    if dest_id is None:
        return templates.TemplateResponse(
            "analytics_rating_result.html",
            {
                "request": request,
                "items": [],
                "destination": t,
                "product_name": product_name,
                "logged_in": logged_in,
            },
        )

    nearest = await find_nearest_basises(
        session,
        float(dest_lat),
        float(dest_lon),
        int(product_id),
        limit=12,
        max_distance_rail=MAX_RAIL_DISTANCE_KM,
        max_distance_auto=MAX_AUTO_DISTANCE_KM,
        destination_name_key=dest_key,
        destination_raw=t,
        rail_only=bool(dest_is_station),
    )
    if not nearest and dest_is_station:
        nearest = await find_nearest_basises(
            session,
            float(dest_lat),
            float(dest_lon),
            int(product_id),
            limit=12,
            max_distance_rail=MAX_RAIL_DISTANCE_KM,
            max_distance_auto=MAX_AUTO_DISTANCE_KM,
            destination_name_key=dest_key,
            destination_raw=t,
            rail_only=False,
        )

    def _fmt_rub(x: float) -> str:
        return f"{float(x):,.0f}".replace(",", " ")

    items = []
    totals: list[float] = []
    entry_scores: list[float] = []
    vols: list[float] = []

    computed = []
    for it in nearest:
        price = it["price"]
        basis = it["basis"]
        code = getattr(price, "instrument_code", None)
        series30 = await load_series_30d(session, str(code)) if code else []
        m30 = compute_metrics_30d(series30) if series30 else None

        base_p = float(getattr(price, "current_price", 0) or 0)
        delivery_p = float(it.get("delivery_cost_per_ton", 0) or 0)
        total = float(it["total_cost_per_ton"])
        totals.append(total)
        if m30 is not None:
            entry_scores.append(float(m30.entry_score))
            vols.append(float(m30.volatility30))

        computed.append((it, basis, m30, total))

    t_min = min(totals) if totals else 0.0
    t_max = max(totals) if totals else 0.0
    e_min = min(entry_scores) if entry_scores else 0.0
    e_max = max(entry_scores) if entry_scores else 0.0
    v_min = min(vols) if vols else 0.0
    v_max = max(vols) if vols else 0.0

    for it, basis, m30, total in computed:
        p_row = it.get("price")
        base_row = float(getattr(p_row, "current_price", 0) or 0)
        del_row = float(it.get("delivery_cost_per_ton", 0) or 0)
        bd = compute_final_score_breakdown(
            total_cost_per_ton=float(total),
            metrics=m30,
            peer_total_min=float(t_min),
            peer_total_max=float(t_max),
            peer_entry_min=float(e_min),
            peer_entry_max=float(e_max),
            peer_vol_min=float(v_min),
            peer_vol_max=float(v_max),
        )
        final, sig = bd.final, bd.signal

        entry_score = f"{float(m30.entry_score):.1f}" if m30 else "—"
        range_pos30 = f"{float(m30.range_pos30):.2f}" if (m30 and m30.range_pos30 is not None) else "—"
        z30 = f"{float(m30.z30):+.2f}" if (m30 and m30.z30 is not None) else "—"
        liq = f"{float(m30.liquidity_score):.0f}/100" if m30 else "—"
        price = it.get("price")
        code = (getattr(price, "instrument_code", None) or "").strip().upper()
        items.append(
            {
                "signal": {"BUY": "Покупать", "WATCH": "Наблюдать", "AVOID": "Избегать"}.get(sig, sig),
                "final_score": f"{final:.0f}",
                "basis_name": basis.name,
                "transport": "Ж/Д" if (it["transport_type"] == "rail") else "Авто",
                "total_per_ton": _fmt_rub(total),
                "price_hint": (
                    f"На базисе: {_fmt_rub(base_row)} ₽/т · Доставка: {_fmt_rub(del_row)} ₽/т · Итого с доставкой"
                ),
                "entry_score": entry_score,
                "range_pos30": range_pos30,
                "z30": z30,
                "liq": liq,
                "instrument_code": code,
                "product_id": int(product_id),
                "basis_id": int(basis.id),
                "_sort": float(final),
            }
        )

    items.sort(key=lambda x: x["_sort"], reverse=True)
    for x in items:
        x.pop("_sort", None)

    return templates.TemplateResponse(
        "analytics_rating_result.html",
        {"request": request, "items": items, "destination": t, "product_name": product_name, "logged_in": logged_in},
    )


@router.post("/analytics/rating/result", response_class=HTMLResponse)
async def rating_result_post(
    request: Request,
    product_id: int = Form(...),
    destination: str = Form(...),
):
    """PRG: после POST редиректим на GET с параметрами."""
    t = (destination or "").strip()
    if not t:
        return RedirectResponse("/analytics/rating", status_code=302)
    # destination в query string
    from urllib.parse import quote
    return RedirectResponse(
        f"/analytics/rating/result?product_id={int(product_id)}&destination={quote(t)}",
        status_code=302,
    )


# ----- Trend -----


@router.get("/analytics/trend", response_class=HTMLResponse)
async def trend_start(request: Request):
    _set_a(request, {"flow": "trend"})
    return templates.TemplateResponse(
        "analytics_trend_search.html",
        {"request": request, "query": "", "offset": 0, "basises": [], "total": 0, "error": None},
    )


@router.post("/analytics/trend/search", response_class=HTMLResponse)
async def trend_search(
    request: Request,
    session: DbSession,
    q: str = Form(""),
    offset: int = Form(0),
):
    _set_a(request, {"flow": "trend"})
    try:
        offset = max(0, int(offset))
    except ValueError:
        offset = 0
    page, total = await search_basises(session, q, offset=offset, page_size=PAGE_SIZE)
    return templates.TemplateResponse(
        "analytics_trend_search.html",
        {
            "request": request,
            "query": q,
            "offset": offset,
            "basises": page,
            "total": total,
            "page_size": PAGE_SIZE,
            "error": None,
        },
    )


@router.get("/analytics/trend/basis/{basis_id}", response_class=HTMLResponse)
async def trend_pick_product(request: Request, session: DbSession, basis_id: int):
    data = _a(request)
    data["flow"] = "trend"
    data["trend_basis_id"] = basis_id
    _set_a(request, data)

    basis = await session.get(Basis, basis_id)
    products = await list_products_for_basis(session, basis_id=basis_id)
    return templates.TemplateResponse(
        "analytics_trend_products.html",
        {"request": request, "basis": basis, "products": products, "error": None},
    )


@router.post("/analytics/trend/result", response_class=HTMLResponse)
async def trend_result(
    request: Request,
    session: DbSession,
    product_id: int = Form(...),
    basis_id: int = Form(...),
):
    return RedirectResponse(
        f"/analytics/trend/result?basis_id={int(basis_id)}&product_id={int(product_id)}",
        status_code=302,
    )


@router.get("/analytics/trend/result", response_class=HTMLResponse)
async def trend_result_get(
    request: Request,
    session: DbSession,
    basis_id: int,
    product_id: int,
):
    session_user = await optional_session_user(request, session)
    logged_in = bool(session_user and session_user.email)
    tr = await compute_trend(session, int(basis_id), int(product_id))
    if not tr:
        basis = await session.get(Basis, int(basis_id))
        products = await list_products_for_basis(session, basis_id=int(basis_id))
        return templates.TemplateResponse(
            "analytics_trend_products.html",
            {
                "request": request,
                "basis": basis,
                "products": products,
                "error": "Нет истории по коду на СПбМТСБ — импортируйте бюллетени.",
            },
        )
    return templates.TemplateResponse(
        "analytics_trend_result.html",
        {"request": request, "tr": tr, "basis_id": int(basis_id), "product_id": int(product_id), "logged_in": logged_in},
    )


# ----- Compare -----


@router.get("/analytics/compare", response_class=HTMLResponse)
async def compare_start(request: Request, session: DbSession):
    _set_a(
        request,
        {
            "flow": "compare",
            "product_id": None,
            "basis_ids": [],
            "basis_names": [],
        },
    )
    products = await list_products_for_calc(session)
    return templates.TemplateResponse(
        "analytics_compare_products.html",
        {"request": request, "products": products},
    )


@router.post("/analytics/compare/product", response_class=HTMLResponse)
async def compare_set_product(request: Request, product_id: int = Form(...)):
    data = _a(request)
    data.update(
        {
            "flow": "compare",
            "product_id": int(product_id),
            "basis_ids": [],
            "basis_names": [],
        }
    )
    _set_a(request, data)
    return RedirectResponse("/analytics/compare/basis-search", status_code=302)


@router.get("/analytics/compare/basis-search", response_class=HTMLResponse)
async def compare_basis_search_get(request: Request):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    if len(data.get("basis_ids") or []) >= 3:
        return RedirectResponse("/analytics/compare/destination", status_code=302)
    return templates.TemplateResponse(
        "analytics_compare_basis_search.html",
        {
            "request": request,
            "query": "",
            "offset": 0,
            "basises": [],
            "total": 0,
            "chosen": list(zip(data.get("basis_ids") or [], data.get("basis_names") or [])),
            "n_needed": 3 - len(data.get("basis_ids") or []),
        },
    )


@router.post("/analytics/compare/basis-search", response_class=HTMLResponse)
async def compare_basis_search_post(
    request: Request,
    session: DbSession,
    q: str = Form(""),
    offset: int = Form(0),
):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    try:
        offset = max(0, int(offset))
    except ValueError:
        offset = 0
    page, total = await search_basises(session, q, offset=offset, page_size=PAGE_SIZE)
    chosen = list(zip(data.get("basis_ids") or [], data.get("basis_names") or []))
    return templates.TemplateResponse(
        "analytics_compare_basis_search.html",
        {
            "request": request,
            "query": q,
            "offset": offset,
            "basises": page,
            "total": total,
            "page_size": PAGE_SIZE,
            "chosen": chosen,
            "n_needed": 3 - len(data.get("basis_ids") or []),
        },
    )


@router.post("/analytics/compare/pick-basis", response_class=HTMLResponse)
async def compare_pick_basis(request: Request, session: DbSession, basis_id: int = Form(...)):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/compare", status_code=302)
    basis = await session.get(Basis, basis_id)
    if not basis:
        return RedirectResponse("/analytics/compare/basis-search", status_code=302)
    ids = list(data.get("basis_ids") or [])
    names = list(data.get("basis_names") or [])
    if basis_id not in ids:
        ids.append(basis_id)
        names.append(basis.name)
    data["basis_ids"] = ids
    data["basis_names"] = names
    _set_a(request, data)
    if len(ids) < 3:
        return RedirectResponse("/analytics/compare/basis-search", status_code=302)
    return RedirectResponse("/analytics/compare/destination", status_code=302)


@router.get("/analytics/compare/destination", response_class=HTMLResponse)
async def compare_destination_get(request: Request):
    data = _a(request)
    if len(data.get("basis_ids") or []) != 3:
        return RedirectResponse("/analytics/compare", status_code=302)
    return templates.TemplateResponse(
        "analytics_compare_destination.html",
        {"request": request, "error": None},
    )


@router.post("/analytics/compare/result", response_class=HTMLResponse)
async def compare_result(
    request: Request,
    session: DbSession,
    destination: str = Form(...),
):
    data = _a(request)
    pid = data.get("product_id")
    basis_ids = data.get("basis_ids") or []
    if not pid or len(basis_ids) != 3:
        return RedirectResponse("/analytics/compare", status_code=302)

    # PRG: запоминаем назначение и редиректим на GET-версию результата.
    data["compare_destination"] = (destination or "").strip()
    _set_a(request, data)
    return RedirectResponse("/analytics/compare/result", status_code=302)


@router.get("/analytics/compare/result", response_class=HTMLResponse)
async def compare_result_get(
    request: Request,
    session: DbSession,
):
    data = _a(request)
    pid = data.get("product_id")
    basis_ids = data.get("basis_ids") or []
    destination = str(data.get("compare_destination") or "").strip()
    if not pid or len(basis_ids) != 3:
        return RedirectResponse("/analytics/compare", status_code=302)
    if not destination:
        return RedirectResponse("/analytics/compare/destination", status_code=302)

    session_user = await optional_session_user(request, session)
    logged_in = bool(session_user and session_user.email)

    res = await compute_compare_three(
        session,
        product_id=int(pid),
        basis_ids=[int(x) for x in basis_ids],
        destination_text=destination,
    )
    if not res:
        return templates.TemplateResponse(
            "analytics_compare_destination.html",
            {
                "request": request,
                "error": "Не удалось определить координаты назначения. Уточните населённый пункт или станцию.",
            },
        )
    return templates.TemplateResponse(
        "analytics_compare_result.html",
        {
            "request": request,
            "res": res,
            "product_id": int(pid),
            "basis_ids": [int(x) for x in basis_ids],
            "destination": destination,
            "logged_in": logged_in,
        },
    )


# ----- Matrix (5x5) -----


@router.get("/analytics/matrix", response_class=HTMLResponse)
async def matrix_start(request: Request, session: DbSession):
    _set_a(
        request,
        {
            "flow": "matrix",
            "product_id": None,
            "basis_ids": [],
            "basis_names": [],
            "dest_city_ids": [],
            "dest_texts": [],
        },
    )
    products = await list_products_for_calc(session)
    return templates.TemplateResponse(
        "analytics_matrix_products.html",
        {"request": request, "products": products},
    )


@router.post("/analytics/matrix/product", response_class=HTMLResponse)
async def matrix_set_product(request: Request, product_id: int = Form(...)):
    data = _a(request)
    data.update(
        {
            "flow": "matrix",
            "product_id": int(product_id),
            "basis_ids": [],
            "basis_names": [],
            "dest_city_ids": [],
            "dest_texts": [],
        }
    )
    _set_a(request, data)
    return RedirectResponse("/analytics/matrix/basis-search", status_code=302)


@router.get("/analytics/matrix/basis-search", response_class=HTMLResponse)
async def matrix_basis_search_get(request: Request):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/matrix", status_code=302)
    return templates.TemplateResponse(
        "analytics_matrix_basis_search.html",
        {
            "request": request,
            "query": "",
            "offset": 0,
            "basises": [],
            "total": 0,
            "chosen": list(zip(data.get("basis_ids") or [], data.get("basis_names") or [])),
            "n_needed": MATRIX_MAX_BASISES - len(data.get("basis_ids") or []),
            "max_n": MATRIX_MAX_BASISES,
        },
    )


@router.post("/analytics/matrix/basis-search", response_class=HTMLResponse)
async def matrix_basis_search_post(
    request: Request,
    session: DbSession,
    q: str = Form(""),
    offset: int = Form(0),
):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/matrix", status_code=302)
    try:
        offset = max(0, int(offset))
    except ValueError:
        offset = 0

    page, total = await search_basises(session, q, offset=offset, page_size=PAGE_SIZE)
    chosen = list(zip(data.get("basis_ids") or [], data.get("basis_names") or []))
    return templates.TemplateResponse(
        "analytics_matrix_basis_search.html",
        {
            "request": request,
            "query": q,
            "offset": offset,
            "basises": page,
            "total": total,
            "page_size": PAGE_SIZE,
            "chosen": chosen,
            "n_needed": MATRIX_MAX_BASISES - len(data.get("basis_ids") or []),
            "max_n": MATRIX_MAX_BASISES,
        },
    )


@router.post("/analytics/matrix/pick-basis", response_class=HTMLResponse)
async def matrix_pick_basis(request: Request, session: DbSession, basis_id: int = Form(...)):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/matrix", status_code=302)
    basis = await session.get(Basis, basis_id)
    if not basis:
        return RedirectResponse("/analytics/matrix/basis-search", status_code=302)

    ids = list(data.get("basis_ids") or [])
    names = list(data.get("basis_names") or [])
    if basis_id not in ids and len(ids) < MATRIX_MAX_BASISES:
        ids.append(basis_id)
        names.append(basis.name)
    data["basis_ids"] = ids
    data["basis_names"] = names
    _set_a(request, data)

    if len(ids) < MATRIX_MAX_BASISES:
        return RedirectResponse("/analytics/matrix/basis-search", status_code=302)
    return RedirectResponse("/analytics/matrix/destinations", status_code=302)


@router.get("/analytics/matrix/destinations", response_class=HTMLResponse)
async def matrix_destinations_get(request: Request, session: DbSession):
    data = _a(request)
    if not data.get("product_id"):
        return RedirectResponse("/analytics/matrix", status_code=302)
    if len(data.get("basis_ids") or []) < 1:
        return RedirectResponse("/analytics/matrix/basis-search", status_code=302)

    session_user = await optional_session_user(request, session)
    logged_in = bool(session_user and session_user.email)

    saved: list[dict] = []
    if logged_in and session_user is not None:
        rows = (
            await session.execute(
                select(UserDestination, CityDestination)
                .join(CityDestination, CityDestination.id == UserDestination.city_destination_id)
                .where(UserDestination.user_id == int(session_user.id))
                .where(UserDestination.is_active.is_(True))
                .order_by(UserDestination.created_at.desc())
                .limit(200)
            )
        ).all()
        for ud, cd in rows:
            saved.append(
                {
                    "city_id": int(cd.id),
                    "label": (str(getattr(ud, "label", "") or "").strip() or str(cd.name)),
                    "name": str(cd.name),
                }
            )

    return templates.TemplateResponse(
        "analytics_matrix_destinations.html",
        {
            "request": request,
            "chosen_basises": list(zip(data.get("basis_ids") or [], data.get("basis_names") or [])),
            "saved": saved,
            "logged_in": logged_in,
            "max_destinations": MATRIX_MAX_DESTINATIONS,
            "error": None,
        },
    )


@router.post("/analytics/matrix/result", response_class=HTMLResponse)
async def matrix_result_post(
    request: Request,
    session: DbSession,
    dest_city_ids: list[int] | None = Form(None),
    dest_texts: str = Form(""),
):
    data = _a(request)
    pid = data.get("product_id")
    basis_ids = data.get("basis_ids") or []
    if not pid or len(basis_ids) < 1:
        return RedirectResponse("/analytics/matrix", status_code=302)

    # manual lines: 1-5
    manual: list[str] = []
    for line in (dest_texts or "").splitlines():
        t = (line or "").strip()
        if t:
            manual.append(t)
    manual = manual[:MATRIX_MAX_DESTINATIONS]

    # saved city ids: cap to 5
    ids: list[int] = []
    if dest_city_ids:
        for x in dest_city_ids[:MATRIX_MAX_DESTINATIONS]:
            try:
                ids.append(int(x))
            except Exception:
                continue

    # combine, keep order: saved first, then manual, total <=5
    combined_city_ids = ids[:MATRIX_MAX_DESTINATIONS]
    remaining = MATRIX_MAX_DESTINATIONS - len(combined_city_ids)
    combined_texts = manual[: max(0, remaining)]

    if not combined_city_ids and not combined_texts:
        return RedirectResponse("/analytics/matrix/destinations?err=dest", status_code=302)

    data["dest_city_ids"] = combined_city_ids
    data["dest_texts"] = combined_texts
    _set_a(request, data)
    return RedirectResponse("/analytics/matrix/result", status_code=302)


@router.get("/analytics/matrix/result", response_class=HTMLResponse)
async def matrix_result_get(request: Request, session: DbSession):
    data = _a(request)
    pid = data.get("product_id")
    basis_ids = data.get("basis_ids") or []
    if not pid or len(basis_ids) < 1:
        return RedirectResponse("/analytics/matrix", status_code=302)

    dest_city_ids = [int(x) for x in (data.get("dest_city_ids") or [])][:MATRIX_MAX_DESTINATIONS]
    dest_texts = [str(x).strip() for x in (data.get("dest_texts") or []) if str(x).strip()][
        :MATRIX_MAX_DESTINATIONS
    ]
    if not dest_city_ids and not dest_texts:
        return RedirectResponse("/analytics/matrix/destinations", status_code=302)

    session_user = await optional_session_user(request, session)
    logged_in = bool(session_user and session_user.email)

    # Resolve destinations to coords + station (no commits)
    resolved: list[dict] = []
    for cid in dest_city_ids:
        cd = await session.get(CityDestination, int(cid))
        if not cd:
            continue
        t = str(getattr(cd, "name", "") or "").strip()
        if not t:
            continue
        dk = normalize_city_name_key(t)
        st = await find_rail_station_for_destination(session, t, dk)
        resolved.append(
            {
                "title": t,
                "dest_id": int(cd.id),
                "lat": float(cd.latitude),
                "lon": float(cd.longitude),
                "key": dk,
                "station": st,
                "source": "saved",
            }
        )
        if len(resolved) >= MATRIX_MAX_DESTINATIONS:
            break

    for t in dest_texts:
        if len(resolved) >= MATRIX_MAX_DESTINATIONS:
            break
        dest_id, lat, lon, dk, _is_station = await resolve_destination_to_id(session, t)
        if dest_id is None:
            resolved.append(
                {
                    "title": t,
                    "error": "Не удалось определить точку",
                    "source": "manual",
                }
            )
            continue
        st = await find_rail_station_for_destination(session, t, dk)
        resolved.append(
            {
                "title": t,
                "dest_id": int(dest_id),
                "lat": float(lat),
                "lon": float(lon),
                "key": dk,
                "station": st,
                "source": "manual",
            }
        )

    res = await compute_matrix(
        session,
        product_id=int(pid),
        basis_ids=[int(x) for x in basis_ids],
        destinations=resolved,
        volume_tons=60.0,
        max_concurrency=6,
    )
    if not res:
        return RedirectResponse("/analytics/matrix/destinations?err=dest", status_code=302)

    return templates.TemplateResponse(
        "analytics_matrix_result.html",
        {
            "request": request,
            "res": res,
            "logged_in": logged_in,
        },
    )


@router.post("/analytics/matrix/save-destination", response_class=HTMLResponse)
async def matrix_save_destination(
    request: Request,
    session: DbSession,
    dest_id: int = Form(...),
    label: str = Form(""),
    next: str = Form("/analytics/matrix/result"),
):
    """
    Сохранить направление (из ручного ввода на матрице) в user_destinations.
    Требует логин по email.
    """
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse(f"/login?next={next}", status_code=302)

    cd = await session.get(CityDestination, int(dest_id))
    if not cd:
        return RedirectResponse("/analytics/matrix/result", status_code=302)

    label_s = (label or "").strip() or None
    ud = UserDestination(
        user_id=int(user.id),
        city_destination_id=int(cd.id),
        label=label_s,
        is_active=True,
    )
    session.add(ud)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        await session.execute(
            update(UserDestination)
            .where(UserDestination.user_id == int(user.id))
            .where(UserDestination.city_destination_id == int(cd.id))
            .values(is_active=True, label=label_s)
        )
        await session.commit()

    # редиректим туда, откуда пришли (обычно /analytics/matrix/result)
    return RedirectResponse(str(next or "/analytics/matrix/result"), status_code=302)


@router.post("/analytics/order")
async def analytics_order(
    request: Request,
    session: DbSession,
    basis_id: int = Form(...),
    product_id: int = Form(...),
    destination: str = Form(...),
    volume: str = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/analytics", status_code=302)

    try:
        vol = float((volume or "").replace(",", ".").strip())
        if vol <= 0:
            raise ValueError
    except Exception:
        return RedirectResponse("/analytics?err=vol", status_code=302)

    dest_text = (destination or "").strip()
    dest_id, dest_lat, dest_lon, dest_key, _dest_station_flag = await resolve_destination_to_id(
        session, dest_text
    )
    if dest_id is None:
        return RedirectResponse("/analytics?err=dest", status_code=302)

    basis = await session.get(Basis, int(basis_id))
    product_row = await session.get(Product, int(product_id))
    exclude_ai100_stub = canonical_fuel_display_name(getattr(product_row, "name", "") or "") == "АИ-100-К5"
    pbp = await pick_best_product_basis_price_row(
        session,
        basis_id=int(basis_id),
        product_ids=[int(product_id)],
        exclude_ai100_price_stub=exclude_ai100_stub,
    )
    if not basis or not pbp:
        return RedirectResponse("/analytics?err=nodata", status_code=302)

    dest_station = await find_rail_station_for_destination(session, dest_text, dest_key)
    sak = is_sakhalin_destination(dest_text, dest_key, dest_station)

    # минимальный distance для UI (finalize_calculation для rail пересчитает по ТР №4)
    from bot.handlers import calculate_distance as hav

    if (basis.transport_type or "").lower() == "rail":
        d_lat = float(dest_station.latitude) if dest_station else float(dest_lat)
        d_lon = float(dest_station.longitude) if dest_station else float(dest_lon)
        o_lat = float(basis.rail_latitude or basis.latitude)
        o_lon = float(basis.rail_longitude or basis.longitude)
        dist = hav(d_lat, d_lon, o_lat, o_lon)
        transport = "rail"
    else:
        dist = hav(float(dest_lat), float(dest_lon), float(basis.latitude), float(basis.longitude))
        transport = "auto"

    selected = {
        "distance": float(dist),
        "basis": basis,
        "price": pbp,
        "transport_type": transport,
        "rate": 0.0,
        "delivery_cost_per_ton": 0.0,
        "total_cost_per_ton": float(pbp.current_price),
        "rail_dest_station_id": int(dest_station.id) if dest_station else None,
        "rail_dest_station_name": str(dest_station.name) if dest_station else None,
        "rail_origin_station_name": None,
        "is_sakhalin_destination": bool(sak),
        "ferry_surcharge_per_ton": 0.0,
    }

    r = await finalize_calculation(
        session,
        user=user,
        product_id=int(product_id),
        destination_id=int(dest_id),
        selected=selected,
        volume=vol,
    )

    lead = Lead(user_id=user.id, request_id=r.request_id, status="email_pending", source="analytics")
    session.add(lead)
    await session.commit()

    ur = await session.get(UserRequest, int(r.request_id))
    if ur:
        try:
            await send_order_to_email(user.email or "", ur, session)
            lead.email = user.email
            lead.status = "sent"
            await session.commit()

            product = await session.get(Product, ur.product_id)
            b = await session.get(Basis, ur.basis_id)
            d = await session.get(CityDestination, ur.city_destination_id)
            lead_text = (
                f"📝 <b>Новая заявка #{ur.id}</b>\n\n"
                f"📧 Email: <b>{user.email}</b>\n"
                f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
                f"📍 {b.name if b else '—'} → {d.name if d else '—'}\n"
                f"📦 {ur.volume:g} т, {('Ж/Д' if ur.transport_type == 'rail' else 'Авто')}\n"
                f"💎 Итого: <b>{float(ur.total_price):,.0f}</b> ₽".replace(",", " ")
            )
            bot = getattr(request.app.state, "bot", None)
            if bot is not None:
                await notify_managers_about_lead(bot, lead_text)
        except Exception:
            pass

    return RedirectResponse("/cabinet?ok=lead", status_code=302)
