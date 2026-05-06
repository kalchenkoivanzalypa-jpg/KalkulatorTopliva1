"""Личный кабинет: подписки, заявки."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from bot.handlers import notify_managers_about_lead, send_order_to_email
from db.database import (
    AnomalyAlert,
    Basis,
    BasisDigestSubscription,
    CityDestination,
    Lead,
    PriceAlert,
    Product,
    ProductBasisPrice,
    TableDigestSubscription,
    UserDestination,
    UserRequest,
)
from utils import canonical_fuel_display_name
from web.deps import DbSession, require_email_user
from web.jinja_env import templates
from web.products_util import list_products_for_calc
from web.services.calc_service import resolve_destination_to_id
from web.services.table_digest_service import validate_selection_payload

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/cabinet/destinations", response_class=HTMLResponse)
async def cabinet_destinations(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    q = await session.execute(
        select(UserDestination, CityDestination)
        .join(CityDestination, CityDestination.id == UserDestination.city_destination_id)
        .where(UserDestination.user_id == int(user.id))
        .where(UserDestination.is_active.is_(True))
        .order_by(UserDestination.created_at.desc(), UserDestination.id.desc())
    )
    rows = []
    for ud, cd in q.all():
        rows.append({"ud": ud, "cd": cd})

    ok = str(request.query_params.get("ok") or "").strip()
    err = str(request.query_params.get("err") or "").strip()

    return templates.TemplateResponse(
        "cabinet_destinations.html",
        {"request": request, "user": user, "rows": rows, "ok": ok, "err": err},
    )


@router.post("/cabinet/destinations/add", response_class=HTMLResponse)
async def cabinet_destinations_add(
    request: Request,
    session: DbSession,
    destination: str = Form(...),
    label: str = Form(""),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    dest_text = (destination or "").strip()
    if not dest_text:
        return RedirectResponse("/cabinet/destinations?err=empty", status_code=302)

    dest_id, _lat, _lon, _key, _is_station = await resolve_destination_to_id(session, dest_text)
    if dest_id is None:
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)

    label_s = (label or "").strip() or None

    ud = UserDestination(
        user_id=int(user.id),
        city_destination_id=int(dest_id),
        label=label_s,
        is_active=True,
    )
    session.add(ud)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        # already exists -> reactivate / update label
        await session.execute(
            update(UserDestination)
            .where(UserDestination.user_id == int(user.id))
            .where(UserDestination.city_destination_id == int(dest_id))
            .values(is_active=True, label=label_s)
        )
        await session.commit()

    return RedirectResponse("/cabinet/destinations?ok=add", status_code=302)


@router.post("/cabinet/destinations/disable", response_class=HTMLResponse)
async def cabinet_destinations_disable(
    request: Request,
    session: DbSession,
    user_destination_id: int = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    ud = await session.get(UserDestination, int(user_destination_id))
    if not ud or int(getattr(ud, "user_id", 0) or 0) != int(user.id):
        return RedirectResponse("/cabinet/destinations?err=notfound", status_code=302)

    ud.is_active = False
    await session.commit()
    return RedirectResponse("/cabinet/destinations?ok=off", status_code=302)


@router.get("/cabinet/basis_products")
async def cabinet_basis_products(request: Request, session: DbSession, basis_id: int) -> JSONResponse:
    """Список продуктов, которые реально есть на базисе (по активным строкам ProductBasisPrice)."""
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)

    b = await session.get(Basis, int(basis_id))
    if not b or not getattr(b, "is_active", True):
        return JSONResponse({"ok": True, "items": []})

    q = await session.execute(
        select(Product.id, Product.name)
        .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
        .where(ProductBasisPrice.basis_id == int(basis_id))
        .where(ProductBasisPrice.is_active.is_(True))
        .where(ProductBasisPrice.current_price > 0)
        .where(Product.is_active.is_(True))
        .order_by(Product.name)
    )
    seen: set[int] = set()
    items: list[dict] = []
    for pid, name in q.all():
        i = int(pid)
        if i in seen:
            continue
        seen.add(i)
        items.append({"id": i, "name": str(name)})

    return JSONResponse({"ok": True, "items": items})


async def _alert_rows_for_user(session, user_id: int) -> list[dict]:
    alerts = (
        await session.execute(
            select(PriceAlert)
            .where(PriceAlert.user_id == user_id)
            .where(PriceAlert.is_active.is_(True))
            .order_by(PriceAlert.created_at.desc())
        )
    ).scalars().all()
    rows: list[dict] = []
    for a in alerts:
        pr = await session.get(Product, a.product_id)
        b = await session.get(Basis, getattr(a, "basis_id", None)) if getattr(a, "basis_id", None) else None
        rows.append({"a": a, "product_name": pr.name if pr else "—", "basis_name": (b.name if b else None)})
    return rows


async def _digest_rows_for_user(session, user_id: int) -> list[dict]:
    digests = (
        await session.execute(
            select(BasisDigestSubscription)
            .where(BasisDigestSubscription.user_id == user_id)
            .where(BasisDigestSubscription.is_active.is_(True))
            .order_by(BasisDigestSubscription.created_at.desc())
        )
    ).scalars().all()
    rows: list[dict] = []
    for d in digests:
        b = await session.get(Basis, int(d.basis_id))
        pr = await session.get(Product, int(d.product_id)) if getattr(d, "product_id", None) and not getattr(d, "all_products", False) else None
        if getattr(d, "all_products", False):
            scope = "все доступные на базисе"
        else:
            scope = pr.name if pr else "—"
        mode = str(getattr(d, "delivery_mode", "") or "prices_only")
        if mode == "with_delivery":
            dest_label = str(getattr(d, "destination_name", None) or "").strip()
            if not dest_label and getattr(d, "destination_id", None):
                cd = await session.get(CityDestination, int(d.destination_id))
                if cd and getattr(cd, "name", None):
                    dest_label = str(cd.name)
            if dest_label:
                scope = f"{scope} + доставка до {dest_label}"
            else:
                scope = f"{scope} + доставка"
        rows.append({"d": d, "basis_name": b.name if b else "—", "scope": scope})
    return rows


async def _table_digest_rows_for_user(session, user_id: int) -> list[dict]:
    subs = (
        await session.execute(
            select(TableDigestSubscription)
            .where(TableDigestSubscription.user_id == user_id)
            .where(TableDigestSubscription.is_active.is_(True))
            .order_by(TableDigestSubscription.created_at.desc())
        )
    ).scalars().all()
    rows: list[dict] = []
    for s in subs:
        try:
            pj = json.loads(str(getattr(s, "product_ids_json", "") or "[]"))
            bj = json.loads(str(getattr(s, "basis_ids_json", "") or "[]"))
            dj = json.loads(str(getattr(s, "destination_ids_json", "") or "[]"))
        except Exception:
            pj, bj, dj = [], [], []
        label = f"{len(set(int(x) for x in pj))} топл. × {len(set(int(x) for x in bj))} баз. × {len(set(int(x) for x in dj))} назнач."
        rows.append({"t": s, "label": label})
    return rows


async def _basis_product_pairs_for_anomaly(session) -> list[dict]:
    """Пары базис+продукт с активной ценой — для подписки на аномалию без ввода кода вручную."""
    q = await session.execute(
        select(
            ProductBasisPrice.basis_id,
            ProductBasisPrice.product_id,
            Basis.name,
            Product.name,
            ProductBasisPrice.instrument_code,
        )
        .join(Basis, Basis.id == ProductBasisPrice.basis_id)
        .join(Product, Product.id == ProductBasisPrice.product_id)
        .where(ProductBasisPrice.is_active.is_(True))
        .where(Basis.is_active.is_(True))
        .where(Product.is_active.is_(True))
        .where(ProductBasisPrice.current_price > 0)
        .order_by(Basis.name, Product.name)
    )
    seen: set[tuple[int, int]] = set()
    out: list[dict] = []
    for bid, pid, bn, pn, code in q.all():
        key = (int(bid), int(pid))
        if key in seen:
            continue
        ic = (code or "").strip()
        if not ic:
            continue
        seen.add(key)
        out.append(
            {
                "basis_id": int(bid),
                "product_id": int(pid),
                "label": f"{bn} — {pn}",
            }
        )
    return out


def _safe_next(next_url: str | None) -> str | None:
    if not next_url:
        return None
    s = str(next_url).strip()
    if not s:
        return None
    # только относительные пути или полный URL к этому же домену — MVP защита от open-redirect
    if s.startswith("/"):
        return s
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return None


@router.get("/cabinet", response_class=HTMLResponse)
async def cabinet(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet", status_code=302)

    reqs = (
        await session.execute(
            select(UserRequest).where(UserRequest.user_id == user.id).order_by(UserRequest.created_at.desc()).limit(50)
        )
    ).scalars().all()

    leads = (
        await session.execute(select(Lead).where(Lead.user_id == user.id).order_by(Lead.created_at.desc()).limit(50))
    ).scalars().all()

    return templates.TemplateResponse(
        "cabinet.html",
        {
            "request": request,
            "user": user,
            "requests": reqs,
            "leads": leads,
            "msg": request.query_params.get("ok"),
            "err": request.query_params.get("err"),
        },
    )


@router.get("/cabinet/subscriptions", response_class=HTMLResponse)
async def cabinet_subscriptions(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    products = await list_products_for_calc(session)
    basises = (
        await session.execute(
            select(Basis).where(Basis.is_active.is_(True)).order_by(Basis.transport_type.desc(), Basis.name)
        )
    ).scalars().all()

    alert_rows = await _alert_rows_for_user(session, user.id)
    digest_rows = await _digest_rows_for_user(session, user.id)
    table_digest_rows = await _table_digest_rows_for_user(session, user.id)
    destinations_pick = (
        await session.execute(select(CityDestination).order_by(CityDestination.name).limit(500))
    ).scalars().all()

    anomaly_alerts = (
        await session.execute(
            select(AnomalyAlert)
            .where(AnomalyAlert.user_id == user.id)
            .where(AnomalyAlert.is_active.is_(True))
            .order_by(AnomalyAlert.created_at.desc())
        )
    ).scalars().all()

    anomaly_pairs = await _basis_product_pairs_for_anomaly(session)
    basis_order: list[dict] = []
    seen_b: set[int] = set()
    for row in anomaly_pairs:
        bid = int(row["basis_id"])
        if bid in seen_b:
            continue
        seen_b.add(bid)
        basis_order.append({"id": bid, "name": str(row["label"]).split(" — ", 1)[0].strip()})

    return templates.TemplateResponse(
        "cabinet_subscriptions.html",
        {
            "request": request,
            "user": user,
            "products": products,
            "basises": basises,
            "alerts": alert_rows,
            "digest_rows": digest_rows,
            "table_digest_rows": table_digest_rows,
            "destinations_pick": destinations_pick,
            "anomaly_alerts": anomaly_alerts,
            "anomaly_pairs": anomaly_pairs,
            "anomaly_basises": basis_order,
        },
    )


@router.get("/cabinet/destinations", response_class=HTMLResponse)
async def cabinet_destinations(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    rows = (
        await session.execute(
            select(UserDestination, CityDestination)
            .join(CityDestination, CityDestination.id == UserDestination.city_destination_id)
            .where(UserDestination.user_id == int(user.id))
            .where(UserDestination.is_active.is_(True))
            .order_by(UserDestination.created_at.desc())
            .limit(200)
        )
    ).all()

    items: list[dict] = []
    for ud, cd in rows:
        items.append(
            {
                "ud": ud,
                "cd": cd,
                "label": (str(getattr(ud, "label", "") or "").strip() or str(getattr(cd, "name", "") or "—")),
            }
        )

    return templates.TemplateResponse(
        "cabinet_destinations.html",
        {
            "request": request,
            "user": user,
            "items": items,
        },
    )


@router.post("/cabinet/destinations/add", response_class=HTMLResponse)
async def cabinet_destinations_add(
    request: Request,
    session: DbSession,
    destination: str = Form(...),
    label: str | None = Form(None),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    dest_text = (destination or "").strip()
    if not dest_text:
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)

    dest_id, _lat, _lon, _key, _is_station = await resolve_destination_to_id(session, dest_text)
    if dest_id is None:
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)

    clean_label = (label or "").strip() or None

    # upsert by (user_id, city_destination_id)
    existing = (
        await session.execute(
            select(UserDestination)
            .where(UserDestination.user_id == int(user.id))
            .where(UserDestination.city_destination_id == int(dest_id))
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.is_active = True
        if clean_label is not None:
            existing.label = clean_label
        try:
            await session.commit()
        except Exception:
            await session.rollback()
            return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)
        return RedirectResponse("/cabinet/destinations?ok=dest_add", status_code=302)

    ud = UserDestination(
        user_id=int(user.id),
        city_destination_id=int(dest_id),
        label=clean_label,
        is_active=True,
    )
    session.add(ud)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        # retry as update (race/duplicate)
        existing2 = (
            await session.execute(
                select(UserDestination)
                .where(UserDestination.user_id == int(user.id))
                .where(UserDestination.city_destination_id == int(dest_id))
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing2 is not None:
            existing2.is_active = True
            if clean_label is not None:
                existing2.label = clean_label
            try:
                await session.commit()
            except Exception:
                await session.rollback()
                return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)
            return RedirectResponse("/cabinet/destinations?ok=dest_add", status_code=302)
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)
    except Exception:
        await session.rollback()
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)

    return RedirectResponse("/cabinet/destinations?ok=dest_add", status_code=302)


@router.post("/cabinet/destinations/disable", response_class=HTMLResponse)
async def cabinet_destinations_disable(
    request: Request,
    session: DbSession,
    ud_id: int = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/destinations", status_code=302)

    ud = await session.get(UserDestination, int(ud_id))
    if ud is None or int(getattr(ud, "user_id", 0) or 0) != int(user.id):
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)
    ud.is_active = False
    try:
        await session.commit()
    except Exception:
        await session.rollback()
        return RedirectResponse("/cabinet/destinations?err=dest", status_code=302)
    return RedirectResponse("/cabinet/destinations?ok=dest_off", status_code=302)


@router.post("/cabinet/subscribe", response_class=HTMLResponse)
async def cabinet_subscribe(
    request: Request,
    session: DbSession,
    product_id: int = Form(...),
    basis_id: int | None = Form(None),
    target_price: str = Form(...),
    next: str | None = Form(None),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    try:
        tp = float(target_price.replace(",", ".").strip())
        if tp <= 0:
            raise ValueError
    except ValueError:
        return RedirectResponse("/cabinet/subscriptions?err=price", status_code=302)

    if basis_id:
        ok_pair = (
            await session.execute(
                select(ProductBasisPrice.id)
                .where(ProductBasisPrice.basis_id == int(basis_id))
                .where(ProductBasisPrice.product_id == int(product_id))
                .where(ProductBasisPrice.is_active.is_(True))
                .limit(1)
            )
        ).scalar_one_or_none()
        if ok_pair is None:
            return RedirectResponse("/cabinet/subscriptions?err=digest_pair", status_code=302)

    session.add(
        PriceAlert(
            user_id=user.id,
            product_id=product_id,
            basis_id=(int(basis_id) if basis_id else None),
            target_price=tp,
            volume=None,
            city_destination_id=None,
            email=user.email,
            is_active=True,
        )
    )
    await session.commit()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=sub"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=sub", status_code=302)


@router.post("/cabinet/digest_subscribe", response_class=HTMLResponse)
async def cabinet_digest_subscribe(
    request: Request,
    session: DbSession,
    basis_id: int = Form(...),
    product_scope: str = Form(...),
    delivery_mode: str = Form("prices_only"),
    destination: str | None = Form(None),
    next: str | None = Form(None),
):
    """Ежедневная сводка по базису (14:15 МСК), один продукт или все на базисе."""
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    bid = int(basis_id)
    basis = await session.get(Basis, bid)
    if not basis or not getattr(basis, "is_active", True):
        return RedirectResponse("/cabinet/subscriptions?err=digest", status_code=302)

    ps = (product_scope or "").strip()
    all_p = ps == "all"
    pid: int | None = None
    if not all_p:
        try:
            pid = int(ps)
        except ValueError:
            return RedirectResponse("/cabinet/subscriptions?err=digest", status_code=302)
        ok = (
            await session.execute(
                select(ProductBasisPrice.id)
                .where(ProductBasisPrice.basis_id == bid)
                .where(ProductBasisPrice.product_id == int(pid))
                .where(ProductBasisPrice.is_active.is_(True))
                .limit(1)
            )
        ).scalar_one_or_none()
        if ok is None:
            return RedirectResponse("/cabinet/subscriptions?err=digest_pair", status_code=302)

    mode = "with_delivery" if str(delivery_mode or "").strip() == "with_delivery" else "prices_only"
    dest_id = None
    dest_text = None
    dest_key = ""
    if mode == "with_delivery":
        dest_text = (destination or "").strip()
        if not dest_text:
            return RedirectResponse("/cabinet/subscriptions?err=digest", status_code=302)
        from web.services.calc_service import resolve_destination_to_id

        did, _lat, _lon, dkey, _is_station = await resolve_destination_to_id(session, dest_text)
        if not did:
            return RedirectResponse("/cabinet/subscriptions?err=digest", status_code=302)
        dest_id = int(did)
        dest_key = str(dkey or "")

    sub_obj = BasisDigestSubscription(
        user_id=user.id,
        basis_id=bid,
        delivery_mode=mode,
        destination_id=dest_id,
        destination_name=dest_text,
        destination_key=dest_key,
        all_products=all_p,
        product_id=None if all_p else int(pid),
        is_active=True,
    )
    session.add(sub_obj)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=digest"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=digest", status_code=302)


@router.post("/cabinet/digest_disable", response_class=HTMLResponse)
async def cabinet_digest_disable(
    request: Request,
    session: DbSession,
    digest_id: int = Form(...),
    next: str | None = Form(None),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    d = await session.get(BasisDigestSubscription, int(digest_id))
    if not d or int(d.user_id) != int(user.id):
        return RedirectResponse("/cabinet/subscriptions?err=digest_off", status_code=302)
    d.is_active = False
    await session.commit()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=digest_off"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=digest_off", status_code=302)


@router.post("/cabinet/table_digest_subscribe", response_class=HTMLResponse)
async def cabinet_table_digest_subscribe(request: Request, session: DbSession):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    form = await request.form()

    def _parse(name: str) -> list[int]:
        out: list[int] = []
        for x in form.getlist(name):
            try:
                out.append(int(x))
            except (TypeError, ValueError):
                continue
        return out

    product_ids = _parse("product_ids")
    basis_ids = _parse("basis_ids")
    destination_ids = _parse("destination_ids")
    err = validate_selection_payload(product_ids, basis_ids, destination_ids)
    if err:
        return RedirectResponse("/cabinet/subscriptions?err=table_digest", status_code=302)

    notify_email = str(form.get("notify_email") or "").lower() in ("1", "on", "true", "yes")
    notify_max = str(form.get("notify_max") or "").lower() in ("1", "on", "true", "yes")
    if not notify_email and not notify_max:
        return RedirectResponse("/cabinet/subscriptions?err=table_channels", status_code=302)

    sub = TableDigestSubscription(
        user_id=int(user.id),
        product_ids_json=json.dumps(sorted({int(x) for x in product_ids})),
        basis_ids_json=json.dumps(sorted({int(x) for x in basis_ids})),
        destination_ids_json=json.dumps(sorted({int(x) for x in destination_ids})),
        send_hour_msk=14,
        send_minute_msk=15,
        notify_email=bool(notify_email),
        notify_max=bool(notify_max),
        is_active=True,
    )
    session.add(sub)
    await session.commit()
    return RedirectResponse("/cabinet/subscriptions?ok=table_digest", status_code=302)


@router.post("/cabinet/table_digest_disable", response_class=HTMLResponse)
async def cabinet_table_digest_disable(
    request: Request,
    session: DbSession,
    table_digest_id: int = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    t = await session.get(TableDigestSubscription, int(table_digest_id))
    if not t or int(t.user_id) != int(user.id):
        return RedirectResponse("/cabinet/subscriptions?err=table_off", status_code=302)
    t.is_active = False
    await session.commit()
    return RedirectResponse("/cabinet/subscriptions?ok=table_off", status_code=302)


@router.post("/cabinet/subscribe_anomaly", response_class=HTMLResponse)
async def cabinet_subscribe_anomaly(
    request: Request,
    session: DbSession,
    basis_id: int | None = Form(None),
    product_id: int | None = Form(None),
    instrument_code: str | None = Form(None),
    threshold_pct: str = Form("3"),
    direction: str = Form("any"),
    next: str | None = Form(None),
):
    """
    Подписка на аномалию (day-to-day) по instrument_code.
    Либо пара базис+продукт (код из ProductBasisPrice), либо явный код инструмента.
    """
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    code = ""
    if basis_id is not None and product_id is not None:
        row = (
            await session.execute(
                select(ProductBasisPrice.instrument_code)
                .where(ProductBasisPrice.basis_id == int(basis_id))
                .where(ProductBasisPrice.product_id == int(product_id))
                .where(ProductBasisPrice.is_active.is_(True))
                .limit(1)
            )
        ).scalar_one_or_none()
        code = (row or "").strip().upper()
    if not code:
        code = (instrument_code or "").strip().upper()
    if not code:
        return RedirectResponse("/cabinet/subscriptions?err=anom", status_code=302)

    try:
        thr = float((threshold_pct or "").replace(",", ".").strip())
        if thr <= 0:
            raise ValueError
    except Exception:
        return RedirectResponse("/cabinet/subscriptions?err=anom_thr", status_code=302)

    dir_s = str(direction or "any").strip().lower()
    if dir_s not in ("any", "up", "down"):
        dir_s = "any"

    session.add(
        AnomalyAlert(
            user_id=user.id,
            instrument_code=code,
            threshold_pct=thr,
            direction=dir_s,
            is_active=True,
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        # уже есть подписка на этот код — просто считаем успехом
        await session.rollback()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=anom"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=anom", status_code=302)


@router.post("/cabinet/price_disable", response_class=HTMLResponse)
async def cabinet_price_disable(
    request: Request,
    session: DbSession,
    price_alert_id: int = Form(...),
    next: str | None = Form(None),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    a = await session.get(PriceAlert, int(price_alert_id))
    if not a or int(a.user_id) != int(user.id):
        return RedirectResponse("/cabinet/subscriptions?err=price_off", status_code=302)
    a.is_active = False
    await session.commit()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=price_off"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=price_off", status_code=302)


@router.post("/cabinet/anomaly_disable", response_class=HTMLResponse)
async def cabinet_anomaly_disable(
    request: Request,
    session: DbSession,
    anomaly_id: int = Form(...),
    next: str | None = Form(None),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    a = await session.get(AnomalyAlert, int(anomaly_id))
    if not a or int(a.user_id) != int(user.id):
        return RedirectResponse("/cabinet/subscriptions?err=anom_off", status_code=302)
    a.is_active = False
    await session.commit()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=anom_off"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=anom_off", status_code=302)


@router.post("/cabinet/subscribe_from_request_price", response_class=HTMLResponse)
async def cabinet_subscribe_from_request_price(
    request: Request,
    session: DbSession,
    request_id: int = Form(...),
    target_price: str = Form(...),
    next: str | None = Form(None),
):
    """Подписка на снижение цены по результату расчёта (product + basis)."""
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    ur = await session.get(UserRequest, int(request_id))
    if not ur or ur.user_id != user.id:
        return RedirectResponse("/cabinet/subscriptions?err=sub", status_code=302)

    try:
        tp = float(target_price.replace(",", ".").strip())
        if tp <= 0:
            raise ValueError
    except Exception:
        return RedirectResponse("/cabinet/subscriptions?err=price", status_code=302)

    session.add(
        PriceAlert(
            user_id=user.id,
            product_id=int(ur.product_id),
            basis_id=int(getattr(ur, "basis_id", None)) if getattr(ur, "basis_id", None) else None,
            target_price=tp,
            volume=None,
            city_destination_id=None,
            email=user.email,
            is_active=True,
        )
    )
    await session.commit()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=sub"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=sub", status_code=302)


@router.post("/cabinet/subscribe_from_request_anomaly", response_class=HTMLResponse)
async def cabinet_subscribe_from_request_anomaly(
    request: Request,
    session: DbSession,
    request_id: int = Form(...),
    threshold_pct: str = Form("3"),
    next: str | None = Form(None),
):
    """Подписка на аномалию по instrument_code из результата расчёта."""
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login?next=/cabinet/subscriptions", status_code=302)

    ur = await session.get(UserRequest, int(request_id))
    if not ur or ur.user_id != user.id:
        return RedirectResponse("/cabinet/subscriptions?err=anom", status_code=302)

    pbp = await session.get(ProductBasisPrice, int(getattr(ur, "price_id", 0) or 0))
    code = (getattr(pbp, "instrument_code", None) or "").strip().upper()
    if not code:
        return RedirectResponse("/cabinet/subscriptions?err=anom", status_code=302)

    try:
        thr = float((threshold_pct or "").replace(",", ".").strip())
        if thr <= 0:
            raise ValueError
    except Exception:
        return RedirectResponse("/cabinet/subscriptions?err=anom_thr", status_code=302)

    session.add(
        AnomalyAlert(
            user_id=user.id,
            instrument_code=code,
            threshold_pct=thr,
            is_active=True,
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
    back = _safe_next(next) or "/cabinet/subscriptions?ok=anom"
    sep = "&" if ("?" in back) else "?"
    return RedirectResponse(f"{back}{sep}ok=anom", status_code=302)


@router.post("/cabinet/lead", response_class=HTMLResponse)
async def cabinet_lead(
    request: Request,
    session: DbSession,
    request_id: int = Form(...),
):
    try:
        user = await require_email_user(request, session)
    except HTTPException:
        return RedirectResponse("/login", status_code=302)

    ur = await session.get(UserRequest, request_id)
    if not ur or ur.user_id != user.id:
        return RedirectResponse("/cabinet?err=lead", status_code=302)

    lead = (
        await session.execute(
            select(Lead).where(Lead.user_id == user.id, Lead.request_id == request_id).limit(1)
        )
    ).scalar_one_or_none()
    if lead and lead.status in ("sent", "contacted", "won"):
        return RedirectResponse("/cabinet?ok=lead", status_code=302)

    if not lead:
        lead = Lead(user_id=user.id, request_id=request_id, status="email_pending", source="web")
        session.add(lead)
        await session.flush()

    try:
        await send_order_to_email(user.email or "", ur, session)
        lead.email = user.email
        lead.status = "sent"
        await session.commit()

        product = await session.get(Product, ur.product_id)
        basis = await session.get(Basis, ur.basis_id)
        dest = await session.get(CityDestination, ur.city_destination_id)
        from utils import canonical_fuel_display_name

        lead_text = (
            f"📝 <b>Новая заявка #{ur.id}</b>\n\n"
            f"📧 Email: <b>{user.email}</b>\n"
            f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
            f"📍 {basis.name if basis else '—'} → {dest.name if dest else '—'}\n"
            f"📦 {ur.volume:g} т, {('Ж/Д' if ur.transport_type == 'rail' else 'Авто')}\n"
            f"💎 Итого: <b>{float(ur.total_price):,.0f}</b> ₽".replace(",", " ")
        )
        bot = getattr(request.app.state, "bot", None)
        if bot is not None:
            await notify_managers_about_lead(bot, lead_text)
    except Exception:
        logger.exception("cabinet lead")
        await session.rollback()
        return RedirectResponse("/cabinet?err=lead_send", status_code=302)

    return RedirectResponse("/cabinet?ok=lead", status_code=302)


@router.get("/basises", response_class=HTMLResponse)
async def basises_public(request: Request, session: DbSession):

    from db.database import ProductBasisPrice

    basises = (
        await session.execute(
            select(Basis).where(Basis.is_active.is_(True)).order_by(Basis.transport_type.asc(), Basis.name)
        )
    ).scalars().all()

    # rail first
    basises.sort(key=lambda b: (0 if (b.transport_type or "") == "rail" else 1, b.name))

    items = []
    for b in basises:
        q = await session.execute(
            select(Product.name)
            .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
            .where(ProductBasisPrice.basis_id == b.id)
            .where(ProductBasisPrice.is_active.is_(True))
            .where(Product.is_active.is_(True))
            .order_by(Product.name)
        )
        names = [canonical_fuel_display_name(n) for (n,) in q.all()]
        uniq = []
        seen = set()
        for n in names:
            if n not in seen:
                uniq.append(n)
                seen.add(n)
        products_txt = ", ".join(uniq[:10]) + (" …" if len(uniq) > 10 else "")
        items.append(
            {
                "name": b.name,
                "transport": ("Ж/Д" if (b.transport_type == "rail") else "Авто"),
                "products": products_txt or "—",
            }
        )

    return templates.TemplateResponse("cabinet_basises.html", {"request": request, "items": items})
