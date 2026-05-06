# -*- coding: utf-8 -*-
"""
Пользовательская ежедневная таблица: котировка СПбМТСБ + доставка до выбранных направлений.
Лимиты: до 5 топлив, 12 базисов, 8 направлений, не более 36 пар продукт×базис, 12×8=96 ячеек доставки.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import Basis, CityDestination, Product, SpimexPrice
import rail_tariff
from rail_tariff import compute_rail_tariff_distance_km_cached
from utils import canonical_fuel_display_name, get_delivery_rate, normalize_city_name_key
from utils.market_price_freshness import pick_best_product_basis_price_row
from utils.rail_logistics import (
    find_rail_station_for_destination,
    fixed_delivery_per_ton_override,
    is_sakhalin_destination,
    is_sakhalin_geo_point,
    sakhalin_ferry_surcharge_per_ton,
)
from bot.handlers import calculate_distance

logger = logging.getLogger(__name__)

MAX_PRODUCTS = 5
MAX_BASIS = 12
MAX_DESTINATIONS = 8
MAX_PAIRS = 36  # len(products) * len(basises)
MAX_DELIVERY_CELLS = 96


def public_site_url() -> str:
    return (os.getenv("WEB_PUBLIC_URL") or "https://calc.nk-vsnp.ru").rstrip("/")


def validate_selection_payload(
    product_ids: list[int],
    basis_ids: list[int],
    destination_ids: list[int],
) -> Optional[str]:
    u_p = sorted({int(x) for x in product_ids if x})
    u_b = sorted({int(x) for x in basis_ids if x})
    u_d = sorted({int(x) for x in destination_ids if x})
    if not u_p or not u_b or not u_d:
        return "Выберите хотя бы одно топливо, базис и назначение."
    if len(u_p) > MAX_PRODUCTS:
        return f"Не более {MAX_PRODUCTS} видов топлива."
    if len(u_b) > MAX_BASIS:
        return f"Не более {MAX_BASIS} базисов."
    if len(u_d) > MAX_DESTINATIONS:
        return f"Не более {MAX_DESTINATIONS} направлений."
    if len(u_p) * len(u_b) > MAX_PAIRS:
        return f"Слишком много сочетаний топливо×базис (максимум {MAX_PAIRS}). Уменьшите списки."
    if len(u_b) * len(u_d) > MAX_DELIVERY_CELLS:
        return f"Слишком много расчётов доставки (базисов × направлений ≤ {MAX_DELIVERY_CELLS})."
    return None


def _vol_rail_tons(canonical: str) -> float:
    if (
        canonical.startswith("ДТ-")
        or canonical == "Мазут топочный М100"
        or canonical == "ТС-1"
    ):
        return 65.0
    return 60.0


async def _market_change_volume(
    session: AsyncSession, instrument_code: str
) -> tuple[str, Optional[float], Optional[float]]:
    """Изменение за последнюю торговую сессию относительно предыдущей, рыночная, объём последней."""
    code_u = str(instrument_code or "").strip().upper()
    if not code_u:
        return ("—", None, None)

    q = await session.execute(
        select(SpimexPrice)
        .where(func.upper(SpimexPrice.exchange_product_id) == code_u)
        .where(or_(SpimexPrice.price_market.isnot(None), SpimexPrice.price.isnot(None)))
        .order_by(SpimexPrice.date.desc())
        .limit(60)
    )
    rows = list(q.scalars().all())
    by_day: dict[date, SpimexPrice] = {}
    for r in rows:
        d_raw = r.date
        if isinstance(d_raw, datetime):
            d_key = d_raw.date()
        elif isinstance(d_raw, date):
            d_key = d_raw
        else:
            continue
        if d_key not in by_day:
            by_day[d_key] = r
    days_sorted = sorted(by_day.keys(), reverse=True)
    if not days_sorted:
        return ("—", None, None)
    last = by_day[days_sorted[0]]
    def _px(x: SpimexPrice) -> float:
        v = x.price_market if x.price_market is not None else x.price
        return float(v) if v is not None else 0.0

    mkt = _px(last)
    vol = float(last.volume) if last.volume is not None else None
    if len(days_sorted) < 2:
        ch = "—"
    else:
        prev = by_day[days_sorted[1]]
        prev_p = _px(prev)
        ch = f"{mkt - prev_p:+.0f}"
    return (ch, mkt if mkt else None, vol)


@dataclass
class _DestMeta:
    title: str
    lat: float
    lon: float
    key: str
    station: Any
    sakhalin: bool


async def _build_dest_meta(session: AsyncSession, cd: CityDestination) -> _DestMeta:
    t = str(cd.name or "").strip()
    dest_key = normalize_city_name_key(t)
    st = await find_rail_station_for_destination(session, t, dest_key)
    sak = is_sakhalin_destination(t, dest_key, st)
    return _DestMeta(
        title=t,
        lat=float(cd.latitude),
        lon=float(cd.longitude),
        key=dest_key,
        station=st,
        sakhalin=bool(sak),
    )


async def _one_total_with_delivery(
    session: AsyncSession,
    *,
    basis: Basis,
    product: Product,
    alias_ids: list[int],
    canonical_name: str,
    d: _DestMeta,
) -> Optional[float]:
    b = basis
    transport_type = (getattr(b, "transport_type", None) or "").strip().lower()
    if transport_type == "auto" and d.sakhalin:
        return None

    pbp = await pick_best_product_basis_price_row(
        session,
        basis_id=int(b.id),
        product_ids=alias_ids,
        exclude_ai100_price_stub=(canonical_name == "АИ-100-К5"),
    )
    if not pbp or not getattr(pbp, "current_price", None):
        return None

    vol = _vol_rail_tons(canonical_name)

    try:
        if transport_type == "rail":
            o_lat = float(getattr(b, "rail_latitude", None) or getattr(b, "latitude", 0.0) or 0.0)
            o_lon = float(getattr(b, "rail_longitude", None) or getattr(b, "longitude", 0.0) or 0.0)
            st = d.station
            if st is not None and (
                not d.sakhalin
                or is_sakhalin_geo_point(float(st.latitude), float(st.longitude))
            ):
                d_lat = float(st.latitude)
                d_lon = float(st.longitude)
            else:
                d_lat = d.lat
                d_lon = d.lon
            dist_km = await asyncio.to_thread(
                compute_rail_tariff_distance_km_cached,
                o_lat,
                o_lon,
                d_lat,
                d_lon,
                (str(getattr(b, "rail_esr", "")).strip() if getattr(b, "rail_esr", None) else None),
                (
                    str(getattr(st, "esr_code", "")).strip()
                    if st is not None and getattr(st, "esr_code", None)
                    else None
                ),
            )
        else:
            dist_km = calculate_distance(d.lat, d.lon, float(b.latitude), float(b.longitude))
    except Exception:
        return None

    try:
        if transport_type == "rail":
            fixed_pt = fixed_delivery_per_ton_override(
                str(getattr(b, "name", "") or ""),
                str(d.key or ""),
            )
            if fixed_pt is not None:
                delivery_per_ton = float(fixed_pt)
            else:
                rr = rail_tariff.calculate_delivery_cost(
                    float(dist_km),
                    float(vol),
                    canonical_name,
                )
                delivery_per_ton = float(rr["total_cost"]) / float(vol)
            delivery_per_ton += float(sakhalin_ferry_surcharge_per_ton(bool(d.sakhalin)) or 0.0)
        else:
            rate = await get_delivery_rate(float(dist_km), "auto", session)
            delivery_per_ton = float(dist_km) * float(rate)
    except Exception:
        return None

    return float(pbp.current_price) + float(delivery_per_ton)


async def build_table_digest_html(session: AsyncSession, sub: Any) -> Optional[str]:
    """HTML-таблица для email и MAX (fmt=html). sub — TableDigestSubscription."""
    try:
        product_ids: list[int] = json.loads(str(getattr(sub, "product_ids_json", "") or "[]"))
        basis_ids: list[int] = json.loads(str(getattr(sub, "basis_ids_json", "") or "[]"))
        dest_ids: list[int] = json.loads(str(getattr(sub, "destination_ids_json", "") or "[]"))
    except Exception:
        return None

    err = validate_selection_payload(product_ids, basis_ids, dest_ids)
    if err:
        return None

    # alias map per product canonical (for pick_best)
    alias_q = await session.execute(
        select(Product.id, Product.name).where(Product.is_active.is_(True)).where(Product.name.isnot(None))
    )
    id_to_canon: dict[int, str] = {}
    for pid, pname in alias_q.all():
        id_to_canon[int(pid)] = canonical_fuel_display_name(str(pname))

    dest_objs: list[CityDestination] = []
    for did in sorted({int(x) for x in dest_ids}):
        cd = await session.get(CityDestination, int(did))
        if cd:
            dest_objs.append(cd)

    dest_metas: list[_DestMeta] = []
    for cd in dest_objs:
        dest_metas.append(await _build_dest_meta(session, cd))

    sem = asyncio.Semaphore(6)

    async def cell_total(pid: int, bid: int, dm: _DestMeta) -> Optional[float]:
        async with sem:
            pr = await session.get(Product, int(pid))
            b = await session.get(Basis, int(bid))
            if not pr or not b:
                return None
            canon = id_to_canon.get(int(pid)) or canonical_fuel_display_name(pr.name)
            alias_ids = [int(i) for i, c in id_to_canon.items() if c == canon]
            if not alias_ids:
                alias_ids = [int(pid)]
            return await _one_total_with_delivery(
                session,
                basis=b,
                product=pr,
                alias_ids=alias_ids,
                canonical_name=canon,
                d=dm,
            )

    # Порядок: продукты как в JSON, базисы как в JSON
    u_p = []
    seen = set()
    for x in product_ids:
        i = int(x)
        if i not in seen:
            seen.add(i)
            u_p.append(i)
    u_b = []
    seen_b = set()
    for x in basis_ids:
        i = int(x)
        if i not in seen_b:
            seen_b.add(i)
            u_b.append(i)

    trade_day = datetime.now().strftime("%d.%m.%Y")

    # Заголовки колонок направлений — короткие имена
    dest_headers = [dm.title[:24] for dm in dest_metas]

    parts: list[str] = [
        f"<p><b>Дата торгов:</b> {trade_day}</p>",
        '<table style="border-collapse:collapse;width:100%;max-width:1200px;font-size:13px;">',
        "<thead><tr>"
        '<th style="border:1px solid #555;padding:6px;text-align:left;">Наименование</th>'
        '<th style="border:1px solid #555;padding:6px;text-align:left;">Базис поставки</th>'
        '<th style="border:1px solid #555;padding:6px;text-align:right;">Изменение</th>'
        '<th style="border:1px solid #555;padding:6px;text-align:right;">Рыночная</th>'
        '<th style="border:1px solid #555;padding:6px;text-align:right;">Объём</th>',
    ]
    for h in dest_headers:
        parts.append(
            f'<th style="border:1px solid #555;padding:6px;text-align:right;">{h}</th>'
        )
    parts.append("</tr></thead><tbody>")

    for pid in u_p:
        pr = await session.get(Product, int(pid))
        if not pr:
            continue
        canon = id_to_canon.get(int(pid)) or canonical_fuel_display_name(pr.name)
        alias_ids = [int(i) for i, c in id_to_canon.items() if c == canon]
        if not alias_ids:
            alias_ids = [int(pid)]

        colspan = 5 + len(dest_metas)
        parts.append(
            f'<tr><td colspan="{colspan}" style="border:1px solid #555;padding:6px;background:#2a2a2a;"><b>{canon}</b></td></tr>'
        )

        for bid in u_b:
            b = await session.get(Basis, int(bid))
            if not b:
                continue
            pbp = await pick_best_product_basis_price_row(
                session,
                basis_id=int(b.id),
                product_ids=alias_ids,
                exclude_ai100_price_stub=(canon == "АИ-100-К5"),
            )
            if not pbp or not pbp.instrument_code:
                span = 3 + len(dest_metas)
                parts.append(
                    f"<tr><td style=\"border:1px solid #555;padding:4px;\">{canon}</td>"
                    f"<td style=\"border:1px solid #555;padding:4px;\">{b.name}</td>"
                    f'<td colspan="{span}" style="border:1px solid #555;padding:4px;">—</td></tr>'
                )
                continue

            ch, mkt, vol = await _market_change_volume(session, str(pbp.instrument_code))
            ch_s = ch if ch is not None else "—"
            mkt_s = f"{mkt:,.0f}".replace(",", " ") if mkt is not None else "—"
            vol_s = f"{vol:,.0f}".replace(",", " ") if vol is not None else "—"

            cells_futs = [cell_total(pid, bid, dm) for dm in dest_metas]
            totals = await asyncio.gather(*cells_futs)
            parts.append("<tr>")
            parts.append(f'<td style="border:1px solid #555;padding:4px;">{canon}</td>')
            parts.append(f'<td style="border:1px solid #555;padding:4px;">{b.name}</td>')
            parts.append(f'<td style="border:1px solid #555;padding:4px;text-align:right;">{ch_s}</td>')
            parts.append(f'<td style="border:1px solid #555;padding:4px;text-align:right;">{mkt_s}</td>')
            parts.append(f'<td style="border:1px solid #555;padding:4px;text-align:right;">{vol_s}</td>')
            for t in totals:
                if t is None:
                    td = "—"
                else:
                    td = f"{t:,.0f}".replace(",", " ")
                parts.append(
                    f'<td style="border:1px solid #555;padding:4px;text-align:right;">{td}</td>'
                )
            parts.append("</tr>")

    parts.append("</tbody></table>")
    parts.append(
        "<p style=\"font-size:12px;color:#888;\">Ориентировочно: рыночная цена СПбМТСБ и оценка доставки. "
        "Точное КП — у ООО «НК-Востокнефтепродукт». Настроить подписку: "
        f'<a href="{public_site_url()}/cabinet/subscriptions#table-digest">{public_site_url()}/cabinet/subscriptions</a></p>'
    )
    return "".join(parts)


__all__ = [
    "MAX_BASIS",
    "MAX_DELIVERY_CELLS",
    "MAX_DESTINATIONS",
    "MAX_PAIRS",
    "MAX_PRODUCTS",
    "build_table_digest_html",
    "public_site_url",
    "validate_selection_payload",
]
