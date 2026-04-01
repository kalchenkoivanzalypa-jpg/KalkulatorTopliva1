"""Аналитика для веб: тренд и сравнение базисов (логика как в bot/analytics_handlers)."""
from __future__ import annotations

import difflib
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import select

from bot.handlers import calculate_distance
from db.database import Basis, Product, ProductBasisPrice, SpimexPrice
from rail_tariff import compute_rail_tariff_distance_km
from utils import canonical_fuel_display_name, get_coordinates_from_city, get_delivery_rate, normalize_city_name_key
from utils.rail_logistics import find_rail_station_for_destination, is_sakhalin_geo_point, is_sakhalin_destination, sakhalin_ferry_surcharge_per_ton

COMPARE_PRODUCTS_ORDER: list[str] = [
    "АИ-100-К5",
    "АИ-92-К5",
    "АИ-95-К5",
    "ДТ-З-К5",
    "ДТ-А-К5",
    "ДТ-Е-К5",
    "ДТ-Л-К5",
    "Мазут топочный М100",
    "ТС-1",
]
COMPARE_PRODUCTS_SET = set(COMPARE_PRODUCTS_ORDER)

RU_MONTHS = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def _fmt_ru_date(dt: datetime) -> str:
    return f"{dt.day} {RU_MONTHS.get(dt.month, str(dt.month))} {dt.year}"


def _price_change_arrow(curr: float | None, prev: float | None) -> str:
    if curr is None or prev is None:
        return ""
    if curr > prev:
        return "↑ "
    if curr < prev:
        return "↓ "
    return "→ "


def _ma(xs: list[float], k: int) -> float | None:
    if k <= 0 or len(xs) < k:
        return None
    return sum(xs[:k]) / k


def _trend_slope(xs: list[float]) -> float | None:
    n = len(xs)
    if n < 2:
        return None
    x_mean = (n - 1) / 2
    y_mean = sum(xs) / n
    num = sum((i - x_mean) * (xs[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return None
    return num / den


def _build_min_max_forecast(prices: list[float]) -> tuple[str, str, str]:
    if not prices:
        return "—", "—", "—"
    pmin = min(prices)
    pmax = max(prices)
    ma3 = _ma(prices, 3)
    slope = _trend_slope(prices)
    tomorrow = "—"
    if ma3 is not None and slope is not None:
        tomorrow = f"{ma3 + slope:,.0f} ₽/т".replace(",", " ")
    return (
        f"{pmin:,.0f} ₽/т".replace(",", " "),
        f"{pmax:,.0f} ₽/т".replace(",", " "),
        tomorrow,
    )


def _transport_rank(t: str | None) -> int:
    tt = (t or "").strip().lower()
    if tt == "rail":
        return 0
    if tt == "auto":
        return 2
    return 1


def _normalize_basis_search_text(value: str) -> str:
    s = (value or "").strip().lower().replace("ё", "е")
    for ch in ("—", "–", "−", "‑", "-", ",", ".", ";", ":", "(", ")"):
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


def pick_compare_products(products: list[Product]) -> list[Product]:
    by_name = {p.name: p for p in products if p.name}
    ordered: list[Product] = []
    for name in COMPARE_PRODUCTS_ORDER:
        if name in by_name:
            ordered.append(by_name[name])
    return ordered


async def search_basises(
    session,
    query: str,
    *,
    offset: int = 0,
    page_size: int = 12,
) -> tuple[list[Basis], int]:
    q_norm = _normalize_basis_search_text(query)
    rows = await session.execute(select(Basis).where(Basis.is_active.is_(True)))
    all_basises = rows.scalars().all()

    if not q_norm:
        basises = list(all_basises)
    else:
        basises = []
        for bs in all_basises:
            name_norm = _normalize_basis_search_text(getattr(bs, "name", ""))
            if q_norm in name_norm:
                basises.append(bs)
        if not basises:
            name_to_basis = {
                _normalize_basis_search_text(getattr(bs, "name", "")): bs
                for bs in all_basises
                if getattr(bs, "name", None)
            }
            matches = difflib.get_close_matches(q_norm, list(name_to_basis.keys()), n=12, cutoff=0.74)
            basises = [name_to_basis[m] for m in matches]

    basises.sort(key=lambda b: (_transport_rank(getattr(b, "transport_type", None)), b.name))
    total = len(basises)
    page = basises[offset : offset + page_size]
    return page, total


@dataclass
class TrendResult:
    basis_name: str
    product_name: str
    instrument_code: str
    lines: list[str]
    forecast_parts: list[str]
    pmin: str
    pmax: str
    pforecast: str
    details_lines: list[str]


async def compute_trend(session, basis_id: int, product_id: int) -> Optional[TrendResult]:
    pbp = (
        await session.execute(
            select(ProductBasisPrice).where(
                ProductBasisPrice.product_id == product_id,
                ProductBasisPrice.basis_id == basis_id,
                ProductBasisPrice.is_active.is_(True),
            ).limit(1)
        )
    ).scalar_one_or_none()
    basis = await session.get(Basis, basis_id)
    product = await session.get(Product, product_id)
    if not pbp or not basis or not product or not pbp.instrument_code:
        return None

    qh = await session.execute(
        select(SpimexPrice)
        .where(SpimexPrice.exchange_product_id == pbp.instrument_code)
        .order_by(SpimexPrice.date.desc())
        .limit(10)
    )
    rows = qh.scalars().all()
    if not rows:
        return None

    prices = [float(r.price or 0) for r in rows]
    ma3 = _ma(prices, 3)
    ma5 = _ma(prices, 5)
    slope = _trend_slope(prices)
    slope_txt = "—" if slope is None else f"{slope:+.0f} ₽/день"

    lines: list[str] = []
    for i, r in enumerate(rows):
        d_obj = r.date.date() if isinstance(r.date, datetime) else None
        d = _fmt_ru_date(datetime.combine(d_obj, datetime.min.time())) if d_obj else str(r.date)
        curr = float(r.price) if r.price is not None else None
        prev = float(rows[i + 1].price) if i + 1 < len(rows) and rows[i + 1].price is not None else None
        arrow = _price_change_arrow(curr, prev)
        p = f"{float(r.price):,.0f}".replace(",", " ") if r.price is not None else "—"
        v = f"{float(r.volume):,.0f}".replace(",", " ") if r.volume is not None else "—"
        lines.append(f"{d}: {arrow}{p} ₽/т, объем {v} т")

    forecast: list[str] = []
    if ma3 is not None:
        forecast.append(f"средняя за 3 дня: {ma3:,.0f} ₽/т".replace(",", " "))
    if ma5 is not None:
        forecast.append(f"средняя за 5 дней: {ma5:,.0f} ₽/т".replace(",", " "))
    forecast.append(f"тренд изменения: {slope_txt}")
    if ma3 is not None and slope is not None:
        tomorrow = ma3 + slope
        forecast.append(f"прогноз на завтра: {tomorrow:,.0f} ₽/т".replace(",", " "))

    pmin, pmax, pforecast = _build_min_max_forecast(prices)
    details_lines = list(lines)

    return TrendResult(
        basis_name=basis.name,
        product_name=canonical_fuel_display_name(product.name),
        instrument_code=str(pbp.instrument_code),
        lines=lines[:5],
        forecast_parts=forecast,
        pmin=pmin,
        pmax=pmax,
        pforecast=pforecast,
        details_lines=details_lines,
    )


@dataclass
class CompareRow:
    basis_name: str
    transport: str
    html_block: str


@dataclass
class CompareResult:
    title_product: str
    destination: str
    rows: list[CompareRow]
    best_line: Optional[str]
    details_html: str


async def compute_compare_three(
    session,
    *,
    product_id: int,
    basis_ids: list[int],
    destination_text: str,
) -> Optional[CompareResult]:
    if len(basis_ids) != 3:
        return None

    t = destination_text.strip()
    dest_key = normalize_city_name_key(t)
    coords = await get_coordinates_from_city(t, session)
    dest_station = None
    if not coords:
        dest_station = await find_rail_station_for_destination(session, t, dest_key)
        if dest_station is None:
            return None
        dest_lat, dest_lon = float(dest_station.latitude), float(dest_station.longitude)
    else:
        dest_lat, dest_lon = coords
        dest_station = await find_rail_station_for_destination(session, t, dest_key)

    sakhalin_dest = is_sakhalin_destination(t, dest_key, dest_station)

    product = await session.get(Product, int(product_id))
    if not product:
        return None
    canonical_name = canonical_fuel_display_name(product.name)
    alias_ids_q = await session.execute(
        select(Product.id, Product.name).where(Product.is_active.is_(True)).where(Product.name.isnot(None))
    )
    alias_ids: list[int] = []
    for p_id, p_name in alias_ids_q.all():
        if canonical_fuel_display_name(str(p_name)) == canonical_name:
            alias_ids.append(int(p_id))
    if not alias_ids:
        alias_ids = [int(product_id)]

    compare_rows: list[CompareRow] = []
    details_blocks: list[str] = []
    best: tuple[float, str] | None = None

    for bid in basis_ids:
        b = await session.get(Basis, int(bid))
        if not b:
            compare_rows.append(
                CompareRow(basis_name="?", transport="—", html_block="<p>Базис не найден</p>")
            )
            continue
        if b.transport_type == "auto" and sakhalin_dest:
            compare_rows.append(
                CompareRow(
                    basis_name=b.name,
                    transport="—",
                    html_block="<p>⛔ Для Сахалина только Ж/Д доставка</p>",
                )
            )
            continue

        pbp = (
            await session.execute(
                select(ProductBasisPrice)
                .where(ProductBasisPrice.product_id.in_(alias_ids))
                .where(ProductBasisPrice.basis_id == int(bid))
                .where(ProductBasisPrice.is_active.is_(True))
                .order_by(ProductBasisPrice.id.asc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if not pbp:
            compare_rows.append(
                CompareRow(
                    basis_name=b.name,
                    transport="Ж/Д" if b.transport_type == "rail" else "Авто",
                    html_block="<p>❌ Нет цены/instrument_code</p>",
                )
            )
            continue

        if b.transport_type == "rail":
            o_lat = float(b.rail_latitude or b.latitude)
            o_lon = float(b.rail_longitude or b.longitude)
            if dest_station is not None and (
                not sakhalin_dest
                or is_sakhalin_geo_point(float(dest_station.latitude), float(dest_station.longitude))
            ):
                d_lat = float(dest_station.latitude)
                d_lon = float(dest_station.longitude)
            else:
                d_lat = dest_lat
                d_lon = dest_lon
            dist = compute_rail_tariff_distance_km(
                o_lat,
                o_lon,
                d_lat,
                d_lon,
                origin_esr=(str(b.rail_esr).strip() if getattr(b, "rail_esr", None) else None),
                dest_esr=(
                    str(dest_station.esr_code).strip()
                    if dest_station and getattr(dest_station, "esr_code", None)
                    else None
                ),
            )
        else:
            dist = calculate_distance(dest_lat, dest_lon, float(b.latitude), float(b.longitude))

        rate = await get_delivery_rate(dist, b.transport_type, session)
        delivery = dist * rate
        if (b.transport_type or "").lower() == "rail":
            delivery += sakhalin_ferry_surcharge_per_ton(sakhalin_dest)
        total = float(pbp.current_price) + delivery

        qh = await session.execute(
            select(SpimexPrice)
            .where(SpimexPrice.exchange_product_id == pbp.instrument_code)
            .order_by(SpimexPrice.date.desc())
            .limit(5)
        )
        hist = qh.scalars().all()
        hist_lines: list[str] = []
        hist_prices = [float(x.price or 0) for x in hist]
        for i, x in enumerate(hist):
            d = _fmt_ru_date(datetime.combine(x.date.date(), datetime.min.time()))
            curr = float(x.price) if x.price is not None else None
            prev = float(hist[i + 1].price) if i + 1 < len(hist) and hist[i + 1].price is not None else None
            arrow = _price_change_arrow(curr, prev)
            hist_lines.append(f"{d}: {arrow}{float(x.price):,.0f}".replace(",", " "))
        p_min = min(hist_prices) if hist_prices else float(pbp.current_price)
        p_max = max(hist_prices) if hist_prices else float(pbp.current_price)
        p_forecast = (sum(hist_prices[:3]) / min(3, len(hist_prices))) if hist_prices else float(pbp.current_price)

        tt = "Ж/Д" if b.transport_type == "rail" else "Авто"
        block = (
            f"<p><b>{b.name}</b> ({tt})<br/>"
            f"Сегодня: {float(pbp.current_price):,.0f} ₽/т + доставка {delivery:,.0f} ₽/т = "
            f"<b>{total:,.0f} ₽/т</b><br/>"
            f"Дистанция: {dist:,.0f} км<br/>"
            f"Мин./макс. 5 дней: {p_min:,.0f} / {p_max:,.0f} ₽/т<br/>"
            f"Прогноз: {p_forecast:,.0f} ₽/т</p>"
        )
        details_blocks.append(f"<b>{b.name}</b> ({tt})<br/>5 дней: {'; '.join(hist_lines) if hist_lines else '—'}")

        compare_rows.append(CompareRow(basis_name=b.name, transport=tt, html_block=block))

        if best is None or total < best[0]:
            best = (total, b.name)

    best_line = f"🥇 Рекомендуем: {best[1]} (мин. цена с доставкой)" if best else None
    details_html = "<br/><br/>".join(details_blocks) if details_blocks else ""

    return CompareResult(
        title_product=canonical_name,
        destination=t,
        rows=compare_rows,
        best_line=best_line,
        details_html=details_html,
    )
