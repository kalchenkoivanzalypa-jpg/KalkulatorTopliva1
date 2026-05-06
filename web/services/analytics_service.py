"""Аналитика для веб: тренд и сравнение базисов (логика как в bot/analytics_handlers)."""
from __future__ import annotations

import difflib
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import func, select

from bot.handlers import calculate_distance
from db.database import Basis, Product, ProductBasisPrice, SpimexPrice
import rail_tariff
from rail_tariff import compute_rail_tariff_distance_km_cached
from utils import canonical_fuel_display_name, get_coordinates_from_city, get_delivery_rate, normalize_city_name_key
from utils.market_price_freshness import pick_best_product_basis_price_row
from utils.rail_logistics import (
    find_rail_station_for_destination,
    fixed_delivery_per_ton_override,
    is_sakhalin_geo_point,
    is_sakhalin_destination,
    sakhalin_ferry_surcharge_per_ton,
)
from analytics.metrics import compute_metrics_30d, load_series_30d

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
    ma5 = _ma(prices, 5)
    slope = _trend_slope(prices)  # на том же окне, что и prices
    tomorrow = "—"
    # Прогноз: средняя за 5 дней + тренд (руб/день)
    if ma5 is not None and slope is not None:
        tomorrow = f"{ma5 + slope:,.0f} ₽/т".replace(",", " ")
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
    """
    Список продуктов для UI (rating/compare).
    В БД названия могут быть «длинные» (например «Топливо для реактивных двигателей ТС-1»),
    поэтому выбираем представителей по каноническому имени.
    """
    # canonical -> (product, rank_key)
    best: dict[str, Product] = {}
    for p in products:
        if not p or not getattr(p, "name", None):
            continue
        canon = canonical_fuel_display_name(str(p.name))
        if canon not in COMPARE_PRODUCTS_SET:
            continue
        curr = best.get(canon)
        if curr is None:
            best[canon] = p
            continue
        # детерминированно: меньший id побеждает
        try:
            if int(getattr(p, "id", 10**18) or 10**18) < int(getattr(curr, "id", 10**18) or 10**18):
                best[canon] = p
        except Exception:
            pass

    ordered: list[Product] = []
    for canon in COMPARE_PRODUCTS_ORDER:
        p = best.get(canon)
        if p is not None:
            ordered.append(p)
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
    details_lines_5: list[str]
    details_lines_10: list[str]
    details_lines_30: list[str]
    # 30d block
    metrics30_text: list[str]
    basis_quality_text: str


async def compute_trend(session, basis_id: int, product_id: int) -> Optional[TrendResult]:
    product = await session.get(Product, product_id)
    if not product:
        return None
    exclude_ai100_stub = canonical_fuel_display_name(getattr(product, "name", "") or "") == "АИ-100-К5"
    pbp = await pick_best_product_basis_price_row(
        session,
        basis_id=int(basis_id),
        product_ids=[int(product_id)],
        exclude_ai100_price_stub=exclude_ai100_stub,
    )
    basis = await session.get(Basis, basis_id)
    if not pbp or not basis or not pbp.instrument_code:
        return None

    series30 = await load_series_30d(session, str(pbp.instrument_code))
    m30 = compute_metrics_30d(series30)
    metrics30_text: list[str] = []
    basis_quality_text = "—"
    if m30 is not None:
        # «качество базиса» по регулярности торгов/ликвидности (MVP)
        if m30.active_ratio >= 0.80 and m30.liquidity_score >= 55:
            basis_quality_text = "🟢 Базис выглядит сильным: торги регулярные, ликвидность высокая."
        elif m30.active_ratio >= 0.55 and m30.liquidity_score >= 35:
            basis_quality_text = "🟡 Базис средний: торги есть, но не каждый день/объёмы умеренные."
        else:
            basis_quality_text = "🔴 Базис слабый по биржевой истории: торги редкие/ликвидность низкая."

        metrics30_text = [
            f"30д: min/max {m30.min30:,.0f} / {m30.max30:,.0f} ₽/т".replace(",", " "),
            (
                f"Позиция цены (30д): {m30.range_pos30:.2f} (дешевизна {m30.cheapness:.2f})"
                if m30.range_pos30 is not None and m30.cheapness is not None
                else "Позиция цены (30д): —"
            ),
            (
                f"Аномалия (30д): {m30.z30:+.2f}, волатильность (σ): {m30.volatility30:,.0f} ₽/т".replace(",", " ")
                if m30.z30 is not None
                else f"Аномалия (30д): —, волатильность (σ): {m30.volatility30:,.0f} ₽/т".replace(",", " ")
            ),
            (
                (
                    "Изменение цены: 5/10/30д = "
                    + (f"{m30.trend5:+.0f}" if m30.trend5 is not None else "—")
                    + " / "
                    + (f"{m30.trend10:+.0f}" if m30.trend10 is not None else "—")
                    + " / "
                    + (f"{m30.trend30:+.0f}" if m30.trend30 is not None else "—")
                    + " ₽/т"
                ).replace(",", " ")
            ),
            (
                f"Ликвидность (30д): средний объём {float(m30.avg_volume30 or 0):,.0f} т/день, активность {m30.active_ratio:.0%}, оценка {m30.liquidity_score:,.0f}/100".replace(
                    ",", " "
                )
            ),
        ]

    # Загружаем 30 торговых дней для min/max/прогноза и деталей
    code_u = str(pbp.instrument_code or "").strip().upper()
    qh = await session.execute(
        select(SpimexPrice)
        .where(func.upper(SpimexPrice.exchange_product_id) == code_u)
        .order_by(SpimexPrice.date.desc())
        .limit(30)
    )
    rows30 = qh.scalars().all()
    if not rows30:
        return None

    prices30 = [float(r.price or 0) for r in rows30]
    ma5 = _ma(prices30, 5)
    ma10 = _ma(prices30, 10)
    slope = _trend_slope(prices30)
    slope_txt = "—" if slope is None else f"{slope:+.0f} ₽/день"

    lines: list[str] = []
    for i, r in enumerate(rows30[:10]):
        d_obj = r.date.date() if isinstance(r.date, datetime) else None
        d = _fmt_ru_date(datetime.combine(d_obj, datetime.min.time())) if d_obj else str(r.date)
        curr = float(r.price) if r.price is not None else None
        prev = (
            float(rows30[i + 1].price)
            if i + 1 < len(rows30) and rows30[i + 1].price is not None
            else None
        )
        arrow = _price_change_arrow(curr, prev)
        p = f"{float(r.price):,.0f}".replace(",", " ") if r.price is not None else "—"
        v = f"{float(r.volume):,.0f}".replace(",", " ") if r.volume is not None else "—"
        lines.append(f"{d}: {arrow}{p} ₽/т, объем {v} т")

    forecast: list[str] = []
    if ma5 is not None:
        forecast.append(f"средняя за 5 дней: {ma5:,.0f} ₽/т".replace(",", " "))
    if ma10 is not None:
        forecast.append(f"средняя за 10 дней: {ma10:,.0f} ₽/т".replace(",", " "))
    forecast.append(f"тренд изменения: {slope_txt}")
    if ma5 is not None and slope is not None:
        tomorrow = ma5 + slope
        forecast.append(f"прогноз на завтра: {tomorrow:,.0f} ₽/т".replace(",", " "))

    pmin, pmax, pforecast = _build_min_max_forecast(prices30)

    def _format_lines(rows: list[SpimexPrice]) -> list[str]:
        out: list[str] = []
        for i, r in enumerate(rows):
            d_obj = r.date.date() if isinstance(r.date, datetime) else None
            d = _fmt_ru_date(datetime.combine(d_obj, datetime.min.time())) if d_obj else str(r.date)
            curr = float(r.price) if r.price is not None else None
            prev = (
                float(rows[i + 1].price)
                if i + 1 < len(rows) and rows[i + 1].price is not None
                else None
            )
            arrow = _price_change_arrow(curr, prev)
            p = f"{float(r.price):,.0f}".replace(",", " ") if r.price is not None else "—"
            v = f"{float(r.volume):,.0f}".replace(",", " ") if r.volume is not None else "—"
            out.append(f"{d}: {arrow}{p} ₽/т, объем {v} т")
        return out

    details_5 = _format_lines(rows30[:5])
    details_10 = _format_lines(rows30[:10])
    details_30 = _format_lines(rows30)

    return TrendResult(
        basis_name=basis.name,
        product_name=canonical_fuel_display_name(product.name),
        instrument_code=str(pbp.instrument_code),
        lines=lines[:5],
        forecast_parts=forecast,
        pmin=pmin,
        pmax=pmax,
        pforecast=pforecast,
        details_lines_5=details_5,
        details_lines_10=details_10,
        details_lines_30=details_30,
        metrics30_text=metrics30_text,
        basis_quality_text=basis_quality_text,
    )


@dataclass
class CompareRow:
    basis_name: str
    transport: str
    html_block: str
    instrument_code: str | None = None


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

        pbp = await pick_best_product_basis_price_row(
            session,
            basis_id=int(bid),
            product_ids=alias_ids,
            exclude_ai100_price_stub=((canonical_name or "") == "АИ-100-К5"),
        )
        if not pbp:
            cnt_q = (
                select(func.count())
                .select_from(ProductBasisPrice)
                .where(ProductBasisPrice.product_id.in_(alias_ids))
                .where(ProductBasisPrice.basis_id == int(bid))
                .where(ProductBasisPrice.is_active.is_(True))
                .where(ProductBasisPrice.current_price > 0)
            )
            if (canonical_name or "") == "АИ-100-К5":
                cnt_q = cnt_q.where(ProductBasisPrice.current_price != 10000)
            n_any = int((await session.execute(cnt_q)).scalar_one() or 0)
            if n_any > 0:
                compare_rows.append(
                    CompareRow(
                        basis_name=b.name,
                        transport="Ж/Д" if b.transport_type == "rail" else "Авто",
                        html_block="<p>⏳ Ни один из кодов инструмента не прошёл свежесть (нет актуальной рыночной строки по бирже).</p>",
                    )
                )
            else:
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
            try:
                dist = await asyncio.to_thread(
                    compute_rail_tariff_distance_km_cached,
                    o_lat,
                    o_lon,
                    d_lat,
                    d_lon,
                    (str(b.rail_esr).strip() if getattr(b, "rail_esr", None) else None),
                    (
                        str(dest_station.esr_code).strip()
                        if dest_station and getattr(dest_station, "esr_code", None)
                        else None
                    ),
                )
            except Exception:
                compare_rows.append(
                    CompareRow(
                        basis_name=b.name,
                        transport="Ж/Д",
                        html_block="<p>⚠️ Не удалось посчитать ж/д дистанцию ТР-4 (нет данных по ЕСР)</p>",
                        instrument_code=str(getattr(pbp, "instrument_code", "") or "").strip().upper() or None,
                    )
                )
                continue
        else:
            dist = calculate_distance(dest_lat, dest_lon, float(b.latitude), float(b.longitude))

        rate = await get_delivery_rate(dist, b.transport_type, session)
        delivery = dist * rate
        if (b.transport_type or "").lower() == "rail":
            delivery += sakhalin_ferry_surcharge_per_ton(sakhalin_dest)
        total = float(pbp.current_price) + delivery

        code_u = str(pbp.instrument_code or "").strip().upper()
        qh = await session.execute(
            select(SpimexPrice)
            .where(func.upper(SpimexPrice.exchange_product_id) == code_u)
            .order_by(SpimexPrice.date.desc())
            .limit(30)
        )
        hist = qh.scalars().all()
        hist_lines: list[str] = []
        hist_prices = [float(x.price or 0) for x in hist]
        for i, x in enumerate(hist[:10]):
            d = _fmt_ru_date(datetime.combine(x.date.date(), datetime.min.time()))
            curr = float(x.price) if x.price is not None else None
            prev = float(hist[i + 1].price) if i + 1 < len(hist) and hist[i + 1].price is not None else None
            arrow = _price_change_arrow(curr, prev)
            hist_lines.append(f"{d}: {arrow}{float(x.price):,.0f}".replace(",", " "))
        p_min = min(hist_prices) if hist_prices else float(pbp.current_price)
        p_max = max(hist_prices) if hist_prices else float(pbp.current_price)
        # прогноз на завтра по окну 30д: ma5 + slope30 (см. _build_min_max_forecast)
        _, _, pf_txt = _build_min_max_forecast(hist_prices)
        try:
            p_forecast = float(str(pf_txt).replace(" ₽/т", "").replace(" ", "").replace(",", "."))
        except Exception:
            p_forecast = float(pbp.current_price)

        tt = "Ж/Д" if b.transport_type == "rail" else "Авто"

        series30 = await load_series_30d(session, str(pbp.instrument_code))
        m30 = compute_metrics_30d(series30) if series30 else None
        m30_line = ""
        if m30 is not None:
            rp = f"{m30.range_pos30:.2f}" if m30.range_pos30 is not None else "—"
            z = f"{m30.z30:+.2f}" if m30.z30 is not None else "—"
            t5 = f"{m30.trend5:+.0f}".replace(",", " ") if m30.trend5 is not None else "—"
            t10 = f"{m30.trend10:+.0f}".replace(",", " ") if m30.trend10 is not None else "—"
            t30 = f"{m30.trend30:+.0f}".replace(",", " ") if m30.trend30 is not None else "—"
            ar = f"{m30.active_ratio:.0%}"
            liq = f"{m30.liquidity_score:,.0f}".replace(",", " ")
            m30_line = (
                "<br/>"
                + "30д: "
                + f"позиция {rp}, Z {z}; "
                + f"Δ5/10/30 {t5}/{t10}/{t30} ₽/т; "
                + f"ликвидность {liq}/100 (активность {ar})"
            )
        block = (
            f"<p><b>{b.name}</b> ({tt})<br/>"
            f"Сегодня: {float(pbp.current_price):,.0f} ₽/т + доставка {delivery:,.0f} ₽/т = "
            f"<b>{total:,.0f} ₽/т</b><br/>"
            f"Дистанция: {dist:,.0f} км<br/>"
            f"Мин./макс. 30д: {p_min:,.0f} / {p_max:,.0f} ₽/т<br/>"
            f"Прогноз (30д): {p_forecast:,.0f} ₽/т"
            f"{m30_line}</p>"
        )
        details_blocks.append(f"<b>{b.name}</b> ({tt})<br/>10 дней: {'; '.join(hist_lines) if hist_lines else '—'}")

        compare_rows.append(
            CompareRow(
                basis_name=b.name,
                transport=tt,
                html_block=block,
                instrument_code=(str(getattr(pbp, "instrument_code", "") or "").strip().upper() or None),
            )
        )

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


@dataclass
class MatrixCell:
    price_per_ton: float | None
    delivery_per_ton: float | None
    total_per_ton: float | None
    distance_km: float | None
    error: str | None = None


@dataclass
class MatrixDestination:
    title: str
    dest_id: int | None
    lat: float | None
    lon: float | None
    key: str | None
    station: object | None
    source: str | None = None


@dataclass
class MatrixRow:
    basis_id: int
    basis_name: str
    transport_label: str
    cells: list[MatrixCell]


@dataclass
class MatrixResult:
    title_product: str
    volume_tons: float
    destinations: list[MatrixDestination]
    rows: list[MatrixRow]


async def compute_matrix(
    session,
    *,
    product_id: int,
    basis_ids: list[int],
    destinations: list[dict],
    volume_tons: float = 60.0,
    max_concurrency: int = 6,
) -> MatrixResult | None:
    """
    Матрица базисы×направления (до 5×5).

    destinations: список словарей с полями (как в web/routes_analytics.py):
      - title, dest_id, lat, lon, key, station, source
    """
    if not product_id or not basis_ids or not destinations:
        return None

    basis_ids = [int(x) for x in basis_ids][:5]
    destinations = list(destinations)[:5]

    product = await session.get(Product, int(product_id))
    if not product:
        return None
    canonical_name = canonical_fuel_display_name(getattr(product, "name", "") or "")

    # product aliases with same canonical name
    alias_ids_q = await session.execute(
        select(Product.id, Product.name).where(Product.is_active.is_(True)).where(Product.name.isnot(None))
    )
    alias_ids: list[int] = []
    for p_id, p_name in alias_ids_q.all():
        if canonical_fuel_display_name(str(p_name)) == canonical_name:
            alias_ids.append(int(p_id))
    if not alias_ids:
        alias_ids = [int(product_id)]

    basises: list[Basis] = []
    for bid in basis_ids:
        b = await session.get(Basis, int(bid))
        if b is None or not getattr(b, "is_active", True):
            continue
        basises.append(b)
    if not basises:
        return None

    # Normalize destinations (title/key/station)
    dest_list: list[MatrixDestination] = []
    dest_meta: list[dict] = []
    for d in destinations:
        title = str(d.get("title") or d.get("text") or "").strip()
        if not title:
            continue
        dest_key = str(d.get("key") or normalize_city_name_key(title))
        st = d.get("station")
        if st is None:
            st = await find_rail_station_for_destination(session, title, dest_key)
        sak = is_sakhalin_destination(title, dest_key, st)
        dest_id = d.get("dest_id")
        try:
            dest_id_i = int(dest_id) if dest_id is not None else None
        except Exception:
            dest_id_i = None
        dest_list.append(
            MatrixDestination(
                title=title,
                dest_id=dest_id_i,
                lat=(float(d.get("lat")) if d.get("lat") is not None else None),
                lon=(float(d.get("lon")) if d.get("lon") is not None else None),
                key=dest_key,
                station=st,
                source=(str(d.get("source") or "").strip() or None),
            )
        )
        dest_meta.append(
            {
                "title": title,
                "lat": float(d.get("lat") or 0.0),
                "lon": float(d.get("lon") or 0.0),
                "key": dest_key,
                "station": st,
                "sakhalin": bool(sak),
            }
        )
    if not dest_list:
        return None

    sem = asyncio.Semaphore(max(1, int(max_concurrency)))
    cell_map: dict[tuple[int, int], MatrixCell] = {}

    async def _compute_cell(basis_idx: int, dest_idx: int) -> None:
        b = basises[basis_idx]
        d = dest_meta[dest_idx]
        txt = str(d["title"])
        sak = bool(d["sakhalin"])
        transport_type = (getattr(b, "transport_type", None) or "").strip().lower()
        transport_label = "Ж/Д" if transport_type == "rail" else "Авто"

        async with sem:
            if transport_type == "auto" and sak:
                cell_map[(basis_idx, dest_idx)] = MatrixCell(
                    price_per_ton=None,
                    delivery_per_ton=None,
                    total_per_ton=None,
                    distance_km=None,
                    error="Для Сахалина авто-доставка не рассчитывается (только Ж/Д).",
                )
                return

            pbp = await pick_best_product_basis_price_row(
                session,
                basis_id=int(b.id),
                product_ids=alias_ids,
                exclude_ai100_price_stub=((canonical_name or "") == "АИ-100-К5"),
            )

            if not pbp or not getattr(pbp, "current_price", None):
                cell_map[(basis_idx, dest_idx)] = MatrixCell(
                    price_per_ton=None,
                    delivery_per_ton=None,
                    total_per_ton=None,
                    distance_km=None,
                    error="Нет актуальной цены по бирже для пары продукт×базис (все коды устарели или нет строк).",
                )
                return

            dest_lat = float(d["lat"])
            dest_lon = float(d["lon"])

            try:
                if transport_type == "rail":
                    o_lat = float(getattr(b, "rail_latitude", None) or getattr(b, "latitude", 0.0) or 0.0)
                    o_lon = float(getattr(b, "rail_longitude", None) or getattr(b, "longitude", 0.0) or 0.0)
                    st = d.get("station")
                    if st is not None and (not sak or is_sakhalin_geo_point(float(st.latitude), float(st.longitude))):
                        d_lat = float(st.latitude)
                        d_lon = float(st.longitude)
                    else:
                        d_lat = dest_lat
                        d_lon = dest_lon
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
                    dist_km = calculate_distance(dest_lat, dest_lon, float(b.latitude), float(b.longitude))
            except Exception:
                cell_map[(basis_idx, dest_idx)] = MatrixCell(
                    price_per_ton=float(pbp.current_price),
                    delivery_per_ton=None,
                    total_per_ton=None,
                    distance_km=None,
                    error="Не удалось посчитать дистанцию (нет данных).",
                )
                return

            try:
                if transport_type == "rail":
                    fixed_pt = fixed_delivery_per_ton_override(
                        str(getattr(b, "name", "") or ""),
                        str(d.get("key") or ""),
                    )
                    if fixed_pt is not None:
                        delivery_per_ton = float(fixed_pt)
                    else:
                        rr = rail_tariff.calculate_delivery_cost(
                            float(dist_km),
                            float(volume_tons),
                            canonical_name,
                        )
                        delivery_per_ton = float(rr["total_cost"]) / float(volume_tons)
                    delivery_per_ton += float(sakhalin_ferry_surcharge_per_ton(bool(sak)) or 0.0)
                else:
                    rate = await get_delivery_rate(float(dist_km), "auto", session)
                    delivery_per_ton = float(dist_km) * float(rate)
            except Exception:
                cell_map[(basis_idx, dest_idx)] = MatrixCell(
                    price_per_ton=float(pbp.current_price),
                    delivery_per_ton=None,
                    total_per_ton=None,
                    distance_km=float(dist_km),
                    error="Не удалось посчитать доставку.",
                )
                return

            price_per_ton = float(pbp.current_price)
            total_per_ton = float(price_per_ton) + float(delivery_per_ton)
            cell_map[(basis_idx, dest_idx)] = MatrixCell(
                price_per_ton=price_per_ton,
                delivery_per_ton=float(delivery_per_ton),
                total_per_ton=float(total_per_ton),
                distance_km=float(dist_km),
                error=None,
            )

    tasks: list[asyncio.Task] = []
    for bi in range(len(basises)):
        for di in range(len(dest_meta)):
            tasks.append(asyncio.create_task(_compute_cell(bi, di)))
    if tasks:
        await asyncio.gather(*tasks)

    rows: list[MatrixRow] = []
    for bi, b in enumerate(basises):
        transport_type = (getattr(b, "transport_type", None) or "").strip().lower()
        transport_label = "Ж/Д" if transport_type == "rail" else "Авто"
        row_cells: list[MatrixCell] = []
        for di in range(len(dest_meta)):
            row_cells.append(cell_map.get((bi, di)) or MatrixCell(None, None, None, None, error="—"))
        rows.append(
            MatrixRow(
                basis_id=int(b.id),
                basis_name=str(getattr(b, "name", "") or f"#{int(b.id)}"),
                transport_label=transport_label,
                cells=row_cells,
            )
        )

    return MatrixResult(
        title_product=canonical_name,
        volume_tons=float(volume_tons),
        destinations=dest_list,
        rows=rows,
    )
