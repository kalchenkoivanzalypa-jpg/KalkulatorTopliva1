#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
import difflib

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
import logging
import traceback
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from bot.keyboards import get_after_calculation_keyboard, get_cancel_keyboard, get_main_keyboard
from bot.handlers import SubscriptionStates, get_or_create_user
from db.database import (
    Basis,
    CityDestination,
    Product,
    ProductBasisPrice,
    SpimexPrice,
    UserRequest,
    get_session,
)
from rail_tariff import compute_rail_tariff_distance_km
from utils import (
    canonical_fuel_display_name,
    get_coordinates_from_city,
    get_delivery_rate,
    normalize_city_name_key,
)
from utils.rail_logistics import (
    find_rail_station_for_destination,
    is_sakhalin_geo_point,
    is_sakhalin_destination,
    sakhalin_ferry_surcharge_per_ton,
)
from bot.handlers import calculate_distance

analytics_router = Router()
logger = logging.getLogger(__name__)

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
LAST_COMPARE_CONTEXT: dict[int, dict[str, object]] = {}
LAST_ANALYTICS_DETAILS: dict[int, dict[str, str]] = {}
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


def _pick_compare_products(products: list[Product]) -> list[Product]:
    """
    Для режима «Сравнить 3 базиса» показываем только базовую продуктовую линейку
    и убираем дубли (в БД часто есть несколько Product с одинаковым каноническим именем).
    """
    best_by_name: dict[str, Product] = {}
    for p in products:
        n = canonical_fuel_display_name(p.name)
        if n not in COMPARE_PRODUCTS_SET:
            continue
        prev = best_by_name.get(n)
        if prev is None or int(p.id) < int(prev.id):
            best_by_name[n] = p
    return [best_by_name[n] for n in COMPARE_PRODUCTS_ORDER if n in best_by_name]


def _fmt_ru_date(dt: datetime) -> str:
    return f"{dt.day} {RU_MONTHS.get(dt.month, '')}".strip()


def _price_change_arrow(curr: float | None, prev: float | None) -> str:
    if curr is None or prev is None:
        return ""
    if curr > prev:
        return "🟢⬆️ "
    if curr < prev:
        return "🔴⬇️ "
    return ""


def _build_min_max_forecast(prices: list[float]) -> tuple[str, str, str]:
    if not prices:
        return "—", "—", "—"
    p_min = min(prices)
    p_max = max(prices)
    ma3 = _ma(prices, 3)
    slope = _trend_slope(prices)
    if ma3 is None:
        forecast = prices[0]
    elif slope is None:
        forecast = ma3
    else:
        forecast = ma3 + slope
    return (
        f"{p_min:,.0f} ₽/т".replace(",", " "),
        f"{p_max:,.0f} ₽/т".replace(",", " "),
        f"{forecast:,.0f} ₽/т".replace(",", " "),
    )


class AnalyticsStates(StatesGroup):
    waiting_for_trend_basis_search = State()
    waiting_for_compare_basis_search = State()
    waiting_for_compare_destination = State()
    waiting_for_order_destination = State()
    waiting_for_order_volume = State()
    waiting_for_delivery_destination = State()
    waiting_for_delivery_volume = State()


def _analytics_menu_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.add(InlineKeyboardButton(text="📈 Тренд на базисе", callback_data="a_trend"))
    b.add(InlineKeyboardButton(text="📊 Сравнить 3 базиса", callback_data="a_compare"))
    b.add(InlineKeyboardButton(text="⬅️ Назад", callback_data="a_back"))
    b.adjust(1)
    return b.as_markup()


def _products_inline_kb(products: list[Product], prefix: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for p in products:
        b.add(
            InlineKeyboardButton(
                text=canonical_fuel_display_name(p.name),
                callback_data=f"{prefix}{p.id}",
            )
        )
    b.adjust(1)
    return b.as_markup()


def _transport_rank(t: str | None) -> int:
    """
    Сортировка базисов в UI:
      0) rail
      1) both/unknown (если появится «и так и так», ставим между)
      2) auto
    """
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


async def _basis_search_kb(
    session,
    query: str,
    *,
    mode: str,
    offset: int,
    page_size: int = 12,
) -> InlineKeyboardMarkup:
    q = (query or "").strip()
    q_norm = _normalize_basis_search_text(q)
    rows = await session.execute(select(Basis).where(Basis.is_active.is_(True)))
    all_basises = rows.scalars().all()

    if not q_norm:
        basises = all_basises
    else:
        basises = []
        for bs in all_basises:
            name_norm = _normalize_basis_search_text(getattr(bs, "name", ""))
            if q_norm in name_norm:
                basises.append(bs)

        # Мягкий фоллбек на опечатки/дефисы
        if not basises:
            name_to_basis = {
                _normalize_basis_search_text(getattr(bs, "name", "")): bs
                for bs in all_basises
                if getattr(bs, "name", None)
            }
            matches = difflib.get_close_matches(
                q_norm,
                list(name_to_basis.keys()),
                n=12,
                cutoff=0.74,
            )
            basises = [name_to_basis[m] for m in matches]
    basises.sort(key=lambda b: (_transport_rank(getattr(b, "transport_type", None)), b.name))

    page = basises[offset : offset + page_size]
    b = InlineKeyboardBuilder()

    for bs in page:
        tt = (getattr(bs, "transport_type", None) or "").lower()
        emoji = "🚂" if tt == "rail" else ("🚛" if tt == "auto" else "🚚")
        b.add(
            InlineKeyboardButton(
                text=f"{emoji} {bs.name}",
                callback_data=f"a_pick_basis_{mode}_{bs.id}",
            )
        )

    nav: list[InlineKeyboardButton] = []
    if offset > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"a_basis_page_{mode}_{max(0, offset - page_size)}",
            )
        )
    if offset + page_size < len(basises):
        nav.append(
            InlineKeyboardButton(
                text="➡️ Далее",
                callback_data=f"a_basis_page_{mode}_{offset + page_size}",
            )
        )
    if nav:
        b.row(*nav)
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="a_back"))

    b.adjust(1)
    return b.as_markup()


async def _upsert_destination(
    *,
    session,
    destination_text: str,
    dest_lat: float,
    dest_lon: float,
) -> CityDestination:
    destination_key = normalize_city_name_key(destination_text)

    # Берём пул кандидатов по ilike и добиваем точным match через python-нормализацию,
    # чтобы не плодить дубликаты и не упираться в UNIQUE по name.
    cand = await session.execute(
        select(CityDestination)
        .where(
            func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                f"%{destination_key}%"
            )
        )
        .order_by(CityDestination.request_count.desc())
        .limit(50)
    )
    candidates = cand.scalars().all()
    for c in candidates:
        if normalize_city_name_key(c.name) == destination_key:
            return c
    if candidates:
        return candidates[0]

    dest_obj = CityDestination(name=destination_text, latitude=dest_lat, longitude=dest_lon)
    session.add(dest_obj)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        # последний шанс: вытащить по точному имени
        dest_obj = (
            await session.execute(
                select(CityDestination)
                .where(CityDestination.name == destination_text)
                .limit(1)
            )
        ).scalar_one_or_none()
        if not dest_obj:
            raise
    return dest_obj


async def _create_user_request_for_basis(
    *,
    session,
    message: Message,
    basis_id: int,
    product_id: int,
    destination_text: str,
    volume: float,
) -> int:
    # Важно: не трогаем ORM-объекты `basis`/`product` напрямую, чтобы не спровоцировать
    # ленивую подзагрузку атрибутов (в AsyncSession это может дать MissingGreenlet).
    basis_row = (
        await session.execute(
            select(
                Basis.id,
                Basis.transport_type,
                Basis.latitude,
                Basis.longitude,
                Basis.rail_latitude,
                Basis.rail_longitude,
                Basis.rail_esr,
            ).where(Basis.id == basis_id)
        )
    ).one_or_none()
    product_row = (
        await session.execute(select(Product.id).where(Product.id == product_id))
    ).one_or_none()
    pbp_row = (
        await session.execute(
            select(ProductBasisPrice.id, ProductBasisPrice.current_price)
            .where(
                ProductBasisPrice.product_id == product_id,
                ProductBasisPrice.basis_id == basis_id,
                ProductBasisPrice.is_active.is_(True),
            )
            .limit(1)
        )
    ).one_or_none()

    if not basis_row or not product_row or not pbp_row:
        raise RuntimeError("Нет данных для заявки (базис/товар/цена)")
    _basis_id, basis_transport_type, basis_lat, basis_lon, basis_r_lat, basis_r_lon, basis_esr = basis_row
    (_product_id,) = product_row
    pbp_id, pbp_current_price = pbp_row

    dest_key = normalize_city_name_key(destination_text)
    coords = await get_coordinates_from_city(destination_text, session)
    dest_station = None
    # Важно: сохраняем поля станции в локальные переменные ДО операций,
    # которые могут сделать rollback и "протухлить" ORM-объекты (MissingGreenlet).
    dest_station_lat: float | None = None
    dest_station_lon: float | None = None
    dest_station_esr: str | None = None
    if not coords:
        dest_station = await find_rail_station_for_destination(session, destination_text, dest_key)
        if dest_station is None:
            raise RuntimeError("Не удалось определить назначение (нет координат и станции)")
        dest_station_lat = float(dest_station.latitude)
        dest_station_lon = float(dest_station.longitude)
        dest_station_esr = (
            str(dest_station.esr_code).strip()
            if getattr(dest_station, "esr_code", None) is not None
            else None
        )
        dest_lat, dest_lon = dest_station_lat, dest_station_lon
    else:
        dest_lat, dest_lon = coords
        dest_station = await find_rail_station_for_destination(session, destination_text, dest_key)
        if dest_station is not None:
            dest_station_lat = float(dest_station.latitude)
            dest_station_lon = float(dest_station.longitude)
            dest_station_esr = (
                str(dest_station.esr_code).strip()
                if getattr(dest_station, "esr_code", None) is not None
                else None
            )
    sakhalin_dest = is_sakhalin_destination(destination_text, dest_key, dest_station)

    dest_obj = await _upsert_destination(
        session=session,
        destination_text=destination_text,
        dest_lat=dest_lat,
        dest_lon=dest_lon,
    )

    if (basis_transport_type or "").lower() == "rail":
        o_lat = float(basis_r_lat or basis_lat)
        o_lon = float(basis_r_lon or basis_lon)
        if (
            dest_station is not None
            and (
                not sakhalin_dest
                or (
                    dest_station_lat is not None
                    and dest_station_lon is not None
                    and is_sakhalin_geo_point(dest_station_lat, dest_station_lon)
                )
            )
        ):
            # Берём сохранённые значения, чтобы не трогать ORM-объект
            d_lat = float(dest_station_lat) if dest_station_lat is not None else float(dest_lat)
            d_lon = float(dest_station_lon) if dest_station_lon is not None else float(dest_lon)
        else:
            d_lat = dest_lat
            d_lon = dest_lon
        distance_km = compute_rail_tariff_distance_km(
            o_lat,
            o_lon,
            d_lat,
            d_lon,
            origin_esr=(str(basis_esr).strip() if basis_esr else None),
            dest_esr=dest_station_esr,
        )
        transport_type = "rail"
    else:
        if sakhalin_dest:
            raise RuntimeError("Для Сахалинской области доступна только Ж/Д доставка.")
        distance_km = calculate_distance(dest_lat, dest_lon, float(basis_lat), float(basis_lon))
        transport_type = "auto"

    rate = await get_delivery_rate(distance_km, transport_type, session)
    delivery_cost = distance_km * volume * rate
    if transport_type == "rail":
        delivery_cost += volume * sakhalin_ferry_surcharge_per_ton(sakhalin_dest)
    base_total = float(pbp_current_price) * volume
    total_price = base_total + delivery_cost

    user = await get_or_create_user(message, session)
    req = UserRequest(
        user_id=user.id,
        product_id=int(_product_id),
        basis_id=int(_basis_id),
        price_id=int(pbp_id),
        city_destination_id=dest_obj.id,
        volume=volume,
        base_price=float(pbp_current_price),
        distance_km=float(distance_km),
        transport_type=transport_type,
        delivery_cost=float(delivery_cost),
        total_price=float(total_price),
    )
    session.add(req)
    await session.commit()
    await session.refresh(req)
    return int(req.id)

def _ma(xs: list[float], k: int) -> float | None:
    if k <= 0 or len(xs) < k:
        return None
    return sum(xs[:k]) / k


def _trend_slope(xs: list[float]) -> float | None:
    # x = 0..n-1, y=xs (latest first)
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


@analytics_router.message(F.text == "📊 Аналитика")
async def analytics_entry(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📊 <b>Аналитика</b>\nВыберите сценарий:",
        reply_markup=_analytics_menu_kb(),
        parse_mode="HTML",
    )


@analytics_router.callback_query(F.data == "a_back")
async def analytics_back(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Главное меню.", reply_markup=get_main_keyboard())
    await cb.answer()


@analytics_router.callback_query(F.data.startswith("a_details_"))
async def analytics_show_details(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    k = cb.data.replace("a_details_", "", 1).strip()
    data = LAST_ANALYTICS_DETAILS.get(int(cb.from_user.id), {})
    txt = data.get(k)
    if not txt:
        await cb.answer("Детали не найдены, сформируйте отчет заново.", show_alert=True)
        return
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=get_main_keyboard())
    await cb.answer()


# =========================
# Trend on basis (10 days)
# =========================


@analytics_router.callback_query(F.data == "a_trend")
async def analytics_trend_start(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(
        "📈 <b>Тренд на базисе</b>\n\nВведите часть названия базиса для поиска:",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML",
    )
    await state.set_state(AnalyticsStates.waiting_for_trend_basis_search)
    await cb.answer()


@analytics_router.message(AnalyticsStates.waiting_for_trend_basis_search, F.text)
async def analytics_trend_basis_search(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return

    session = await get_session()
    try:
        await state.update_data(trend_basis_query=t, trend_basis_offset=0)
        kb = await _basis_search_kb(session, t, mode="trend", offset=0)
        await message.answer("Выберите базис:", reply_markup=kb)
    finally:
        await session.close()


@analytics_router.callback_query(F.data.startswith("a_basis_page_trend_"))
async def analytics_trend_basis_page(cb: CallbackQuery, state: FSMContext):
    offset = int(cb.data.split("_")[-1])
    data = await state.get_data()
    q = data.get("trend_basis_query", "")
    session = await get_session()
    try:
        kb = await _basis_search_kb(session, q, mode="trend", offset=offset)
    finally:
        await session.close()
    await state.update_data(trend_basis_offset=offset)
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer()


@analytics_router.callback_query(F.data.startswith("a_pick_basis_trend_"))
async def analytics_trend_basis_picked(cb: CallbackQuery, state: FSMContext):
    basis_id = int(cb.data.split("_")[-1])
    await state.update_data(trend_basis_id=basis_id)
    session = await get_session()
    try:
        basis = await session.get(Basis, basis_id)
        qp = await session.execute(
            select(Product)
            .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
            .where(ProductBasisPrice.basis_id == basis_id)
            .where(ProductBasisPrice.is_active.is_(True))
            .where(Product.is_active.is_(True))
            .group_by(Product.id)
            .order_by(Product.name)
        )
        products = qp.scalars().all()
    finally:
        await session.close()

    if not basis or not products:
        await cb.message.answer("❌ На этом базисе нет товаров/данных.", reply_markup=get_main_keyboard())
        await state.clear()
        await cb.answer()
        return

    await cb.message.answer(
        f"Базис: <b>{basis.name}</b>\nВыберите топливо:",
        reply_markup=_products_inline_kb(products, prefix="a_trend_prod_"),
        parse_mode="HTML",
    )
    await cb.answer()

@analytics_router.callback_query(F.data.startswith("a_trend_prod_"))
async def analytics_trend_show(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split("_")[-1])
    data = await state.get_data()
    basis_id = data.get("trend_basis_id")
    if not basis_id:
        await cb.message.answer("❌ Нет базиса. Начните заново.", reply_markup=get_main_keyboard())
        await state.clear()
        await cb.answer()
        return

    session = await get_session()
    try:
        pbp = (
            await session.execute(
                select(ProductBasisPrice)
                .where(
                    ProductBasisPrice.product_id == pid,
                    ProductBasisPrice.basis_id == basis_id,
                    ProductBasisPrice.is_active.is_(True),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        basis = await session.get(Basis, basis_id)
        product = await session.get(Product, pid)
        if not pbp or not pbp.instrument_code:
            await cb.message.answer("❌ Не найден instrument_code для этого базиса/товара.")
            await cb.answer()
            return

        qh = await session.execute(
            select(SpimexPrice)
            .where(SpimexPrice.exchange_product_id == pbp.instrument_code)
            .order_by(SpimexPrice.date.desc())
            .limit(10)
        )
        rows = qh.scalars().all()
        if not rows:
            await cb.message.answer("❌ Нет истории. Импортируйте бюллетени за 10 дней.")
            await cb.answer()
            return

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
        text = (
            f"📈 <b>Тренд за 10 торговых дней</b>\n"
            f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name) if product else ''}\n"
            f"📍 <b>Базис:</b> {basis.name if basis else ''}\n"
            f"🔑 <b>Код:</b> {pbp.instrument_code}\n\n"
            + "<b>Последние дни:</b>\n"
            + "\n".join(lines[:5])
            + "\n\n"
            f"📉 <b>Минимум:</b> {pmin}\n"
            f"📈 <b>Максимум:</b> {pmax}\n"
            f"🔮 <b>Прогноз на завтра:</b> {pforecast}\n\n"
            + "<b>Прогноз:</b> "
            + "; ".join(forecast)
            + "\n\n<i>⚠️ Цена ориентировочная. Точное КП предоставляет ООО «НК-Востокнефтепродукт».</i>"
        )
        details_text = (
            f"📋 <b>Подробнее по дням (10)</b>\n"
            f"🛢️ {canonical_fuel_display_name(product.name) if product else ''}\n"
            f"📍 {basis.name if basis else ''}\n\n"
            + "\n".join(lines)
        )
        actions = InlineKeyboardBuilder()
        actions.add(
            InlineKeyboardButton(
                text="📝 Оставить заявку",
                callback_data=f"a_order_trend_{basis_id}_{pid}",
            )
        )
        actions.add(
            InlineKeyboardButton(
                text="🔔 Подписаться на снижение",
                callback_data=f"a_sub_trend_{basis_id}_{pid}",
            )
        )
        actions.add(
            InlineKeyboardButton(
                text="⚠️ Алерт по аномалиям (±3%)",
                callback_data=f"a_anom_{pbp.instrument_code}",
            )
        )
        actions.add(
            InlineKeyboardButton(
                text="📦 Тренд с доставкой до точки",
                callback_data=f"a_trend_delivery_{basis_id}_{pid}",
            )
        )
        if cb.from_user:
            LAST_ANALYTICS_DETAILS[int(cb.from_user.id)] = {
                **LAST_ANALYTICS_DETAILS.get(int(cb.from_user.id), {}),
                "trend": details_text,
            }
        actions.add(
            InlineKeyboardButton(
                text="📋 Подробнее по дням",
                callback_data="a_details_trend",
            )
        )
        actions.adjust(1)
        await cb.message.answer(text, parse_mode="HTML", reply_markup=actions.as_markup())
        await state.clear()
    finally:
        await session.close()
    await cb.answer()


@analytics_router.callback_query(F.data.startswith("a_trend_delivery_"))
async def analytics_trend_delivery_start(cb: CallbackQuery, state: FSMContext):
    # a_trend_delivery_{basis_id}_{product_id}
    parts = cb.data.split("_")
    basis_id = int(parts[3])
    product_id = int(parts[4])
    await state.clear()
    await state.update_data(trend_delivery_basis_id=basis_id, trend_delivery_product_id=product_id)
    await cb.message.answer(
        "Введите пункт назначения (поселение или ЖД станцию):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AnalyticsStates.waiting_for_delivery_destination)
    await cb.answer()


@analytics_router.message(AnalyticsStates.waiting_for_delivery_destination, F.text)
async def analytics_trend_delivery_destination(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    await state.update_data(trend_delivery_destination_text=t)
    await message.answer("Введите объем (тонн):", reply_markup=get_cancel_keyboard())
    await state.set_state(AnalyticsStates.waiting_for_delivery_volume)


@analytics_router.message(AnalyticsStates.waiting_for_delivery_volume, F.text)
async def analytics_trend_delivery_volume(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    try:
        vol = float(t.replace(",", "."))
        if vol <= 0:
            raise ValueError
    except Exception:
        await message.answer("❌ Введите корректный объем (например 60):")
        return

    data = await state.get_data()
    basis_id = int(data["trend_delivery_basis_id"])
    product_id = int(data["trend_delivery_product_id"])
    dest_text = str(data["trend_delivery_destination_text"])

    session = await get_session()
    try:
        basis = await session.get(Basis, basis_id)
        product = await session.get(Product, product_id)
        pbp = (
            await session.execute(
                select(ProductBasisPrice)
                .where(
                    ProductBasisPrice.product_id == product_id,
                    ProductBasisPrice.basis_id == basis_id,
                    ProductBasisPrice.is_active.is_(True),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if not basis or not product or not pbp or not pbp.instrument_code:
            await message.answer("❌ Нет данных по базису/товару/коду.", reply_markup=get_main_keyboard())
            await state.clear()
            return

        dest_key = normalize_city_name_key(dest_text)
        coords = await get_coordinates_from_city(dest_text, session)
        dest_station = None
        if not coords:
            dest_station = await find_rail_station_for_destination(session, dest_text, dest_key)
            if dest_station is None:
                await message.answer("❌ Не нашёл координаты/станцию назначения.", reply_markup=get_main_keyboard())
                await state.clear()
                return
            dest_lat, dest_lon = float(dest_station.latitude), float(dest_station.longitude)
        else:
            dest_lat, dest_lon = coords
            dest_station = await find_rail_station_for_destination(session, dest_text, dest_key)
        sakhalin_dest = is_sakhalin_destination(dest_text, dest_key, dest_station)

        # Дистанция + доставка за тонну
        if (getattr(basis, "transport_type", None) or "").lower() == "rail":
            o_lat = float(basis.rail_latitude or basis.latitude)
            o_lon = float(basis.rail_longitude or basis.longitude)
            if (
                dest_station is not None
                and (
                    not sakhalin_dest
                    or is_sakhalin_geo_point(float(dest_station.latitude), float(dest_station.longitude))
                )
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
                origin_esr=(str(basis.rail_esr).strip() if getattr(basis, "rail_esr", None) else None),
                dest_esr=(
                    str(dest_station.esr_code).strip()
                    if dest_station and getattr(dest_station, "esr_code", None)
                    else None
                ),
            )
            transport = "Ж/Д"
        else:
            if sakhalin_dest:
                await message.answer(
                    "❌ Для Сахалинской области доступна только Ж/Д доставка.",
                    reply_markup=get_main_keyboard(),
                )
                await state.clear()
                return
            dist = calculate_distance(dest_lat, dest_lon, float(basis.latitude), float(basis.longitude))
            transport = "Авто"

        rate = await get_delivery_rate(dist, (getattr(basis, "transport_type", None) or "auto"), session)
        delivery_per_ton = float(dist) * float(rate)
        if (getattr(basis, "transport_type", None) or "").lower() == "rail":
            delivery_per_ton += sakhalin_ferry_surcharge_per_ton(sakhalin_dest)

        # 10 дней истории (рыночная)
        qh = await session.execute(
            select(SpimexPrice)
            .where(SpimexPrice.exchange_product_id == pbp.instrument_code)
            .order_by(SpimexPrice.date.desc())
            .limit(10)
        )
        rows = qh.scalars().all()
        if not rows:
            await message.answer("❌ Нет истории по коду (spimex_prices).", reply_markup=get_main_keyboard())
            await state.clear()
            return

        lines: list[str] = []
        for i, r in enumerate(rows):
            d_obj = r.date.date() if isinstance(r.date, datetime) else None
            d = _fmt_ru_date(datetime.combine(d_obj, datetime.min.time())) if d_obj else str(r.date)
            base = float(r.price) if r.price is not None else 0.0
            prev_base = float(rows[i + 1].price) if i + 1 < len(rows) and rows[i + 1].price is not None else None
            arrow = _price_change_arrow(base, prev_base)
            total_per_ton = base + delivery_per_ton
            vol_day = f"{float(r.volume):,.0f}".replace(",", " ") if r.volume is not None else "—"
            lines.append(
                f"{d}: {arrow}{base:,.0f} + доставка {delivery_per_ton:,.0f} = <b>{total_per_ton:,.0f} ₽/т</b> (объем {vol_day} т)".replace(",", " ")
            )

        totals = [float(r.price or 0) + float(delivery_per_ton) for r in rows if r.price is not None]
        tmin, tmax, tforecast = _build_min_max_forecast(totals if totals else [delivery_per_ton])
        text = (
            f"📦 <b>Тренд с доставкой (10 дней)</b>\n"
            f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name)}\n"
            f"📍 <b>Базис:</b> {basis.name}\n"
            f"📍 <b>Назначение:</b> {dest_text}\n"
            f"📦 <b>Объем:</b> {vol:g} т\n"
            f"🚚 <b>Транспорт:</b> {transport}\n"
            f"📏 <b>Дистанция:</b> {dist:,.0f} км\n"
            f"🚚 <b>Доставка:</b> {delivery_per_ton:,.0f} ₽/т (ставка {float(rate):.2f} ₽/т·км)\n"
            + (
                f"⛴️ <b>Паром Сахалин:</b> +{sakhalin_ferry_surcharge_per_ton(True):,.0f} ₽/т\n"
                if sakhalin_dest and (getattr(basis, "transport_type", None) or "").lower() == "rail"
                else ""
            )
            +
            f"🔑 <b>Код:</b> {pbp.instrument_code}\n\n"
            f"📉 <b>Минимум (с доставкой):</b> {tmin}\n"
            f"📈 <b>Максимум (с доставкой):</b> {tmax}\n"
            f"🔮 <b>Прогноз на завтра:</b> {tforecast}\n"
            + "\n\n<i>⚠️ Цена ориентировочная. Точное КП предоставляет ООО «НК-Востокнефтепродукт».</i>"
        ).replace(",", " ")
        details_text = (
            f"📋 <b>Подробнее по дням (с доставкой)</b>\n"
            f"🛢️ {canonical_fuel_display_name(product.name)}\n"
            f"📍 {basis.name} → {dest_text}\n\n"
            + "\n".join(lines)
        )
        if message.from_user:
            LAST_ANALYTICS_DETAILS[int(message.from_user.id)] = {
                **LAST_ANALYTICS_DETAILS.get(int(message.from_user.id), {}),
                "delivery": details_text,
            }
        kb = InlineKeyboardBuilder()
        kb.add(InlineKeyboardButton(text="📋 Подробнее по дням", callback_data="a_details_delivery"))
        kb.add(InlineKeyboardButton(text="⬅️ В меню", callback_data="a_back"))
        kb.adjust(1)
        await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())
        await state.clear()
    finally:
        await session.close()


@analytics_router.callback_query(F.data.startswith("a_anom_"))
async def analytics_anomaly_subscribe(cb: CallbackQuery):
    code = cb.data.split("_", 2)[2].strip()
    if not code:
        await cb.answer()
        return
    session = await get_session()
    try:
        from db.database import AnomalyAlert

        user = await get_or_create_user(cb.message, session)
        existing = (
            await session.execute(
                select(AnomalyAlert)
                .where(
                    AnomalyAlert.user_id == user.id,
                    AnomalyAlert.instrument_code == code,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing:
            existing.is_active = True
            existing.threshold_pct = 3.0
        else:
            session.add(
                AnomalyAlert(
                    user_id=user.id,
                    instrument_code=code,
                    threshold_pct=3.0,
                    is_active=True,
                )
            )
        await session.commit()
    finally:
        await session.close()
    await cb.message.answer(
        f"✅ Подписка на аномалии включена: {code}\nПорог: ±3% за день.",
        reply_markup=get_main_keyboard(),
    )
    await cb.answer()


# =========================
# Compare 3 bases (5 days)
# =========================


@analytics_router.callback_query(F.data == "a_compare")
async def analytics_compare_start(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    session = await get_session()
    try:
        q = await session.execute(
            select(Product).where(Product.is_active.is_(True)).order_by(Product.name)
        )
        products = _pick_compare_products(q.scalars().all())
    finally:
        await session.close()
    await cb.message.answer(
        "📊 <b>Сравнить 3 базиса</b>\n\nВыберите топливо:",
        reply_markup=_products_inline_kb(products, prefix="a_cmp_prod_"),
        parse_mode="HTML",
    )
    await cb.answer()


@analytics_router.callback_query(F.data.startswith("a_cmp_prod_"))
async def analytics_compare_bases(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split("_")[-1])
    await state.update_data(
        compare_product_id=pid,
        compare_basis_ids=[],
        compare_basis_names=[],
        compare_basis_query="",
        compare_basis_offset=0,
    )
    await cb.message.answer(
        "Введите часть названия базиса для поиска (выберем 3 базиса по очереди):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AnalyticsStates.waiting_for_compare_basis_search)
    await cb.answer()


@analytics_router.message(AnalyticsStates.waiting_for_compare_basis_search, F.text)
async def analytics_compare_basis_search(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return

    data = await state.get_data()
    chosen = data.get("compare_basis_names") or []
    session = await get_session()
    try:
        await state.update_data(compare_basis_query=t, compare_basis_offset=0)
        kb = await _basis_search_kb(session, t, mode="cmp", offset=0)
    finally:
        await session.close()
    await message.answer(
        "Выберите базис (из 3):\n" + ("\n".join(f"• {x}" for x in chosen) if chosen else ""),
        reply_markup=kb,
    )


@analytics_router.callback_query(F.data.startswith("a_basis_page_cmp_"))
async def analytics_compare_basis_page(cb: CallbackQuery, state: FSMContext):
    offset = int(cb.data.split("_")[-1])
    data = await state.get_data()
    q = data.get("compare_basis_query", "")
    session = await get_session()
    try:
        kb = await _basis_search_kb(session, q, mode="cmp", offset=offset)
    finally:
        await session.close()
    await state.update_data(compare_basis_offset=offset)
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer()


@analytics_router.callback_query(F.data.startswith("a_pick_basis_cmp_"))
async def analytics_compare_basis_picked(cb: CallbackQuery, state: FSMContext):
    basis_id = int(cb.data.split("_")[-1])
    data = await state.get_data()

    ids = list(data.get("compare_basis_ids") or [])
    names = list(data.get("compare_basis_names") or [])

    session = await get_session()
    try:
        basis = await session.get(Basis, basis_id)
    finally:
        await session.close()

    if not basis:
        await cb.answer()
        return

    if basis_id not in ids:
        ids.append(basis_id)
        names.append(basis.name)
    await state.update_data(compare_basis_ids=ids, compare_basis_names=names)

    if len(ids) < 3:
        await cb.message.answer(
            f"✅ Добавлено: {basis.name}\n\nВведите часть названия следующего базиса:",
            reply_markup=get_cancel_keyboard(),
        )
        await cb.answer()
        return

    await cb.message.answer(
        "Введите пункт назначения (поселение или ЖД станцию):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AnalyticsStates.waiting_for_compare_destination)
    await cb.answer()


@analytics_router.message(AnalyticsStates.waiting_for_compare_destination, F.text)
async def analytics_compare_show(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return

    data = await state.get_data()
    pid = data.get("compare_product_id")
    basis_ids = data.get("compare_basis_ids") or []
    basis_names = data.get("compare_basis_names") or []
    if not pid or len(basis_ids) != 3:
        await message.answer("❌ Недостаточно данных. Начните заново.")
        await state.clear()
        return

    dest_key = normalize_city_name_key(t)
    session = await get_session()
    try:
        coords = await get_coordinates_from_city(t, session)
        dest_station = None
        if not coords:
            dest_station = await find_rail_station_for_destination(session, t, dest_key)
            if dest_station is None:
                await message.answer("❌ Не нашёл координаты/станцию назначения.")
                return
            dest_lat, dest_lon = float(dest_station.latitude), float(dest_station.longitude)
        else:
            dest_lat, dest_lon = coords
            dest_station = await find_rail_station_for_destination(session, t, dest_key)
        sakhalin_dest = is_sakhalin_destination(t, dest_key, dest_station)

        product = await session.get(Product, int(pid))
        if not product:
            await message.answer("❌ Топливо не найдено. Начните заново.", reply_markup=get_main_keyboard())
            await state.clear()
            return
        canonical_name = canonical_fuel_display_name(product.name)
        alias_ids_q = await session.execute(
            select(Product.id, Product.name)
            .where(Product.is_active.is_(True))
            .where(Product.name.isnot(None))
        )
        alias_ids = []
        for p_id, p_name in alias_ids_q.all():
            if canonical_fuel_display_name(str(p_name)) == canonical_name:
                alias_ids.append(int(p_id))
        if not alias_ids:
            alias_ids = [int(pid)]

        lines = [
            "📊 <b>Сравнение 3 базисов</b>",
            f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name) if product else ''}",
            f"📍 <b>Назначение:</b> {t}",
            "",
        ]

        best: tuple[float, str] | None = None
        best_basis_id: int | None = None
        best_product_id: int | None = None
        details_blocks: list[str] = []
        available: list[tuple[int, int, str]] = []
        for bid, _bname in zip(basis_ids, basis_names):
            b = await session.get(Basis, int(bid))
            if b and sakhalin_dest and (b.transport_type or "").lower() == "auto":
                lines.append(f"⛔ {b.name}: для Сахалина только Ж/Д доставка")
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
            if not pbp or not b:
                lines.append(f"❌ {b.name if b else _bname}: нет цены/кода")
                continue

            if b.transport_type == "rail":
                o_lat = float(b.rail_latitude or b.latitude)
                o_lon = float(b.rail_longitude or b.longitude)
                if (
                    dest_station is not None
                    and (
                        not sakhalin_dest
                        or is_sakhalin_geo_point(float(dest_station.latitude), float(dest_station.longitude))
                    )
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

            lines.append(
                f"<b>{b.name}</b> ({'Ж/Д' if b.transport_type=='rail' else 'Авто'})\n"
                f"- сегодня: {float(pbp.current_price):,.0f} ₽/т + доставка {delivery:,.0f} ₽/т = <b>{total:,.0f} ₽/т</b>\n"
                f"- дистанция: {dist:,.0f} км\n"
                f"- минимум 5 дней: {p_min:,.0f} ₽/т\n"
                f"- максимум 5 дней: {p_max:,.0f} ₽/т\n"
                f"- прогноз на завтра: {p_forecast:,.0f} ₽/т\n"
                f"\n"
            )
            details_blocks.append(
                f"<b>{b.name}</b> ({'Ж/Д' if b.transport_type=='rail' else 'Авто'})\n"
                f"- 5 дней: {'; '.join(hist_lines) if hist_lines else '—'}\n"
            )
            available.append((int(b.id), int(pbp.product_id), b.name))

            if best is None or total < best[0]:
                best = (total, b.name)
                best_basis_id = int(b.id)
                best_product_id = int(pbp.product_id)

        if best:
            lines.append(f"🥇 <b>Рекомендуем:</b> {best[1]} (минимальная цена с доставкой)")
            lines.append("\nВыберите базис ниже и оставьте заявку в 1 клик.")

        actions = InlineKeyboardBuilder()
        if message.from_user:
            LAST_COMPARE_CONTEXT[int(message.from_user.id)] = {
                "destination_text": t,
                "product_id": int(pid),
            }
            LAST_ANALYTICS_DETAILS[int(message.from_user.id)] = {
                **LAST_ANALYTICS_DETAILS.get(int(message.from_user.id), {}),
                "compare": "📋 <b>Подробнее по 5 дням</b>\n\n" + "\n".join(details_blocks),
            }
        for b_id, p_id, b_name in available[:3]:
            short_name = b_name if len(b_name) <= 22 else (b_name[:22] + "…")
            actions.add(
                InlineKeyboardButton(
                    text=f"📝 Заявка: {short_name}",
                    callback_data=f"a_order_cmp_{b_id}_{p_id}",
                )
            )
        for b_id, p_id, b_name in available[:3]:
            short_name = b_name if len(b_name) <= 18 else (b_name[:18] + "…")
            actions.add(
                InlineKeyboardButton(
                    text=f"🔔 Подписка: {short_name}",
                    callback_data=f"a_sub_cmp_{b_id}_{p_id}",
                )
            )
        if available:
            actions.add(
                InlineKeyboardButton(
                    text="📋 Подробнее по 5 дням",
                    callback_data="a_details_compare",
                )
            )
        if available:
            actions.adjust(1)

        await message.answer(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=actions.as_markup() if available else get_main_keyboard(),
        )
        await state.clear()
    finally:
        await session.close()


def _order_actions_kb(request_id: int) -> InlineKeyboardMarkup:
    # Переиспользуем стандартную клавиатуру после расчёта:
    # там уже есть «📝 Оставить заявку» и «🔔 Подписаться на снижение».
    return get_after_calculation_keyboard(request_id)


@analytics_router.callback_query(F.data.startswith("a_order_"))
async def analytics_order_start(cb: CallbackQuery, state: FSMContext):
    # Отвечаем на callback сразу, иначе Telegram может истечь по таймауту
    try:
        await cb.answer()
    except Exception:
        pass
    # a_order_(trend|cmp)_{basis_id}_{product_id}
    parts = cb.data.split("_")
    src = parts[2]
    basis_id = int(parts[3])
    product_id = int(parts[4])
    await state.clear()
    payload = {"order_basis_id": basis_id, "order_product_id": product_id}
    if src == "cmp" and cb.from_user:
        ctx = LAST_COMPARE_CONTEXT.get(int(cb.from_user.id), {})
        dest = str(ctx.get("destination_text") or "").strip()
        if dest:
            payload["order_destination_text"] = dest
            await state.update_data(**payload)
            await cb.message.answer(
                f"Назначение из сравнения: <b>{dest}</b>\nВведите объем (тонн):",
                parse_mode="HTML",
                reply_markup=get_cancel_keyboard(),
            )
            await state.set_state(AnalyticsStates.waiting_for_order_volume)
            return
    await state.update_data(**payload)
    await cb.message.answer(
        "Введите пункт назначения (поселение или ЖД станцию):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AnalyticsStates.waiting_for_order_destination)


@analytics_router.callback_query(F.data.startswith("a_sub_"))
async def analytics_subscribe_start(cb: CallbackQuery, state: FSMContext):
    # Отвечаем на callback сразу, иначе Telegram может истечь по таймауту
    try:
        await cb.answer()
    except Exception:
        pass

    # Подписка: НЕ спрашиваем объём. Клиент подписывается на цену за тонну.
    # Используем общий flow подписки из `bot/handlers.py` (SubscriptionStates).
    parts = cb.data.split("_")
    src = parts[2]
    basis_id = int(parts[3])
    product_id = int(parts[4])
    await state.clear()
    payload = {"order_basis_id": basis_id, "order_product_id": product_id}
    if src == "cmp" and cb.from_user:
        ctx = LAST_COMPARE_CONTEXT.get(int(cb.from_user.id), {})
        dest = str(ctx.get("destination_text") or "").strip()
        if dest:
            payload["order_destination_text"] = dest

    # Запускаем общий flow подписки на снижение (целевую цену за тонну)
    await state.update_data(
        product_id=product_id,
        volume=None,
        city_destination_id=None,
        # сохраняем контекст аналитики (на будущее, если захотим расширить подписку)
        analytics_basis_id=basis_id,
        analytics_destination_text=payload.get("order_destination_text"),
    )
    await cb.message.answer(
        "🔔 <b>Подписка на снижение цены</b>\n\n"
        "Введите целевую цену за тонну (₽/т):",
        parse_mode="HTML",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(SubscriptionStates.waiting_for_target_price)


@analytics_router.message(AnalyticsStates.waiting_for_order_destination, F.text)
async def analytics_order_destination(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    await state.update_data(order_destination_text=t)
    await message.answer("Введите объем (тонн):", reply_markup=get_cancel_keyboard())
    await state.set_state(AnalyticsStates.waiting_for_order_volume)


@analytics_router.message(AnalyticsStates.waiting_for_order_volume, F.text)
async def analytics_order_volume(message: Message, state: FSMContext):
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    try:
        vol = float(t.replace(",", "."))
        if vol <= 0:
            raise ValueError
    except Exception:
        await message.answer("❌ Введите корректный объем (например 60):")
        return

    data = await state.get_data()
    basis_id = int(data["order_basis_id"])
    product_id = int(data["order_product_id"])
    dest_text = str(data["order_destination_text"])

    session = await get_session()
    try:
        request_id = await _create_user_request_for_basis(
            session=session,
            message=message,
            basis_id=basis_id,
            product_id=product_id,
            destination_text=dest_text,
            volume=vol,
        )
    except Exception as e:
        logger.error(
            "analytics_order_volume failed: %s\n%s",
            repr(e),
            traceback.format_exc(),
        )
        await message.answer(
            "❌ Не удалось сформировать расчёт. Попробуйте ещё раз.\n"
            "Если повторится — я уже записал тех.детали в лог.",
            reply_markup=get_main_keyboard(),
        )
        await state.clear()
        return
    finally:
        await session.close()

    await message.answer(
        "✅ Готово. Выберите действие:",
        reply_markup=_order_actions_kb(request_id),
    )
    await state.clear()

