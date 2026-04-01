#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import math
import os
from datetime import datetime
from typing import Optional

from aiogram import F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from bot.keyboards import (
    get_after_calculation_keyboard,
    get_cancel_keyboard,
    get_main_keyboard,
    get_products_keyboard,
    get_subscription_keyboard,
    get_transport_keyboard,
)
from db.database import (
    Basis,
    CityDestination,
    PriceAlert,
    Product,
    ProductBasisPrice,
    RailStation,
    User,
    UserRequest,
    get_session,
)
from rail_tariff import (
    calculate_delivery_cost as calculate_rail_delivery_cost,
    compute_rail_tariff_distance_km,
    compute_rail_tariff_distance_debug,
    get_rail_rate,
)
from utils import (
    calculate_delivery_cost,
    canonical_fuel_display_name,
    get_best_transport_type,
    get_coordinates_from_city,
    get_delivery_rate,
    normalize_city_name_key,
)
from utils.rail_logistics import (
    basis_rail_origin_coords,
    basis_rail_origin_label,
    find_rail_station_for_destination,
    is_sakhalin_geo_point,
    is_sakhalin_destination,
    nearest_rail_station_to_point,
    sakhalin_ferry_surcharge_per_ton,
    sakhalin_ferry_surcharge_total,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Роутер для обработчиков
router = Router()

MAX_RAIL_DISTANCE_KM = float(os.getenv("MAX_RAIL_DISTANCE_KM", "6000"))
MAX_AUTO_DISTANCE_KM = float(os.getenv("MAX_AUTO_DISTANCE_KM", "1500"))


# Состояния FSM
class CalculationStates(StatesGroup):
    waiting_for_product = State()
    waiting_for_destination = State()
    waiting_for_basis_selection = State()
    waiting_for_volume = State()
    waiting_for_email = State()  # для ввода email при оформлении заявки


class SubscriptionStates(StatesGroup):
    waiting_for_product = State()
    waiting_for_target_price = State()
    waiting_for_volume = State()
    waiting_for_destination = State()
    waiting_for_email = State()


# ==================== ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОТМЕНЫ ====================

@router.message(F.text == "❌ Отмена")
async def cancel_operation(message: Message, state: FSMContext):
    """Глобальный обработчик отмены"""
    current_state = await state.get_state()
    # Даже если FSM-состояния нет, клиент ожидает возврат в меню.
    await state.clear()
    await message.answer(
        "❌ Действие отменено. Выберите пункт меню:",
        reply_markup=get_main_keyboard()
    )


# Разрешаем отмену и в нижнем регистре/без эмодзи
@router.message(F.text.in_(["Отмена", "отмена", "❌ отмена"]))
async def cancel_operation_manual(message: Message, state: FSMContext):
    await cancel_operation(message, state)


# ==================== ФУНКЦИИ ДЛЯ РАСЧЁТА РАССТОЯНИЙ ====================

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Рассчитывает расстояние между двумя точками по формуле гаверсинуса
    """
    R = 6371  # Радиус Земли в км
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


async def find_nearest_basises(
    session,
    city_lat: float,
    city_lon: float,
    product_id: int,
    limit: int = 10,
    max_distance_rail: float = MAX_RAIL_DISTANCE_KM,
    max_distance_auto: float = MAX_AUTO_DISTANCE_KM,
    destination_name_key: Optional[str] = None,
    destination_raw: Optional[str] = None,
):
    """
    Находит ближайшие базисы с ценами для указанного продукта
    Возвращает список с расстоянием, ценой и типом транспорта (из БД)
    
    Параметры:
        max_distance_rail: максимальное расстояние для Ж/Д (км)
        max_distance_auto: максимальное расстояние для авто (км)
        destination_name_key: нормализованное имя НП (для привязки к станции по названию)

    Авто: расстояние «по прямой» от координат клиента до координат базиса.
    Ж/Д: станция назначения — по названию (если есть в справочнике) или ближайшая к координатам;
         километраж — ТР №4 (если подключён и есть коды ЕСР) иначе оценка по гео.
    """
    dest_station = None
    if destination_name_key or destination_raw:
        dest_station = await find_rail_station_for_destination(
            session,
            destination_raw or "",
            destination_name_key or "",
        )
        if dest_station is not None:
            logger.info(
                "🚉 Станция назначения найдена по названию: %s (settlement: %s)",
                dest_station.name,
                getattr(dest_station, "settlement_name", None),
            )
    if dest_station is None:
        # Фоллбек: ближайшая станция к координатам точки назначения
        dest_station = await nearest_rail_station_to_point(session, city_lat, city_lon)
    sakhalin_dest = is_sakhalin_destination(
        destination_raw or "",
        destination_name_key or "",
        dest_station,
    )

    # Если станция назначения найдена — используем её координаты как координаты точки назначения
    # и для авто (оценка «по прямой»). Это важно, когда пользователь вводит именно станцию.
    dest_lat_eff = city_lat
    dest_lon_eff = city_lon
    if dest_station is not None and getattr(dest_station, "latitude", None) is not None and getattr(dest_station, "longitude", None) is not None:
        try:
            dest_lat_eff = float(dest_station.latitude)
            dest_lon_eff = float(dest_station.longitude)
        except Exception:
            dest_lat_eff = city_lat
            dest_lon_eff = city_lon

    # Получаем все цены для продукта
    prices_result = await session.execute(
        select(ProductBasisPrice, Basis)
        .join(Basis, ProductBasisPrice.basis_id == Basis.id)
        .where(ProductBasisPrice.product_id == product_id)
        .where(ProductBasisPrice.current_price > 0)
        .where(Basis.latitude != 0)
        .where(Basis.longitude != 0)
        .where(Basis.is_active == True)
    )
    
    prices_with_basises = prices_result.all()
    
    if not prices_with_basises:
        return []
    
    # Рассчитываем расстояние и полную стоимость для каждого
    basises_with_details = []
    for price, basis in prices_with_basises:
        if sakhalin_dest and basis.transport_type == "auto":
            logger.info(
                "⏭️ Авто базис %s исключен: Сахалин доступен только по Ж/Д",
                basis.name,
            )
            continue
        if basis.transport_type == "rail":
            o_lat, o_lon = basis_rail_origin_coords(basis)
            if (
                dest_station is not None
                and (
                    not sakhalin_dest
                    or is_sakhalin_geo_point(float(dest_station.latitude), float(dest_station.longitude))
                )
            ):
                d_lat, d_lon = float(dest_station.latitude), float(dest_station.longitude)
            else:
                d_lat, d_lon = city_lat, city_lon
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
        else:
            dist = calculate_distance(
                dest_lat_eff, dest_lon_eff, basis.latitude, basis.longitude
            )

        # Фильтр по расстоянию в зависимости от типа транспорта
        if basis.transport_type == 'rail':
            if dist > max_distance_rail:
                logger.info(f"⏭️ Ж/Д базис {basis.name} исключен (расстояние {dist:.0f} км > {max_distance_rail} км)")
                continue
        else:  # auto
            if dist > max_distance_auto:
                logger.info(f"⏭️ Авто базис {basis.name} исключен (расстояние {dist:.0f} км > {max_distance_auto} км)")
                continue
        
        # Получаем ставку доставки для этого типа транспорта
        rate = await get_delivery_rate(dist, basis.transport_type, session)
        
        # Рассчитываем стоимость доставки за тонну
        delivery_cost_per_ton = dist * rate
        if basis.transport_type == "rail":
            delivery_cost_per_ton += sakhalin_ferry_surcharge_per_ton(sakhalin_dest)
        
        # Полная стоимость за тонну (цена + доставка)
        total_cost_per_ton = price.current_price + delivery_cost_per_ton
        
        basises_with_details.append({
            'distance': dist,
            'basis': basis,
            'price': price,
            'transport_type': basis.transport_type,
            'rate': rate,
            'delivery_cost_per_ton': delivery_cost_per_ton,
            'total_cost_per_ton': total_cost_per_ton,
            'rail_dest_station_id': (
                dest_station.id
                if basis.transport_type == "rail" and dest_station is not None
                else None
            ),
            'rail_dest_station_name': (
                dest_station.name
                if basis.transport_type == "rail" and dest_station is not None
                else None
            ),
            'rail_origin_station_name': (
                basis_rail_origin_label(basis)
                if basis.transport_type == "rail"
                else None
            ),
            'is_sakhalin_destination': sakhalin_dest,
            'ferry_surcharge_per_ton': (
                sakhalin_ferry_surcharge_per_ton(sakhalin_dest)
                if basis.transport_type == "rail"
                else 0.0
            ),
        })
    
    # Сортируем по полной стоимости (цена + доставка)
    basises_with_details.sort(key=lambda x: x['total_cost_per_ton'])
    
    # Логируем статистику
    rail_count = sum(1 for b in basises_with_details if b['transport_type'] == 'rail')
    auto_count = sum(1 for b in basises_with_details if b['transport_type'] == 'auto')
    logger.info(f"✅ Найдено базисов: Ж/Д={rail_count}, Авто={auto_count}, всего={len(basises_with_details)}")
    
    return basises_with_details[:limit]


def get_best_options_keyboard(basises: list, product_id: int, 
                              dest_lat: float, dest_lon: float) -> InlineKeyboardMarkup:
    """
    Создаёт клавиатуру с лучшими вариантами:
    1. Самый выгодный
    2. Второй по выгодности
    """
    builder = InlineKeyboardBuilder()
    
    if not basises:
        return builder.as_markup()
    
    # 1. Самый выгодный вариант
    best = basises[0]
    transport_emoji = "🚛" if best['transport_type'] == 'auto' else "🚂"
    best_text = f"🥇 ЛУЧШИЙ: {transport_emoji} {best['basis'].name} - {best['total_cost_per_ton']:,.0f} руб/т"
    builder.add(InlineKeyboardButton(
        text=best_text,
        callback_data=f"select_basis_{best['basis'].id}_{product_id}"
    ))
    
    # 2. Второй по выгодности
    if len(basises) > 1:
        second = basises[1]
        transport_emoji = "🚛" if second['transport_type'] == 'auto' else "🚂"
        second_text = f"🥈 Альтернатива: {transport_emoji} {second['basis'].name} - {second['total_cost_per_ton']:,.0f} руб/т"
        builder.add(InlineKeyboardButton(
            text=second_text,
            callback_data=f"select_basis_{second['basis'].id}_{product_id}"
        ))

    # 3. Третий по выгодности
    if len(basises) > 2:
        third = basises[2]
        transport_emoji = "🚛" if third['transport_type'] == 'auto' else "🚂"
        third_text = f"🥉 Третий вариант: {transport_emoji} {third['basis'].name} - {third['total_cost_per_ton']:,.0f} руб/т"
        builder.add(InlineKeyboardButton(
            text=third_text,
            callback_data=f"select_basis_{third['basis'].id}_{product_id}"
        ))
    
    # 3. Кнопка "Показать все"
    builder.add(InlineKeyboardButton(
        text="📋 Показать все базисы",
        callback_data=f"show_all_basises_{product_id}"
    ))
    
    builder.adjust(1)
    return builder.as_markup()


def get_all_basises_keyboard(basises: list, product_id: int) -> InlineKeyboardMarkup:
    """Клавиатура со всеми базисами"""
    builder = InlineKeyboardBuilder()
    
    for i, item in enumerate(basises, 1):
        basis = item['basis']
        total = item['total_cost_per_ton']
        transport = "🚛" if item['transport_type'] == 'auto' else "🚂"
        
        button_text = f"{i}. {transport} {basis.name} - {total:,.0f} руб/т"
        builder.add(InlineKeyboardButton(
            text=button_text,
            callback_data=f"select_basis_{basis.id}_{product_id}"
        ))
    
    builder.adjust(1)
    return builder.as_markup()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

async def get_or_create_user(message: Message, session) -> User:
    """Получение или создание пользователя в БД"""
    telegram_id = message.from_user.id
    
    result = await session.execute(
        select(User).where(User.telegram_id == telegram_id)
    )
    user = result.scalar_one_or_none()
    
    if not user:
        user = User(
            telegram_id=telegram_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            is_active=True
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        logger.info(f"✅ Новый пользователь: {telegram_id}")
    
    return user


async def send_order_to_email(email: str, request: UserRequest, session):
    """Отправка заявки на email (SMTP) + фоллбек лог."""
    import asyncio
    import os
    import smtplib
    from email.message import EmailMessage
    from datetime import datetime, timezone

    # Куда слать заявку (ваша почта продаж). Если не задано — шлём только пользователю.
    sales_to = (os.getenv("SALES_TO_EMAIL") or "").strip()
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_port = int(os.getenv("SMTP_PORT") or "587")
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()
    smtp_from = (os.getenv("SMTP_FROM") or smtp_user or sales_to or "no-reply@example.com").strip()
    use_tls = (os.getenv("SMTP_TLS", "1").strip() not in ("0", "false", "False"))

    # Собираем карточку заявки
    product = await session.get(Product, request.product_id)
    basis = await session.get(Basis, request.basis_id)
    dest = await session.get(CityDestination, request.city_destination_id)
    price = await session.get(ProductBasisPrice, request.price_id) if request.price_id else None

    subject = f"Заявка #{request.id} — {canonical_fuel_display_name(product.name) if product else 'топливо'}"
    body = "\n".join(
        [
            f"Заявка #{request.id}",
            f"Дата: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            f"Пользователь: {request.user_id}",
            f"Email клиента: {email}",
            "",
            f"Топливо: {canonical_fuel_display_name(product.name) if product else '—'}",
            f"Базис: {basis.name if basis else '—'}",
            f"Назначение: {dest.name if dest else '—'}",
            f"Объем: {request.volume:g} т",
            f"Транспорт: {request.transport_type}",
            f"Расстояние: {request.distance_km:,.0f} км".replace(",", " "),
            f"Цена топлива: {request.base_price:,.0f} ₽/т".replace(",", " "),
            f"Доставка: {request.delivery_cost:,.0f} ₽".replace(",", " "),
            f"Итого: {request.total_price:,.0f} ₽".replace(",", " "),
            f"Код инструмента: {getattr(price, 'instrument_code', None) or '—'}",
        ]
    )

    if not smtp_host:
        logger.info("SMTP_HOST не задан — логирую заявку вместо отправки")
        logger.info("📧 Заявка #%s: to=%s sales_to=%s\n%s", request.id, email, sales_to or "—", body)
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    # Кому: сначала ваша почта (если задана), плюс клиент
    to_list = [x for x in [sales_to, email] if x]
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)

    def _send_sync() -> None:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
            if use_tls:
                smtp.starttls()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)

    await asyncio.to_thread(_send_sync)
    logger.info("📧 Заявка #%s отправлена: to=%s", request.id, ", ".join(to_list))


async def notify_managers_about_lead(bot, lead_text: str) -> None:
    """Уведомление менеджеров в Telegram (id из MANAGER_TELEGRAM_IDS)."""
    import os

    ids_raw = (os.getenv("MANAGER_TELEGRAM_IDS") or "").strip()
    if not ids_raw:
        return
    ids: list[int] = []
    for part in ids_raw.replace(";", ",").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            ids.append(int(p))
        except Exception:
            continue
    if not ids:
        return
    for tid in ids:
        try:
            await bot.send_message(tid, lead_text, parse_mode="HTML")
        except Exception:
            logger.exception("Не удалось уведомить менеджера telegram_id=%s", tid)


# ==================== ОБРАБОТЧИКИ КОМАНД ====================

@router.message(Command("myid"))
async def cmd_myid(message: Message):
    """Показать Telegram ID пользователя (для настройки MANAGER_TELEGRAM_IDS)."""
    if not message.from_user:
        return
    await message.answer(
        f"Ваш Telegram ID: <code>{message.from_user.id}</code>",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(),
    )


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработчик команды /start"""
    await state.clear()
    
    # Сохраняем пользователя в БД
    session = await get_session()
    try:
        user = await get_or_create_user(message, session)
    finally:
        await session.close()
    
    welcome_text = (
        "👋 Добро пожаловать в бот расчета стоимости ГСМ!\n\n"
        "Я помогу вам быстро рассчитать стоимость топлива с доставкой "
        "до любого населенного пункта России. Просто введите город, село или "
        "поселок назначения, и я найду ближайшие базисы с актуальными ценами.\n\n"
        "🔍 Нажмите «Новый расчет», чтобы начать.\n"
        "📋 В разделе «Мои подписки» можно управлять уведомлениями о снижении цен."
    )
    
    await message.answer(
        welcome_text,
        reply_markup=get_main_keyboard()
    )


@router.message(F.text == "🔍 Новый расчет")
async def new_calculation(message: Message, state: FSMContext):
    """Начало нового расчета"""
    session = await get_session()
    try:
        # Получаем активные продукты
        result = await session.execute(
            select(Product).where(Product.is_active == True)
        )
        products = result.scalars().all()
        
        if not products:
            await message.answer(
                "❌ К сожалению, временно нет доступных продуктов для расчета.",
                reply_markup=get_main_keyboard()
            )
            return
        
        await message.answer(
            "Выберите вид топлива:",
            reply_markup=await get_products_keyboard(products, session=session)
        )
    finally:
        await session.close()
    
    await state.set_state(CalculationStates.waiting_for_product)


@router.callback_query(CalculationStates.waiting_for_product, F.data.startswith("product_"))
async def process_product_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора топлива"""
    product_id = int(callback.data.split("_")[1])
    await state.update_data(product_id=product_id)
    
    # Удаляем сообщение с кнопками выбора топлива
    await callback.message.delete()
    
    # Отправляем новое сообщение с запросом города
    await callback.message.answer(
        "🏙️ Введите населенный пункт назначения (город, село, поселок):\n"
        "Например: Москва, Санкт-Петербург, Краснодар, Новосибирск",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(CalculationStates.waiting_for_destination)
    await callback.answer()


@router.message(CalculationStates.waiting_for_destination, F.text)
async def process_destination(message: Message, state: FSMContext):
    """Обработка ввода города назначения и поиск ближайших базисов"""
    t = (message.text or "").strip().lower()
    if "отмена" in t:
        await state.clear()
        await message.answer(
            "Расчет отменен. Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return
    
    destination = message.text.strip()
    destination_key = normalize_city_name_key(destination)
    
    # Получаем данные из состояния
    data = await state.get_data()
    product_id = data.get('product_id')
    
    if not product_id:
        await message.answer(
            "❌ Ошибка: не выбран продукт. Начните расчет заново.",
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    session = await get_session()
    try:
        # Отправляем сообщение о поиске
        await message.answer(
            f"🔍 Ищу базисы:\n"
            f"• Ж/Д: в радиусе {int(MAX_RAIL_DISTANCE_KM)} км от {destination}\n"
            f"• Авто: в радиусе {int(MAX_AUTO_DISTANCE_KM)} км от {destination}\n"
            f"(базисы дальше указанных расстояний не рассматриваются)",
            reply_markup=get_cancel_keyboard()
        )
        
        # Получаем координаты населенного пункта
        coords = await get_coordinates_from_city(destination, session)
        
        if not coords:
            # Фоллбек: пользователь мог ввести ЖД станцию (а не населённый пункт).
            dest_station_for_coords = await find_rail_station_for_destination(
                session,
                destination,
                destination_key,
            )
            if dest_station_for_coords is None:
                await message.answer(
                    f"❌ Не удалось определить координаты '{destination}'. "
                    f"Проверьте название и попробуйте снова.\n\n"
                    f"Примеры: Москва, Санкт-Петербург, Краснодар, Новосибирск\n"
                    f"Или попробуйте указать населённый пункт, к которому относится станция.",
                    reply_markup=get_cancel_keyboard()
                )
                return
            dest_lat = float(dest_station_for_coords.latitude)
            dest_lon = float(dest_station_for_coords.longitude)
        else:
            dest_lat, dest_lon = coords

        # 1) Пытаемся найти существующий город по координатам (защита от UNIQUE constraint)
        dest_obj = None
        geo_result = await session.execute(
            select(CityDestination)
            .where(
                func.abs(CityDestination.latitude - dest_lat) < 0.000001,
                func.abs(CityDestination.longitude - dest_lon) < 0.000001,
            )
            .limit(1)
        )
        dest_obj = geo_result.scalar_one_or_none()

        # 2) Если не нашли по гео — ищем по имени с учетом ё->е
        if not dest_obj:
            result = await session.execute(
                select(CityDestination).where(
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е") == destination_key
                )
            )
            dest_obj = result.scalar_one_or_none()
        
        # Если точное не найдено, ищем по вхождению (берем самый популярный)
        if not dest_obj:
            result = await session.execute(
                select(CityDestination)
                .where(
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                        f"%{destination_key}%"
                    )
                )
                .order_by(CityDestination.request_count.desc())
                .limit(1)
            )
            dest_obj = result.scalar_one_or_none()
        
        if dest_obj:
            dest_id = dest_obj.id
            logger.info(f"✅ Найден город: {dest_obj.name}")
            
            # Увеличиваем счетчик запросов
            await session.execute(
                update(CityDestination)
                .where(CityDestination.id == dest_id)
                .values(request_count=CityDestination.request_count + 1)
            )
        else:
            # Создаем новый населенный пункт
            new_dest = CityDestination(
                name=destination,
                latitude=dest_lat,
                longitude=dest_lon
            )
            session.add(new_dest)
            try:
                await session.commit()
                await session.refresh(new_dest)
                dest_id = new_dest.id
                logger.info(f"✅ Добавлен новый город: {destination}")
            except IntegrityError:
                # Если город уже успел появиться — откатываем и берём существующий.
                await session.rollback()
                dest_obj = await session.execute(
                    select(CityDestination).where(
                        func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е") == destination_key
                    ).limit(1)
                )
                dest_obj = dest_obj.scalar_one_or_none()
                if dest_obj:
                    dest_id = dest_obj.id
                else:
                    raise
        
        await state.update_data(destination_name=destination, destination_id=dest_id, 
                                dest_lat=dest_lat, dest_lon=dest_lon)
        
        # Получаем продукт для отображения
        product = await session.get(Product, product_id)
        canonical_product = canonical_fuel_display_name(product.name) if product else ""
        
        # Ищем ближайшие базисы с полной стоимостью
        nearby_basises = await find_nearest_basises(
            session,
            dest_lat,
            dest_lon,
            product_id,
            limit=10,
            max_distance_rail=MAX_RAIL_DISTANCE_KM,
            max_distance_auto=MAX_AUTO_DISTANCE_KM,
            destination_name_key=destination_key,
            destination_raw=destination,
        )
        
        if not nearby_basises:
            # Если нет базисов в указанных радиусах
            await message.answer(
                f"❌ К сожалению, не найдено базисов с ценами для {canonical_product}.\n\n"
                f"Ж/Д базисы искались в радиусе {int(MAX_RAIL_DISTANCE_KM)} км\n"
                f"Авто базисы искались в радиусе {int(MAX_AUTO_DISTANCE_KM)} км\n\n"
                f"Попробуйте другой населенный пункт или вид топлива.",
                reply_markup=get_main_keyboard()
            )
            await state.clear()
            return
        
        # Формируем сообщение с лучшими вариантами
        msg = f"📍 <b>Населенный пункт:</b> {destination}\n"
        msg += f"🛢️ <b>Топливо:</b> {canonical_product}\n\n"
        msg += f"🔍 <b>Лучшие варианты:</b>\n\n"
        
        for i, item in enumerate(nearby_basises[:3], 1):
            basis = item['basis']
            dist = item['distance']
            price = item['price'].current_price
            total = item['total_cost_per_ton']
            transport = "🚛" if item['transport_type'] == 'auto' else "🚂"
            transport_name = "Авто" if item['transport_type'] == 'auto' else "Ж/Д"
            
            # Эмодзи для первых трех
            if i == 1:
                medal = "🥇"
            elif i == 2:
                medal = "🥈"
            else:
                medal = "🥉"
            
            # Форматируем расстояние
            if dist < 10:
                dist_str = f"{dist:.1f} км"
            else:
                dist_str = f"{dist:.0f} км"
            
            msg += f"{medal} {transport} <b>{basis.name}</b> ({transport_name})\n"
            msg += f"   📏 Расстояние: {dist_str}\n"
            msg += f"   💰 Цена: {price:,.0f} руб/т\n"
            msg += f"   🚚 Доставка: {item['delivery_cost_per_ton']:,.0f} руб/т\n"
            msg += f"   💎 <b>ИТОГО: {total:,.0f} руб/т</b>\n\n"
        
        msg += "👇 Выберите вариант для детального расчета:"
        
        # Отправляем сообщение с клавиатурой выбора
        await message.answer(
            msg,
            parse_mode="HTML",
            reply_markup=get_best_options_keyboard(nearby_basises, product_id, 
                                                    dest_lat, dest_lon)
        )
        
        # Сохраняем найденные базисы в состояние
        await state.update_data(nearby_basises=nearby_basises)
        await state.set_state(CalculationStates.waiting_for_basis_selection)
        
    finally:
        await session.close()


@router.callback_query(CalculationStates.waiting_for_basis_selection, F.data.startswith("select_basis_"))
async def process_basis_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора базиса"""
    parts = callback.data.split("_")
    basis_id = int(parts[2])
    product_id = int(parts[3])
    
    await state.update_data(basis_id=basis_id)
    
    # Удаляем предыдущее сообщение с кнопками
    await callback.message.delete()
    
    # Запрашиваем объем
    await callback.message.answer(
        "📦 Введите объем в тоннах (например: 20, 45.5):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(CalculationStates.waiting_for_volume)
    await callback.answer()


@router.callback_query(CalculationStates.waiting_for_basis_selection, F.data.startswith("show_all_basises_"))
async def show_all_basises(callback: CallbackQuery, state: FSMContext):
    """Показать все базисы"""
    product_id = int(callback.data.split("_")[3])
    
    data = await state.get_data()
    nearby_basises = data.get('nearby_basises', [])
    
    if not nearby_basises:
        await callback.message.edit_text("❌ Нет доступных базисов")
        return
    
    # Формируем сообщение со всеми базисами
    msg = f"📋 <b>Все доступные базисы:</b>\n\n"
    
    for i, item in enumerate(nearby_basises, 1):
        basis = item['basis']
        dist = item['distance']
        total = item['total_cost_per_ton']
        transport = "🚛" if item['transport_type'] == 'auto' else "🚂"
        
        dist_str = f"{dist:.0f} км" if dist >= 10 else f"{dist:.1f} км"
        msg += f"{i}. {transport} <b>{basis.name}</b> - {total:,.0f} руб/т\n"
        msg += f"   📏 {dist_str}, доставка: {item['delivery_cost_per_ton']:,.0f} руб/т\n\n"
    
    await callback.message.edit_text(
        msg,
        parse_mode="HTML",
        reply_markup=get_all_basises_keyboard(nearby_basises, product_id)
    )
    await callback.answer()


@router.message(CalculationStates.waiting_for_volume, F.text)
async def process_volume(message: Message, state: FSMContext):
    """Обработка ввода объема"""
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer(
            "Расчет отменен. Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return
    
    try:
        volume = float(message.text.replace(',', '.'))
        if volume <= 0:
            raise ValueError("Объем должен быть положительным")
    except ValueError:
        await message.answer(
            "❌ Пожалуйста, введите корректное число (например: 20 или 45.5):",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(volume=volume)
    
    # Получаем выбранный базис из состояния
    data = await state.get_data()
    nearby_basises = data.get('nearby_basises', [])
    selected_basis_id = data.get('basis_id')
    
    # Находим выбранный базис
    selected = None
    for item in nearby_basises:
        if item['basis'].id == selected_basis_id:
            selected = item
            break
    
    if selected:
        # Сразу переходим к расчету, без выбора транспорта
        await calculate_final_result(message, state, selected, volume)
    else:
        await message.answer(
            "❌ Ошибка: базис не найден. Начните расчет заново.",
            reply_markup=get_main_keyboard()
        )
        await state.clear()


async def calculate_final_result(message: Message, state: FSMContext, selected: dict, volume: float):
    """Финальный расчет с выбранным базисом"""
    data = await state.get_data()
    
    session = await get_session()
    try:
        # Получаем данные
        product = await session.get(Product, data['product_id'])
        basis = selected['basis']
        destination = await session.get(CityDestination, data['destination_id'])
        product_price = selected['price']
        
        distance_km = selected['distance']
        final_transport = selected['transport_type']

        # Ж/Д: пересчитываем километраж так же, как в подборе (ТР №4 / гео), по id станции назначения
        if final_transport == "rail":
            rs_id = selected.get("rail_dest_station_id")
            dest_station = await session.get(RailStation, rs_id) if rs_id else None
            o_lat, o_lon = basis_rail_origin_coords(basis)
            if dest_station is not None:
                if (
                    not bool(selected.get("is_sakhalin_destination"))
                    or is_sakhalin_geo_point(float(dest_station.latitude), float(dest_station.longitude))
                ):
                    d_lat, d_lon = float(dest_station.latitude), float(dest_station.longitude)
                else:
                    d_lat, d_lon = float(destination.latitude), float(destination.longitude)
            else:
                d_lat, d_lon = float(destination.latitude), float(destination.longitude)
            distance_km = compute_rail_tariff_distance_km(
                o_lat,
                o_lon,
                d_lat,
                d_lon,
                origin_esr=(
                    str(basis.rail_esr).strip() if getattr(basis, "rail_esr", None) else None
                ),
                dest_esr=(
                    str(dest_station.esr_code).strip()
                    if dest_station and getattr(dest_station, "esr_code", None)
                    else None
                ),
            )
        
        # Расчет стоимости доставки
        if final_transport == 'rail':
            # Для Ж/Д: стоимость от расстояния (ТР №4 или оценка) и ставки
            rail_result = calculate_rail_delivery_cost(distance_km, volume)
            delivery_cost = rail_result['total_cost']
            ferry_surcharge_total = sakhalin_ferry_surcharge_total(
                volume,
                bool(selected.get("is_sakhalin_destination")),
            )
            delivery_cost += ferry_surcharge_total
            rate = rail_result['rate_per_ton_km']
            wagons_info = f"\n   🚂 Вагонов: {rail_result['wagons_needed']} (по {rail_result['tons_per_wagon']} т)"
        else:
            # Для авто используем упрощенный расчет
            rate = await get_delivery_rate(distance_km, final_transport, session)
            delivery_cost = distance_km * volume * rate
            ferry_surcharge_total = 0.0
            wagons_info = ""
        
        # Итоговая цена
        base_total = product_price.current_price * volume
        total_price = base_total + delivery_cost
        
        # Сохраняем запрос в историю
        user = await get_or_create_user(message, session)
        
        user_request = UserRequest(
            user_id=user.id,
            product_id=product.id,
            basis_id=basis.id,
            price_id=product_price.id,
            city_destination_id=destination.id,
            volume=volume,
            base_price=product_price.current_price,
            distance_km=distance_km,
            transport_type=final_transport,
            delivery_cost=delivery_cost,
            total_price=total_price
        )
        session.add(user_request)
        await session.commit()
        await session.refresh(user_request)
        
        # Форматируем расстояние для вывода
        if distance_km < 10:
            dist_str = f"{distance_km:.1f}"
        else:
            dist_str = f"{distance_km:.0f}"
        
        transport_emoji = "🚛" if final_transport == 'auto' else "🚂"
        transport_name = "Авто" if final_transport == 'auto' else "Ж/Д"

        rail_leg = ""
        if final_transport == "rail":
            rs_name = selected.get("rail_dest_station_name")
            ro_name = selected.get("rail_origin_station_name")
            if rs_name or ro_name:
                rail_leg = (
                    f"🚉 <b>Станция отпр.:</b> {ro_name or '—'}\n"
                    f"🚉 <b>Станция назн.:</b> {rs_name or '—'}\n"
                )

        dist_label = (
            "📏 <b>Расстояние (ж/д, оценка маршрута):</b>"
            if final_transport == "rail"
            else "📏 <b>Расстояние:</b>"
        )

        result_text = (
            f"✅ <b>Результат расчета</b>\n\n"
            f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name)}\n"
            f"📍 <b>Базис:</b> {basis.name}\n"
            f"🏙️ <b>Назначение:</b> {destination.name}\n"
            f"📦 <b>Объем:</b> {volume} т\n"
            f"{transport_emoji} <b>Транспорт:</b> {transport_name}\n"
            f"{rail_leg}"
            f"{dist_label} {dist_str} км\n"
            f"{wagons_info}\n\n"
            f"💰 <b>Стоимость топлива:</b> {product_price.current_price:,.0f} руб/т = {base_total:,.2f} руб\n"
            f"🚚 <b>Доставка:</b> {delivery_cost:,.2f} руб (ставка {rate:.2f} руб/т·км)\n"
            + (
                f"⛴️ <b>Паром Сахалин:</b> +{ferry_surcharge_total:,.2f} руб "
                f"({sakhalin_ferry_surcharge_per_ton(True):,.0f} руб/т)\n"
                if ferry_surcharge_total > 0
                else ""
            )
            +
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <b>ИТОГО:</b> {total_price:,.2f} руб\n\n"
            f"<i>⚠️ Цены и расчет в боте являются предварительными.</i>\n"
            f"<i>Точный расчет и коммерческое предложение предоставляет только ООО «НК-Востокнефтепродукт».</i>"
        )
        
        # После расчета показываем только три кнопки
        await message.answer(
            result_text,
            parse_mode="HTML",
            reply_markup=get_after_calculation_keyboard(user_request.id)
        )
        
        # Очищаем состояние
        await state.clear()
            
    finally:
        await session.close()


@router.callback_query(F.data.startswith("create_order_"))
async def create_order(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия на кнопку 'Оставить заявку'"""
    request_id = int(callback.data.split("_")[2])
    
    session = await get_session()
    try:
        # Получаем данные запроса
        request = await session.get(UserRequest, request_id)
        if not request:
            await callback.message.answer("❌ Запрос не найден")
            return
        
        # Получаем пользователя
        user = await get_or_create_user(callback.message, session)

        # Создаём/обновляем лид
        from db.database import Lead
        lead = (
            await session.execute(
                select(Lead)
                .where(Lead.user_id == user.id, Lead.request_id == request_id)
                .limit(1)
            )
        ).scalar_one_or_none()
        if not lead:
            lead = Lead(user_id=user.id, request_id=request_id, status="email_pending", source="calc")
            session.add(lead)
            await session.commit()
        
        # Сохраняем ID запроса в состояние
        await state.update_data(request_id=request_id)
        
        # Если у пользователя уже есть email
        if user.email:
            # Отправляем заявку на email
            await send_order_to_email(user.email, request, session)
            lead.email = user.email
            lead.status = "sent"
            await session.commit()

            # Уведомляем менеджеров
            try:
                product = await session.get(Product, request.product_id)
                basis = await session.get(Basis, request.basis_id)
                dest = await session.get(CityDestination, request.city_destination_id)
                lead_text = (
                    f"📝 <b>Новая заявка #{request.id}</b>\n\n"
                    f"📧 Email: <b>{user.email}</b>\n"
                    f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
                    f"📍 {basis.name if basis else '—'} → {dest.name if dest else '—'}\n"
                    f"📦 {request.volume:g} т, {('Ж/Д' if request.transport_type=='rail' else 'Авто')}\n"
                    f"💎 Итого: <b>{float(request.total_price):,.0f}</b> ₽".replace(",", " ")
                )
                await notify_managers_about_lead(callback.bot, lead_text)
            except Exception:
                logger.exception("Не удалось сформировать/отправить уведомление менеджерам")
            
            await callback.message.answer(
                "✅ **Заявка успешно отправлена!**\n\n"
                f"Детали заявки направлены на ваш email: {user.email}\n"
                "Наш менеджер свяжется с вами в ближайшее время.",
                reply_markup=get_main_keyboard()
            )
            await callback.answer()
            return
        
        # Если email нет - запрашиваем
        await callback.message.answer(
            "📧 **Для оформления заявки укажите ваш email**\n\n"
            "На этот адрес мы отправим детальный расчет и коммерческое предложение.\n\n"
            "Отправляя email, вы соглашаетесь на получение КП/уведомлений.\n\n"
            "Введите email в ответном сообщении:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(CalculationStates.waiting_for_email)
        
    finally:
        await session.close()
    
    await callback.answer()


@router.message(CalculationStates.waiting_for_email, F.text)
async def process_order_email(message: Message, state: FSMContext):
    """Обработка ввода email для оформления заявки"""
    if message.text == "❌ Отмена":
        await message.answer(
            "Оформление заявки отменено.",
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    email = message.text.strip()
    if '@' not in email or '.' not in email:
        await message.answer(
            "❌ Пожалуйста, введите корректный email (например: name@domain.ru):"
        )
        return
    
    data = await state.get_data()
    request_id = data.get('request_id')
    
    session = await get_session()
    try:
        # Сохраняем email пользователя
        user = await get_or_create_user(message, session)
        user.email = email
        await session.commit()

        # Обновляем лид, если он уже создан на create_order
        from db.database import Lead
        lead = None
        if request_id:
            lead = (
                await session.execute(
                    select(Lead)
                    .where(Lead.user_id == user.id, Lead.request_id == int(request_id))
                    .limit(1)
                )
            ).scalar_one_or_none()
            if lead:
                lead.email = email
                lead.status = "email_pending"
                await session.commit()
        
        # Если есть ID запроса - отправляем заявку
        if request_id:
            request = await session.get(UserRequest, request_id)
            if request:
                await send_order_to_email(email, request, session)
                if lead:
                    lead.status = "sent"
                    await session.commit()

                # Уведомляем менеджеров
                try:
                    product = await session.get(Product, request.product_id)
                    basis = await session.get(Basis, request.basis_id)
                    dest = await session.get(CityDestination, request.city_destination_id)
                    lead_text = (
                        f"📝 <b>Новая заявка #{request.id}</b>\n\n"
                        f"📧 Email: <b>{email}</b>\n"
                        f"🛢️ {canonical_fuel_display_name(product.name) if product else '—'}\n"
                        f"📍 {basis.name if basis else '—'} → {dest.name if dest else '—'}\n"
                        f"📦 {request.volume:g} т, {('Ж/Д' if request.transport_type=='rail' else 'Авто')}\n"
                        f"💎 Итого: <b>{float(request.total_price):,.0f}</b> ₽".replace(",", " ")
                    )
                    await notify_managers_about_lead(message.bot, lead_text)
                except Exception:
                    logger.exception("Не удалось сформировать/отправить уведомление менеджерам")
                
                await message.answer(
                    "✅ **Заявка успешно отправлена!**\n\n"
                    f"Детали заявки направлены на ваш email: {email}\n"
                    "Наш менеджер свяжется с вами в ближайшее время.",
                    reply_markup=get_main_keyboard()
                )
            else:
                await message.answer(
                    "✅ Email успешно сохранен!",
                    reply_markup=get_main_keyboard()
                )
        else:
            await message.answer(
                "✅ Email успешно сохранен!",
                reply_markup=get_main_keyboard()
            )
    finally:
        await session.close()
    
    await state.clear()


@router.callback_query(F.data.startswith("subscribe_after_"))
async def subscribe_after_calculation(callback: CallbackQuery, state: FSMContext):
    """Подписка после расчета"""
    request_id = int(callback.data.split("_")[2])
    
    session = await get_session()
    try:
        request = await session.get(UserRequest, request_id)
        if not request:
            await callback.message.answer("❌ Запрос не найден")
            return
        
        product = await session.get(Product, request.product_id)
        
        await state.update_data(
            product_id=request.product_id,
            volume=request.volume,
            city_destination_id=request.city_destination_id
        )
        
        await callback.message.answer(
            f"🔔 Создание подписки на снижение цены для {product.name}\n\n"
            f"Введите целевую цену за тонну:"
        )
        
        await state.set_state(SubscriptionStates.waiting_for_target_price)
    finally:
        await session.close()
    
    await callback.answer()


@router.message(SubscriptionStates.waiting_for_target_price, F.text)
async def process_target_price(message: Message, state: FSMContext):
    """Обработка ввода целевой цены"""
    try:
        target_price = float(message.text.replace(',', '.'))
        if target_price <= 0:
            raise ValueError("Цена должна быть положительной")
    except ValueError:
        await message.answer(
            "❌ Введите корректное число (например: 45000):"
        )
        return
    
    await state.update_data(target_price=target_price)
    
    await message.answer(
        "📧 Введите ваш email для уведомлений (или отправьте /skip чтобы пропустить).\n"
        "Отправляя email, вы соглашаетесь на получение уведомлений."
    )
    await state.set_state(SubscriptionStates.waiting_for_email)


@router.message(SubscriptionStates.waiting_for_email, F.text)
async def process_subscription_email(message: Message, state: FSMContext):
    """Обработка ввода email и создание подписки"""
    email = None
    if message.text != "/skip":
        email = message.text.strip()
        if '@' not in email or '.' not in email:
            await message.answer("❌ Введите корректный email:")
            return
    
    data = await state.get_data()
    
    session = await get_session()
    try:
        user = await get_or_create_user(message, session)
        # Всегда сохраняем email пользователя (если введён)
        if email:
            user.email = email
        
        alert = PriceAlert(
            user_id=user.id,
            product_id=data['product_id'],
            target_price=data['target_price'],
            volume=data.get('volume'),
            email=email,
            is_active=True
        )
        session.add(alert)
        await session.commit()
        
        await message.answer(
            "✅ Подписка успешно создана! Я уведомлю вас, когда цена упадет.",
            reply_markup=get_main_keyboard()
        )
    finally:
        await session.close()
    
    await state.clear()


@router.message(F.text == "📋 Мои подписки")
async def my_subscriptions(message: Message):
    """Просмотр активных подписок"""
    session = await get_session()
    try:
        user = await get_or_create_user(message, session)
        
        result = await session.execute(
            select(PriceAlert)
            .where(PriceAlert.user_id == user.id)
            .where(PriceAlert.is_active == True)
        )
        alerts = result.scalars().all()
        
        if not alerts:
            await message.answer(
                "📋 У вас нет активных подписок.",
                reply_markup=get_main_keyboard()
            )
            return
        
        text = "📋 Ваши активные подписки:\n\n"
        for alert in alerts:
            product = await session.get(Product, alert.product_id)
            text += f"• {product.name}: уведомить при цене {alert.target_price} руб/т\n"
        
        await message.answer(text, reply_markup=get_main_keyboard())
    finally:
        await session.close()


@router.message(F.text == "ℹ️ О боте")
async def about_bot(message: Message):
    """Информация о боте"""
    text = (
        "ℹ️ <b>О боте расчета ГСМ</b>\n\n"
        "Этот бот помогает:\n"
        "• Рассчитать стоимость топлива с доставкой\n"
        "• Находит ближайшие к вам базисы по всей России\n"
        "• Работает с любыми населенными пунктами (города, села, поселки)\n"
        "• Отслеживает снижение цен\n"
        "• Отправляет уведомления о выгодных предложениях\n\n"
        "⚠️ Все расчеты в боте предварительные.\n"
        "Точные коммерческие условия предоставляет только ООО «НК-Востокнефтепродукт».\n\n"
        "Разработано для клиентов ООО «НК-Востокнефтепродукт»"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.callback_query(F.data == "new_calculation")
async def callback_new_calculation(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия на кнопку 'Новый расчет' из инлайн-клавиатуры"""
    await callback.message.delete()
    await new_calculation(callback.message, state)
    await callback.answer()


@router.callback_query(F.data.startswith("why_"))
async def explain_why(callback: CallbackQuery):
    """Короткий разбор расчёта «почему так» по сохраненному UserRequest (клиентская версия)."""
    request_id = int(callback.data.split("_")[1])
    session = await get_session()
    try:
        req = await session.get(UserRequest, request_id)
        if not req:
            await callback.message.answer("❌ Запрос не найден.")
            return
        basis = await session.get(Basis, req.basis_id)
        product = await session.get(Product, req.product_id)
        dest = await session.get(CityDestination, req.city_destination_id)
        pbp = await session.get(ProductBasisPrice, req.price_id) if req.price_id else None

        if not basis or not product or not dest:
            await callback.message.answer("❌ Не хватает данных для пояснения.")
            return

        # Пытаемся найти станцию назначения по названию (для ж/д)
        dest_raw = dest.name
        dest_key = normalize_city_name_key(dest_raw)
        dest_station = await find_rail_station_for_destination(session, dest_raw, dest_key)
        sakhalin_dest = is_sakhalin_destination(dest_raw, dest_key, dest_station)

        if basis.transport_type == "rail":
            o_lat, o_lon = basis_rail_origin_coords(basis)
            d_lat = float(dest_station.latitude) if dest_station else float(dest.latitude)
            d_lon = float(dest_station.longitude) if dest_station else float(dest.longitude)

            dbg = compute_rail_tariff_distance_debug(
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
            dist_txt = f"{float(dbg['distance_km']):,.0f}".replace(",", " ")
            dest_point = dest_station.name if dest_station else dest.name
            dist_line = f"📏 <b>Расстояние (ж/д):</b> {dist_txt} км\n"
        else:
            d_lat = float(dest.latitude)
            d_lon = float(dest.longitude)
            dist = calculate_distance(d_lat, d_lon, float(basis.latitude), float(basis.longitude))
            dest_point = dest.name
            dist_line = f"📏 <b>Расстояние (авто):</b> {dist:,.0f} км\n".replace(",", " ")

        base_price = float(req.base_price)
        rate = (
            float(req.delivery_cost) / (float(req.distance_km) * float(req.volume))
            if req.distance_km and req.volume
            else 0.0
        )
        text = (
            f"🧾 <b>Почему так?</b>\n\n"
            f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name)}\n"
            f"📍 <b>Базис:</b> {basis.name} ({basis.transport_type})\n"
            f"📍 <b>Назначение:</b> {dest_point}\n"
            f"📦 <b>Объем:</b> {float(req.volume):g} т\n"
            f"{dist_line}"
            f"💰 <b>Цена топлива:</b> {base_price:,.0f} ₽/т (код {pbp.instrument_code if pbp else '—'})\n"
            f"🚚 <b>Доставка:</b> {float(req.delivery_cost):,.0f} ₽ (≈ {rate:.2f} ₽/т·км)\n"
            + (
                f"⛴️ <b>Паром Сахалин:</b> включен (+{sakhalin_ferry_surcharge_per_ton(True):,.0f} ₽/т)\n"
                if sakhalin_dest and basis.transport_type == "rail"
                else ""
            )
            +
            f"💎 <b>Итого:</b> {float(req.total_price):,.0f} ₽\n\n"
            f"<i>Если нужно — откройте тех. детали.</i>"
        ).replace(",", " ")

        kb = InlineKeyboardBuilder()
        kb.add(
            InlineKeyboardButton(
                text="Подробнее (тех.)",
                callback_data=f"whytech_{request_id}",
            )
        )
        kb.add(
            InlineKeyboardButton(
                text="🔄 Новый расчет",
                callback_data="new_calculation",
            )
        )
        kb.adjust(1)
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())
    finally:
        await session.close()
    await callback.answer()


@router.callback_query(F.data.startswith("whytech_"))
async def explain_why_tech(callback: CallbackQuery):
    """Техническая расшифровка расчёта «почему так» (ESR/источник расстояния/фоллбеки)."""
    request_id = int(callback.data.split("_")[1])
    session = await get_session()
    try:
        req = await session.get(UserRequest, request_id)
        if not req:
            await callback.message.answer("❌ Запрос не найден.")
            return
        basis = await session.get(Basis, req.basis_id)
        product = await session.get(Product, req.product_id)
        dest = await session.get(CityDestination, req.city_destination_id)
        pbp = await session.get(ProductBasisPrice, req.price_id) if req.price_id else None

        if not basis or not product or not dest:
            await callback.message.answer("❌ Не хватает данных для пояснения.")
            return

        dest_raw = dest.name
        dest_key = normalize_city_name_key(dest_raw)
        dest_station = await find_rail_station_for_destination(session, dest_raw, dest_key)
        sakhalin_dest = is_sakhalin_destination(dest_raw, dest_key, dest_station)

        if basis.transport_type == "rail":
            o_lat, o_lon = basis_rail_origin_coords(basis)
            d_lat = float(dest_station.latitude) if dest_station else float(dest.latitude)
            d_lon = float(dest_station.longitude) if dest_station else float(dest.longitude)

            dbg = compute_rail_tariff_distance_debug(
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

            src = dbg.get("source", "geo_fallback")
            if src == "tr4_module":
                src_txt = "ТР №4 (модуль)"
            elif src == "local_books":
                src_txt = "Книги 2/3 (локально)"
            else:
                src_txt = "гео‑оценка (фоллбек)"

            dist_txt = f"{float(dbg['distance_km']):,.0f}".replace(",", " ")
            straight_txt = (
                f"{float(dbg.get('straight_km', 0.0)):,.0f}".replace(",", " ")
                if dbg.get("straight_km") is not None
                else "—"
            )
            factor_txt = f"{float(dbg.get('route_factor', 1.0)):.2f}"
            err_txt = dbg.get("error")

            tech = (
                f"🧾 <b>Почему так? (тех.)</b>\n\n"
                f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name)}\n"
                f"📍 <b>Базис:</b> {basis.name} ({basis.transport_type})\n"
                f"📍 <b>Назначение:</b> {dest.name}\n"
                f"🚉 <b>Станция назн. (если найдена):</b> {dest_station.name if dest_station else '—'}\n"
                f"🔢 <b>ESR отпр.:</b> {dbg.get('origin_esr','—')}\n"
                f"🔢 <b>ESR назн.:</b> {dbg.get('dest_esr','—')}\n"
                f"📏 <b>Источник расстояния:</b> {src_txt}\n"
                f"📏 <b>Расстояние (ж/д):</b> {dist_txt} км\n"
                + (
                    f"📐 <b>Прямая:</b> {straight_txt} км, коэф={factor_txt}\n"
                    if src == "geo_fallback"
                    else ""
                )
                + (f"⚠️ <b>Причина фоллбека:</b> {err_txt}\n" if err_txt else "")
                + "\n"
                f"💰 <b>Цена топлива:</b> {float(req.base_price):,.0f} ₽/т (код {pbp.instrument_code if pbp else '—'})\n"
                f"🚚 <b>Доставка:</b> {float(req.delivery_cost):,.0f} ₽\n"
                + (
                    f"⛴️ <b>Паром Сахалин:</b> включен (+{sakhalin_ferry_surcharge_per_ton(True):,.0f} ₽/т)\n"
                    if sakhalin_dest
                    else ""
                )
                +
                f"💎 <b>Итого:</b> {float(req.total_price):,.0f} ₽\n"
            ).replace(",", " ")
        else:
            dist = float(req.distance_km) if req.distance_km else 0.0
            tech = (
                f"🧾 <b>Почему так? (тех.)</b>\n\n"
                f"🛢️ <b>Топливо:</b> {canonical_fuel_display_name(product.name)}\n"
                f"📍 <b>Базис:</b> {basis.name} ({basis.transport_type})\n"
                f"📍 <b>Назначение:</b> {dest.name}\n"
                f"📏 <b>Расстояние (авто):</b> {dist:,.0f} км\n"
                f"💰 <b>Цена топлива:</b> {float(req.base_price):,.0f} ₽/т (код {pbp.instrument_code if pbp else '—'})\n"
                f"🚚 <b>Доставка:</b> {float(req.delivery_cost):,.0f} ₽\n"
                f"💎 <b>Итого:</b> {float(req.total_price):,.0f} ₽\n"
            ).replace(",", " ")

        kb = InlineKeyboardBuilder()
        kb.add(
            InlineKeyboardButton(
                text="⬅️ Назад (коротко)",
                callback_data=f"why_{request_id}",
            )
        )
        kb.adjust(1)
        await callback.message.answer(tech, parse_mode="HTML", reply_markup=kb.as_markup())
    finally:
        await session.close()
    await callback.answer()