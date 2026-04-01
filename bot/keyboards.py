# -*- coding: utf-8 -*-
import re
from typing import List

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from sqlalchemy import func, select

from db.database import Basis, Product, ProductBasisPrice
from utils import canonical_fuel_display_name


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главная клавиатура"""
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="🔍 Новый расчет"))
    builder.add(KeyboardButton(text="📋 Мои подписки"))
    builder.add(KeyboardButton(text="📊 Аналитика"))
    builder.add(KeyboardButton(text="ℹ️ О боте"))
    builder.adjust(2, 2)
    return builder.as_markup(resize_keyboard=True)


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой отмены"""
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="❌ Отмена"))
    return builder.as_markup(resize_keyboard=True)


async def get_products_keyboard(products: List[Product], session=None) -> InlineKeyboardMarkup:
    """Клавиатура выбора топлива (канонические коды)"""
    builder = InlineKeyboardBuilder()

    active_products = [p for p in products if p.is_active]
    if not active_products:
        builder.adjust(1)
        return builder.as_markup()

    # Группируем по каноническим кодам, чтобы Telegram не ругался на длину markup.
    canonical_to_best: dict[str, tuple[int, int]] = {}

    product_ids = [p.id for p in active_products]
    price_count_map: dict[int, int] = {pid: 0 for pid in product_ids}

    if session is not None and product_ids:
        # Берём product_id с максимумом цен в product_basis_prices
        rows = await session.execute(
            select(ProductBasisPrice.product_id, func.count(ProductBasisPrice.id))
            .where(ProductBasisPrice.product_id.in_(product_ids))
            .where(ProductBasisPrice.is_active == True)
            .group_by(ProductBasisPrice.product_id)
        )
        for pid, cnt in rows.all():
            price_count_map[int(pid)] = int(cnt)

    for product in active_products:
        canonical = canonical_fuel_display_name(product.name)
        pid = product.id
        cnt = price_count_map.get(pid, 0)

        best = canonical_to_best.get(canonical)
        if best is None:
            canonical_to_best[canonical] = (pid, cnt)
        else:
            best_pid, best_cnt = best
            if cnt > best_cnt or (cnt == best_cnt and pid < best_pid):
                canonical_to_best[canonical] = (pid, cnt)

    allowed_re = r"^(АИ-(92|95|100)-К5|ДТ-(А|Е|З|Л)-К5|ТС-1|Мазут топочный М100)$"
    for canonical, (pid, _cnt) in sorted(canonical_to_best.items(), key=lambda x: x[0]):
        if not re.match(allowed_re, canonical):
            continue
        builder.add(InlineKeyboardButton(text=canonical, callback_data=f"product_{pid}"))

    builder.adjust(1)
    return builder.as_markup()


async def get_basises_keyboard(basises: List[Basis]) -> InlineKeyboardMarkup:
    """Клавиатура выбора базиса"""
    builder = InlineKeyboardBuilder()
    for basis in basises:
        if basis.is_active:
            builder.add(InlineKeyboardButton(
                text=basis.city,
                callback_data=f"basis_{basis.id}"
            ))
    builder.adjust(1)
    return builder.as_markup()


async def get_basises_with_prices_keyboard(basises: List[Basis], product_id: int, session) -> InlineKeyboardMarkup:
    """Клавиатура выбора базиса с ценами для этого продукта"""
    builder = InlineKeyboardBuilder()
    
    for basis in basises:
        if basis.is_active:
            # Получаем цену для этого продукта на этом базисе
            price_result = await session.execute(
                select(ProductBasisPrice)
                .where(ProductBasisPrice.product_id == product_id)
                .where(ProductBasisPrice.basis_id == basis.id)
                .where(ProductBasisPrice.is_active == True)
            )
            price = price_result.scalar_one_or_none()
            
            if price:
                button_text = f"{basis.city} - {price.current_price:,.0f} руб/т"
            else:
                button_text = f"{basis.city} (нет цены)"
            
            builder.add(InlineKeyboardButton(
                text=button_text,
                callback_data=f"basis_{basis.id}"
            ))
    
    builder.adjust(1)
    return builder.as_markup()


def get_transport_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора транспорта"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🚛 Авто", callback_data="transport_auto"))
    builder.add(InlineKeyboardButton(text="🚂 Ж/Д", callback_data="transport_rail"))
    builder.add(InlineKeyboardButton(text="⚡ Оптимально", callback_data="transport_best"))
    builder.adjust(2, 1)
    return builder.as_markup()


def get_subscription_keyboard(alert_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для управления подпиской"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="🔔 Отписаться", 
        callback_data=f"unsubscribe_{alert_id}"
    ))
    builder.add(InlineKeyboardButton(
        text="💰 Оформить заявку", 
        callback_data=f"order_from_alert_{alert_id}"
    ))
    builder.adjust(1)
    return builder.as_markup()


def get_after_calculation_keyboard(request_id: int = None) -> InlineKeyboardMarkup:
    """Клавиатура после расчета"""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="🔔 Подписаться на снижение", 
        callback_data=f"subscribe_after_{request_id}" if request_id else "subscribe_new"
    ))
    builder.add(InlineKeyboardButton(
        text="📝 Оставить заявку", 
        callback_data=f"create_order_{request_id}" if request_id else "create_order"
    ))
    if request_id:
        builder.add(InlineKeyboardButton(
            text="🧾 Почему так?",
            callback_data=f"why_{request_id}",
        ))
    builder.add(InlineKeyboardButton(
        text="🔄 Новый расчет", 
        callback_data="new_calculation"
    ))
    builder.adjust(1)
    return builder.as_markup()