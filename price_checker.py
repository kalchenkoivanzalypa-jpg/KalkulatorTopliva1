# -*- coding: utf-8 -*-
"""
Фоновая проверка подписок PriceAlert: сравнение с минимальной ценой по продукту.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from aiogram import Bot
from sqlalchemy import select

from config import config
from db.database import (
    AnomalyAlert,
    AsyncSessionLocal,
    PriceAlert,
    Basis,
    Product,
    ProductBasisPrice,
    SpimexPrice,
    User,
)

logger = logging.getLogger(__name__)


def _html_to_plain(html: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    s = re.sub(r"</p>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return s.strip()


async def _notify_user_html(bot: Optional[Bot], user: User, html: str, subject: str) -> None:
    """Telegram (если есть id и бот), иначе email."""
    plain = _html_to_plain(html)
    if user.telegram_id and int(user.telegram_id) > 0 and bot is not None:
        await bot.send_message(int(user.telegram_id), html, parse_mode="HTML")
        return
    if user.email:
        from web.email_util import send_smtp_email

        await send_smtp_email(subject=subject, body=plain, to_addrs=[user.email])
        return
    raise RuntimeError("Нет канала уведомления (ни Telegram, ни email)")


async def _check_once(bot: Optional[Bot]) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PriceAlert).where(PriceAlert.is_active.is_(True))
        )
        alerts = result.scalars().all()
        if not alerts:
            return

        now = datetime.now(timezone.utc)

        for alert in alerts:
            min_row = (
                await session.execute(
                    select(
                        ProductBasisPrice.current_price,
                        ProductBasisPrice.instrument_code,
                        Product.name,
                        Basis.name,
                        Basis.transport_type,
                    )
                    .join(Product, Product.id == ProductBasisPrice.product_id)
                    .join(Basis, Basis.id == ProductBasisPrice.basis_id)
                    .where(
                        ProductBasisPrice.product_id == alert.product_id,
                        ProductBasisPrice.is_active.is_(True),
                        ProductBasisPrice.current_price > 0,
                    )
                    .order_by(ProductBasisPrice.current_price.asc())
                    .limit(1)
                )
            ).one_or_none()
            if min_row is None:
                continue
            min_price, instrument_code, product_name, basis_name, transport_type = min_row
            if float(min_price) > float(alert.target_price):
                continue

            user = await session.get(User, alert.user_id)
            if not user:
                continue
            if not (
                (user.telegram_id and int(user.telegram_id) > 0)
                or (user.email and str(user.email).strip())
            ):
                continue

            text = (
                f"🔔 <b>Сработала подписка на цену</b>\n\n"
                f"🛢️ Продукт: <b>{product_name}</b>\n"
                f"📍 Базис: <b>{basis_name}</b> ({'Ж/Д' if (transport_type or '').lower()=='rail' else 'Авто'})\n"
                f"🔑 Код: <code>{instrument_code or '—'}</code>\n\n"
                f"💰 Минимальная цена сейчас: <b>{float(min_price):,.0f}</b> ₽/т\n"
                f"🎯 Ваша целевая цена: <b>{float(alert.target_price):,.0f}</b> ₽/т"
            )
            try:
                await _notify_user_html(
                    bot,
                    user,
                    text,
                    subject="Сработала подписка на цену — НК калькулятор топлива",
                )
            except Exception as exc:
                logger.warning(
                    "Не удалось отправить уведомление user_id=%s: %s",
                    user.id,
                    exc,
                )

            alert.is_active = False
            alert.triggered_at = now
            alert.notification_sent = True

        await session.commit()


async def _check_anomalies_once(bot: Optional[Bot]) -> None:
    """Проверка подписок на аномалии по instrument_code (история spimex_prices)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AnomalyAlert).where(AnomalyAlert.is_active.is_(True))
        )
        alerts = result.scalars().all()
        if not alerts:
            return

        for alert in alerts:
            user = await session.get(User, alert.user_id)
            if not user:
                continue
            if not (
                (user.telegram_id and int(user.telegram_id) > 0)
                or (user.email and str(user.email).strip())
            ):
                continue

            q = await session.execute(
                select(SpimexPrice)
                .where(SpimexPrice.exchange_product_id == alert.instrument_code)
                .order_by(SpimexPrice.date.desc())
                .limit(2)
            )
            rows = q.scalars().all()
            if len(rows) < 2:
                continue

            cur = rows[0]
            prev = rows[1]
            if cur.price is None or prev.price is None:
                continue

            prev_price = float(prev.price)
            cur_price = float(cur.price)
            if prev_price <= 0:
                continue

            pct = (cur_price - prev_price) / prev_price * 100.0
            if abs(pct) < float(alert.threshold_pct):
                continue

            if alert.last_notified_date is not None and cur.date is not None:
                try:
                    if alert.last_notified_date.date() == cur.date.date():
                        continue
                except Exception:
                    pass

            sign = "⬆️" if pct > 0 else "⬇️"
            d = cur.date.date().isoformat() if cur.date else "—"
            text = (
                f"⚠️ <b>Аномалия цены</b>\n\n"
                f"Код: <b>{alert.instrument_code}</b>\n"
                f"Дата: <b>{d}</b>\n\n"
                f"{sign} Изменение: <b>{pct:+.1f}%</b>\n"
                f"Вчера: <b>{prev_price:,.0f}</b> ₽/т\n"
                f"Сегодня: <b>{cur_price:,.0f}</b> ₽/т"
            ).replace(",", " ")

            try:
                await _notify_user_html(
                    bot,
                    user,
                    text,
                    subject="Аномалия цены — НК калькулятор топлива",
                )
            except Exception as exc:
                logger.warning(
                    "Не удалось отправить anomaly уведомление user_id=%s: %s",
                    user.id,
                    exc,
                )
                continue

            alert.last_notified_date = cur.date

        await session.commit()


async def start_price_checker(bot: Optional[Bot]) -> None:
    """Запуск бесконечного цикла проверки (интервал из CHECK_INTERVAL_MINUTES)."""
    minutes = max(1, int(getattr(config, "CHECK_INTERVAL_MINUTES", 60) or 60))
    interval_sec = minutes * 60

    async def _loop() -> None:
        while True:
            try:
                await _check_once(bot)
                await _check_anomalies_once(bot)
            except Exception:
                logger.exception("Ошибка в price_checker")
            await asyncio.sleep(interval_sec)

    asyncio.create_task(_loop())
    logger.info(
        "Проверка цен для подписок запущена (каждые %s мин)",
        minutes,
    )


__all__ = ["start_price_checker"]
