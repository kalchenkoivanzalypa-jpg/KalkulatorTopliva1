# -*- coding: utf-8 -*-
"""
Фоновая проверка подписок PriceAlert: сравнение с минимальной ценой по продукту.
"""
from __future__ import annotations

import asyncio
import logging
import re
import os
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from aiogram import Bot
from sqlalchemy import select

from config import config
from max_bot.max_api import MaxApi, MaxApiError
from db.database import (
    AnomalyAlert,
    AsyncSessionLocal,
    BasisDigestSubscription,
    PriceAlert,
    Basis,
    CityDestination,
    Product,
    ProductBasisPrice,
    SpimexPrice,
    TableDigestSubscription,
    User,
)

logger = logging.getLogger(__name__)


def _html_to_plain(html: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    s = re.sub(r"</p>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return s.strip()


_max_api_cached: MaxApi | None = None
_max_api_token: str | None = None


def _max_api() -> MaxApi | None:
    global _max_api_cached
    global _max_api_token
    token = (os.getenv("MAX_BOT_TOKEN", "") or "").strip()
    if not token:
        _max_api_cached = None
        _max_api_token = None
        return None
    # If token unchanged and client exists — reuse it.
    if _max_api_cached is not None and _max_api_token == token:
        return _max_api_cached
    # (Re)create client. Important: do NOT permanently cache None — env may appear after restart.
    try:
        _max_api_cached = MaxApi(token)
        _max_api_token = token
        return _max_api_cached
    except Exception:
        _max_api_cached = None
        _max_api_token = token
        logger.exception("Не удалось инициализировать MAX API клиент")
        return None


async def _notify_user_table_digest(
    bot: Optional[Bot],
    user: User,
    html: str,
    subject: str,
    *,
    notify_max: bool,
    notify_email: bool,
) -> None:
    """Таблица: отправка только на email (MAX не поддерживает большие таблицы)."""
    plain = _html_to_plain(html)
    sent_any = False
    if notify_email and user.email:
        from web.email_util import send_smtp_email

        await send_smtp_email(subject=subject, body=plain, html=html, to_addrs=[user.email])
        sent_any = True
    if not sent_any:
        raise RuntimeError("Нет канала для table_digest (email не включен/не задан)")


async def _notify_user_html(bot: Optional[Bot], user: User, html: str, subject: str) -> None:
    """MAX (если есть max_user_id), иначе email. Telegram-канал отключён."""
    plain = _html_to_plain(html)
    # MAX messenger
    if getattr(user, "max_user_id", None):
        api = _max_api()
        if api is not None:
            try:
                await api.send_message(user_id=int(user.max_user_id), text=html, fmt="html")
                return
            except Exception:
                logger.exception("MAX notify failed for user_id=%s max_user_id=%s", user.id, getattr(user, "max_user_id", None))
        else:
            logger.warning(
                "MAX_BOT_TOKEN не задан/клиент не инициализирован — пропускаю MAX уведомление user_id=%s max_user_id=%s",
                user.id,
                getattr(user, "max_user_id", None),
            )
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
            # Если в подписке указан базис — проверяем цену только для этого базиса.
            # Иначе (старое поведение) — берём минимальную цену по продукту среди всех базисов.
            basis_filter = []
            if getattr(alert, "basis_id", None):
                basis_filter = [ProductBasisPrice.basis_id == int(alert.basis_id)]

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
                        *basis_filter,
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
                or (getattr(user, "max_user_id", None) and int(getattr(user, "max_user_id", 0) or 0) > 0)
                or (user.email and str(user.email).strip())
            ):
                continue

            text = (
                f"🔔 <b>Сработала подписка на цену</b>\n\n"
                f"🛢️ Продукт: <b>{product_name}</b>\n"
                f"📍 Базис: <b>{basis_name}</b> ({'Ж/Д' if (transport_type or '').lower()=='rail' else 'Авто'})\n"
                f"🔑 Код: <code>{instrument_code or '—'}</code>\n\n"
                f"💰 Минимальная цена сейчас: <b>{float(min_price):,.0f}</b> ₽/т\n"
                f"🎯 Ваша целевая цена: <b>{float(alert.target_price):,.0f}</b> ₽/т\n\n"
                f"<b>Хотите купить топливо по биржевым ценам с доставкой?</b>\n"
                f"Напишите: <b>nk.vnp@mail.ru</b>\n"
                f"Сайт компании: https://nk-vsnp.ru/\n\n"
                f"Полная аналитика: https://calc.nk-vsnp.ru/analytics"
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
                or (getattr(user, "max_user_id", None) and int(getattr(user, "max_user_id", 0) or 0) > 0)
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
            thr = float(alert.threshold_pct)
            direction = str(getattr(alert, "direction", None) or "any").strip().lower()
            if direction not in ("any", "up", "down"):
                direction = "any"

            if direction == "up":
                if pct < thr:
                    continue
            elif direction == "down":
                if pct > -thr:
                    continue
            else:
                if abs(pct) < thr:
                    continue
                continue

            if alert.last_notified_date is not None and cur.date is not None:
                try:
                    if alert.last_notified_date.date() == cur.date.date():
                        continue
                except Exception:
                    pass

            sign = "⬆️" if pct > 0 else "⬇️"
            d = cur.date.date().isoformat() if cur.date else "—"
            dir_label = {"any": "рост/падение", "up": "только рост", "down": "только падение"}.get(direction, "рост/падение")
            text = (
                f"⚠️ <b>Аномалия цены</b>\n\n"
                f"Код: <b>{alert.instrument_code}</b>\n"
                f"Дата: <b>{d}</b>\n\n"
                f"Режим: <b>{dir_label}</b>\n"
                f"{sign} Изменение: <b>{pct:+.1f}%</b>\n"
                f"Вчера: <b>{prev_price:,.0f}</b> ₽/т\n"
                f"Сегодня: <b>{cur_price:,.0f}</b> ₽/т\n\n"
                f"<b>Хотите купить топливо по биржевым ценам с доставкой?</b>\n"
                f"Напишите: <b>nk.vnp@mail.ru</b>\n"
                f"Сайт компании: https://nk-vsnp.ru/\n\n"
                f"Полная аналитика: https://calc.nk-vsnp.ru/analytics"
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


_MSK = ZoneInfo("Europe/Moscow")


async def _digest_price_rows_for_basis(
    session, sub: BasisDigestSubscription
) -> list[tuple[str, str, float]]:
    """
    Одна строка на каноническое топливо: приоритет ГОСТ-кода (как в расчёте),
    цена — последняя биржевая из spimex_prices.
    """
    from utils import canonical_fuel_display_name
    from utils.market_price_freshness import display_price_for_pbp, pick_best_product_basis_price_row

    basis_id = int(sub.basis_id)
    all_products_q = await session.execute(
        select(Product.id, Product.name).where(Product.is_active.is_(True)).where(Product.name.isnot(None))
    )
    id_to_canon: dict[int, str] = {}
    for pid, pname in all_products_q.all():
        id_to_canon[int(pid)] = canonical_fuel_display_name(str(pname))

    q = (
        select(Product.id, Product.name)
        .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
        .where(ProductBasisPrice.basis_id == basis_id)
        .where(ProductBasisPrice.is_active.is_(True))
        .where(Product.is_active.is_(True))
        .where(ProductBasisPrice.current_price > 0)
    )
    if not bool(getattr(sub, "all_products", False)):
        pid = getattr(sub, "product_id", None)
        if not pid:
            return []
        q = q.where(Product.id == int(pid))

    products = (await session.execute(q)).all()
    seen_canon: set[str] = set()
    out: list[tuple[str, str, float]] = []

    for pid, pname in products:
        canon = id_to_canon.get(int(pid)) or canonical_fuel_display_name(str(pname or ""))
        if canon in seen_canon:
            continue
        seen_canon.add(canon)
        alias_ids = [int(i) for i, c in id_to_canon.items() if c == canon]
        if not alias_ids:
            alias_ids = [int(pid)]
        pbp = await pick_best_product_basis_price_row(
            session,
            basis_id=basis_id,
            product_ids=alias_ids,
            exclude_ai100_price_stub=(canon == "АИ-100-К5"),
        )
        if not pbp:
            continue
        px = float(await display_price_for_pbp(session, pbp))
        if px <= 0:
            continue
        code = str(getattr(pbp, "instrument_code", None) or "").strip() or "—"
        out.append((canon, code, px))

    out.sort(key=lambda x: x[0])
    return out


async def _digest_html_for_subscription(session, sub: BasisDigestSubscription) -> Optional[str]:
    """Текст ежедневной сводки по одной подписке (биржевые цены на базисе)."""
    basis = await session.get(Basis, int(sub.basis_id))
    if not basis:
        return None
    basis_name = str(basis.name or "—")
    transport = "Ж/Д" if str(basis.transport_type or "").lower() == "rail" else "Авто"

    mode = str(getattr(sub, "delivery_mode", "") or "prices_only").strip() or "prices_only"
    if mode not in ("prices_only", "with_delivery"):
        mode = "prices_only"

    rows = await _digest_price_rows_for_basis(session, sub)

    lines: list[str] = [
        "📊 <b>Ежедневная сводка по базису</b>",
        "",
        f"Базис: <b>{basis_name}</b> ({transport})",
        "После выхода бюллетеня СПбМТСБ.",
        "",
    ]
    if not rows:
        lines.append("По выбранному охвату пока нет активных цен в базе — после импорта бюллетеня строки появятся.")
    else:
        if mode == "prices_only":
            lines.append("<b>Цены на базисе (СПбМТСБ, ₽/т):</b>")
            for name, code, price in rows:
                c = (code or "—").strip() or "—"
                lines.append(
                    f"• {name}: <b>{float(price):,.0f}</b>, код <code>{c}</code>".replace(",", " ")
                )
        else:
            dest_id = getattr(sub, "destination_id", None)
            if not dest_id:
                lines.append("⚠️ Для этой подписки не задана точка доставки — включите заново и выберите назначение.")
            else:
                dest = await session.get(CityDestination, int(dest_id))
                if not dest:
                    lines.append("⚠️ Точка доставки не найдена — включите подписку заново.")
                else:
                    dest_name = str(getattr(sub, "destination_name", None) or getattr(dest, "name", None) or "—")
                    dest_key = str(getattr(sub, "destination_key", None) or "").strip()
                    lines.append(f"<b>Точка доставки:</b> {dest_name}")
                    lines.append("")
                    lines.append("<b>Цены (СПбМТСБ) / доставка (оценка) / итог (₽/т):</b>")

                    from utils import canonical_fuel_display_name, get_delivery_rate_sync, normalize_city_name_key
                    from utils.rail_logistics import (
                        basis_rail_origin_coords,
                        find_rail_station_for_destination,
                        fixed_delivery_per_ton_override,
                        rail_dest_station_for_city_key,
                        sakhalin_ferry_surcharge_per_ton,
                        is_sakhalin_destination,
                    )
                    from rail_tariff import calculate_delivery_cost, compute_rail_tariff_distance_km_cached
                    from bot.handlers import calculate_distance

                    async def _distance_km_for_basis() -> float | None:
                        t = str(getattr(basis, "transport_type", "") or "").lower()
                        if t == "rail":
                            o = basis_rail_origin_coords(basis)
                            if not o:
                                return None
                            o_lat, o_lon = float(o[0]), float(o[1])
                            d_lat, d_lon = float(dest.latitude), float(dest.longitude)
                            st = None
                            if dest_key:
                                st = await rail_dest_station_for_city_key(session, dest_key)
                            if st is None:
                                st = await find_rail_station_for_destination(session, dest_name, dest_key or dest_name)
                            d_esr = (str(getattr(st, "esr_code", "")) if st is not None else "").strip() or None
                            o_esr = (str(getattr(basis, "rail_esr", "")) or "").strip() or None
                            try:
                                return float(
                                    await asyncio.to_thread(
                                        compute_rail_tariff_distance_km_cached,
                                        o_lat,
                                        o_lon,
                                        d_lat,
                                        d_lon,
                                        o_esr,
                                        d_esr,
                                    )
                                )
                            except Exception:
                                return None

                        o_lat = getattr(basis, "latitude", None)
                        o_lon = getattr(basis, "longitude", None)
                        if o_lat is None or o_lon is None:
                            return None
                        try:
                            return float(
                                calculate_distance(
                                    float(o_lat),
                                    float(o_lon),
                                    float(dest.latitude),
                                    float(dest.longitude),
                                )
                            )
                        except Exception:
                            return None

                    dist_km = await _distance_km_for_basis()
                    if not dist_km or float(dist_km) <= 0:
                        lines.append("⚠️ Не удалось посчитать доставку для этого базиса/назначения.")
                    else:
                        lines.append(f"Расстояние: <b>{float(dist_km):,.0f}</b> км".replace(",", " "))
                        t = str(getattr(basis, "transport_type", "") or "").lower()
                        sak = is_sakhalin_destination(dest_name, dest_key or normalize_city_name_key(dest_name), None)
                        ferry_pt = float(sakhalin_ferry_surcharge_per_ton(sak) or 0.0)

                        async def _delivery_per_ton_for_product(nm: str) -> float:
                            canon = canonical_fuel_display_name(nm or "")
                            vol = (
                                65.0
                                if (
                                    canon.startswith("ДТ-")
                                    or canon == "Мазут топочный М100"
                                    or canon == "ТС-1"
                                )
                                else 60.0
                            )
                            fixed_pt = fixed_delivery_per_ton_override(
                                str(getattr(basis, "name", "") or ""),
                                normalize_city_name_key(dest_key or dest_name),
                            )
                            if fixed_pt is not None:
                                return float(fixed_pt) + ferry_pt
                            if t == "rail":
                                rr = await asyncio.to_thread(
                                    calculate_delivery_cost,
                                    float(dist_km),
                                    float(vol),
                                    nm,
                                )
                                return float(rr["cost_per_ton"]) + ferry_pt
                            return float(dist_km) * float(get_delivery_rate_sync(float(dist_km), "auto"))

                        for name, code, price in rows:
                            p = float(price)
                            c = (code or "—").strip() or "—"
                            delivery_per_ton = float(await _delivery_per_ton_for_product(str(name or "")))
                            total = p + delivery_per_ton
                            lines.append(
                                (
                                    f"• {name}: цена <b>{p:,.0f}</b> + доставка <b>{delivery_per_ton:,.0f}</b> = "
                                    f"<b>{total:,.0f}</b> ₽/т, код <code>{c}</code>"
                                ).replace(",", " ")
                            )
                        lines.append("")
                        lines.append(
                            "⚠️ Цены — из бюллетеней СПбМТСБ. Логистика ориентировочная и может меняться. "
                            "Точное КП можно запросить у ООО «НК-Востокнефтепродукт» на сайте или по почте ниже."
                        )
    lines.append("")
    lines.append("<b>Хотите купить топливо по биржевым ценам с доставкой?</b>")
    lines.append("Напишите: <b>nk.vnp@mail.ru</b>")
    lines.append("Сайт компании: https://nk-vsnp.ru/")
    lines.append("")
    lines.append("Полная аналитика: https://calc.nk-vsnp.ru/analytics")
    return "\n".join(lines)


async def _send_digest_once(bot: Optional[Bot], *, force: bool = False) -> int:
    """
    Сводка по базису. Шлём только при force=True (после импорта бюллетеня).
    Возвращает число успешно отмеченных отправок.
    """
    if not force:
        return 0
    now_msk = datetime.now(_MSK)
    today_msk = now_msk.date()
    sent = 0

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BasisDigestSubscription).where(BasisDigestSubscription.is_active.is_(True))
        )
        subs = list(result.scalars().all())

        for sub in subs:
            if sub.last_sent_at is not None:
                last = sub.last_sent_at
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if last.astimezone(_MSK).date() == today_msk:
                    continue

            user = await session.get(User, int(sub.user_id))
            if not user:
                continue
            if not (
                (user.telegram_id and int(user.telegram_id) > 0)
                or (getattr(user, "max_user_id", None) and int(getattr(user, "max_user_id", 0) or 0) > 0)
                or (user.email and str(user.email).strip())
            ):
                continue

            html = await _digest_html_for_subscription(session, sub)
            if not html:
                continue
            try:
                await _notify_user_html(
                    bot,
                    user,
                    html,
                    subject="Сводка по базису — НК калькулятор топлива",
                )
            except Exception as exc:
                logger.warning("Не удалось отправить digest user_id=%s sub_id=%s: %s", user.id, sub.id, exc)
                continue

            sub.last_sent_at = datetime.now(timezone.utc)
            await session.commit()
            sent += 1
    return sent


async def _send_table_digest_once(bot: Optional[Bot], *, force: bool = False) -> int:
    """
    Таблица (СПбМТСБ + доставка). Только после нового бюллетеня (force=True).
    """
    if not force:
        return 0
    now_msk = datetime.now(_MSK)
    today_msk = now_msk.date()
    sent = 0

    from web.services.table_digest_service import build_table_digest_html

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(TableDigestSubscription).where(TableDigestSubscription.is_active.is_(True))
        )
        subs = list(result.scalars().all())

        for sub in subs:
            if sub.last_sent_at is not None:
                last = sub.last_sent_at
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if last.astimezone(_MSK).date() == today_msk:
                    continue

            user = await session.get(User, int(sub.user_id))
            if not user:
                continue

            nm = bool(getattr(sub, "notify_max", True))
            ne = bool(getattr(sub, "notify_email", True))
            if not nm and not ne:
                continue
            can_max = nm and bool(getattr(user, "max_user_id", None))
            can_em = ne and bool((user.email or "").strip())
            if not can_max and not can_em:
                continue

            html = await build_table_digest_html(session, sub)
            if not html:
                continue
            try:
                await _notify_user_table_digest(
                    bot,
                    user,
                    html,
                    "Таблица нефтепродуктов (СПбМТСБ + доставка) — НК калькулятор топлива",
                    notify_max=bool(getattr(sub, "notify_max", True)),
                    notify_email=bool(getattr(sub, "notify_email", True)),
                )
            except Exception as exc:
                logger.warning(
                    "Не удалось отправить table_digest user_id=%s sub_id=%s: %s",
                    user.id,
                    sub.id,
                    exc,
                )
                continue

            sub.last_sent_at = datetime.now(timezone.utc)
            await session.commit()
            sent += 1
    return sent


async def send_digests_after_bulletin(*, include_basis_digest: bool = True) -> dict[str, int]:
    """После импорта бюллетеня: таблицы всегда; сводки по базису — по флагу."""
    out = {"table": 0, "basis": 0}
    try:
        out["table"] = int(await _send_table_digest_once(None, force=True))
    except Exception:
        logger.exception("Ошибка рассылки table_digest после бюллетеня")
    if include_basis_digest:
        try:
            out["basis"] = int(await _send_digest_once(None, force=True))
        except Exception:
            logger.exception("Ошибка рассылки basis digest после бюллетеня")
    return out


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
        "Проверка цен для подписок запущена (каждые %s мин); "
        "сводки/таблицы — после автоимпорта бюллетеня СПбМТСБ (не по часам 14:15)",
        minutes,
    )


__all__ = ["start_price_checker", "send_digests_after_bulletin"]
