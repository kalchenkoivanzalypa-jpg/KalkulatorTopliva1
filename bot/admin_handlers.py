#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone
import csv

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, User
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select

from bot.keyboards import get_cancel_keyboard, get_main_keyboard
from db.database import Basis, Lead, Product, ProductBasisPrice, SpimexPrice, UserRequest, get_session

admin_router = Router()


def _admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_TELEGRAM_IDS", "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            continue
    return out


def _is_admin_user(user: User | None) -> bool:
    if user is None:
        return False
    admins = _admin_ids()
    if not admins:
        return False
    return int(user.id) in admins


def _is_admin_message(message: Message | None) -> bool:
    return _is_admin_user(message.from_user if message else None)


def _admin_menu_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.add(InlineKeyboardButton(text="📥 Импорт: последний бюллетень", callback_data="adm_import_latest"))
    b.add(InlineKeyboardButton(text="📥 Импорт: последние N бюллетеней", callback_data="adm_import_lastn"))
    b.add(InlineKeyboardButton(text="📂 Бюллетени: список (последние 10)", callback_data="adm_bull_list"))
    b.add(InlineKeyboardButton(text="🔎 Проверить instrument_code", callback_data="adm_check_code"))
    b.add(InlineKeyboardButton(text="🏷️ Проверить базис", callback_data="adm_check_basis"))
    b.add(InlineKeyboardButton(text="🧾 Лиды: последние 20", callback_data="adm_leads_last"))
    b.add(InlineKeyboardButton(text="📤 Лиды: выгрузить CSV", callback_data="adm_leads_csv"))
    b.add(InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_back"))
    b.adjust(1)
    return b.as_markup()


class AdminStates(StatesGroup):
    waiting_for_last_n = State()
    waiting_for_code = State()
    waiting_for_basis = State()
    waiting_for_leads_n = State()


@admin_router.message(Command("admin"))
async def admin_entry(message: Message, state: FSMContext):
    await state.clear()
    if not _is_admin_message(message):
        await message.answer("❌ Нет доступа.", reply_markup=get_main_keyboard())
        return
    await message.answer(
        "🛠️ <b>Админ</b>\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=_admin_menu_kb(),
    )


@admin_router.callback_query(F.data == "adm_back")
async def admin_back(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Главное меню.", reply_markup=get_main_keyboard())
    await cb.answer()


@admin_router.callback_query(F.data == "adm_import_latest")
async def admin_import_latest(cb: CallbackQuery):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    # отвечаем сразу, иначе Telegram "query is too old" при долгом импорте PDF
    try:
        await cb.answer()
    except Exception:
        pass
    await cb.message.answer("⏳ Импортирую последний бюллетень…")
    try:
        import import_spimex_prices_from_pdf as spx

        d = spx.default_bulletins_directory()
        pdf = spx.pick_latest_bulletin_pdf(d)
        # Для админ-импорта берём строго рыночную, чтобы не подмешивать запасные колонки.
        quotes = spx.extract_market_quotes_from_pdf(pdf, strict_market_only=True)
        pairs = [(q.instrument_code, q.market_price) for q in quotes]
        up, miss, _ = await spx.apply_prices(pairs)
        await spx.apply_spimex_history(
            bulletin_path=pdf,
            trade_dt=spx.bulletin_trade_date(pdf),
            quotes=quotes,
        )
        await cb.message.answer(
            f"✅ Готово: {pdf.name}\nОбновлено цен: {up}\nКодов не в каталоге: {miss}"
        )
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка импорта: {e}")
    # cb.answer() уже был выше


@admin_router.callback_query(F.data == "adm_bull_list")
async def admin_bulletins_list(cb: CallbackQuery):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await cb.answer()
    try:
        import import_spimex_prices_from_pdf as spx

        d = spx.default_bulletins_directory()
        pdfs = sorted(d.glob("*.pdf"))
        ranked = sorted(pdfs, key=spx.bulletin_sort_key, reverse=True)[:10]
        if not ranked:
            await cb.message.answer(
                f"📂 Каталог бюллетеней пуст: <code>{d}</code>",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(),
            )
            return
        lines = [f"📂 <b>Бюллетени (последние {len(ranked)})</b>", f"<code>{d}</code>", ""]
        for p in ranked:
            lines.append(f"• <code>{p.name}</code>")
        await cb.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=get_main_keyboard())
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка: {e}", reply_markup=get_main_keyboard())


@admin_router.callback_query(F.data == "adm_leads_last")
async def admin_leads_last(cb: CallbackQuery):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await cb.answer()
    session = await get_session()
    try:
        q = await session.execute(
            select(Lead).order_by(Lead.created_at.desc()).limit(20)
        )
        leads = q.scalars().all()
        if not leads:
            await cb.message.answer("🧾 Лидов пока нет.", reply_markup=get_main_keyboard())
            return
        lines = ["🧾 <b>Последние лиды</b>\n"]
        for l in leads:
            rid = f"#{l.request_id}" if l.request_id else "—"
            lines.append(
                f"• <b>{l.status}</b> lead#{l.id} req{rid}\n"
                f"  📧 {l.email or '—'}"
            )
        await cb.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=get_main_keyboard())
    finally:
        await session.close()


@admin_router.callback_query(F.data == "adm_leads_csv")
async def admin_leads_csv(cb: CallbackQuery):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await cb.answer()
    out_dir = Path(__file__).resolve().parent.parent / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"leads_{ts}.csv"

    session = await get_session()
    try:
        q = await session.execute(select(Lead).order_by(Lead.created_at.desc()))
        leads = q.scalars().all()
        if not leads:
            await cb.message.answer("🧾 Лидов пока нет.", reply_markup=get_main_keyboard())
            return
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "lead_id",
                    "status",
                    "source",
                    "user_id",
                    "request_id",
                    "email",
                    "phone",
                    "company",
                    "comment",
                    "created_at",
                ]
            )
            for l in leads:
                w.writerow(
                    [
                        l.id,
                        l.status,
                        l.source,
                        l.user_id,
                        l.request_id,
                        l.email,
                        l.phone,
                        l.company,
                        l.comment,
                        l.created_at,
                    ]
                )
        await cb.message.answer(
            "📤 CSV выгрузка готова.\n"
            f"Файл на диске: <code>{out_path}</code>",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(),
        )
    finally:
        await session.close()


@admin_router.callback_query(F.data == "adm_import_lastn")
async def admin_import_lastn_prompt(cb: CallbackQuery, state: FSMContext):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await state.clear()
    await cb.message.answer(
        "Введите N (сколько последних бюллетеней импортировать):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AdminStates.waiting_for_last_n)
    await cb.answer()


@admin_router.message(AdminStates.waiting_for_last_n, F.text)
async def admin_import_lastn_run(message: Message, state: FSMContext):
    if not _is_admin_message(message):
        await state.clear()
        await message.answer("❌ Нет доступа.", reply_markup=get_main_keyboard())
        return
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    try:
        n = int(t)
        if n <= 0 or n > 30:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите целое N от 1 до 30:")
        return

    await message.answer(f"⏳ Импортирую последние {n} бюллетеней…")
    try:
        import import_spimex_prices_from_pdf as spx

        d = spx.default_bulletins_directory()
        newest_first = spx.pick_latest_n_bulletin_pdfs(d, n)
        chrono = list(reversed(newest_first))
        updated_total = 0
        for pdf in chrono:
            quotes = spx.extract_market_quotes_from_pdf(pdf, strict_market_only=True)
            pairs = [(q.instrument_code, q.market_price) for q in quotes]
            up, _miss, _ = await spx.apply_prices(pairs)
            await spx.apply_spimex_history(
                bulletin_path=pdf,
                trade_dt=spx.bulletin_trade_date(pdf),
                quotes=quotes,
            )
            updated_total += int(up)
        await message.answer(f"✅ Готово. Всего обновлений цен: {updated_total}")
    except Exception as e:
        await message.answer(f"❌ Ошибка импорта: {e}")
    await state.clear()


@admin_router.callback_query(F.data == "adm_check_code")
async def admin_check_code_prompt(cb: CallbackQuery, state: FSMContext):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await state.clear()
    await cb.message.answer(
        "Введите <code>instrument_code</code> (например <code>DSC5ANK065F</code>):",
        parse_mode="HTML",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AdminStates.waiting_for_code)
    await cb.answer()


@admin_router.message(AdminStates.waiting_for_code, F.text)
async def admin_check_code_run(message: Message, state: FSMContext):
    if not _is_admin_message(message):
        await state.clear()
        await message.answer("❌ Нет доступа.", reply_markup=get_main_keyboard())
        return
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return
    code = t.upper().strip()

    session = await get_session()
    try:
        pbp = (
            await session.execute(
                select(ProductBasisPrice, Product, Basis)
                .join(Product, Product.id == ProductBasisPrice.product_id)
                .join(Basis, Basis.id == ProductBasisPrice.basis_id)
                .where(ProductBasisPrice.instrument_code == code)
                .limit(1)
            )
        ).first()
        if not pbp:
            await message.answer("❌ Код не найден в каталоге product_basis_prices.")
            return
        row, pr, bs = pbp

        qh = await session.execute(
            select(SpimexPrice)
            .where(SpimexPrice.exchange_product_id == code)
            .order_by(SpimexPrice.date.desc())
            .limit(10)
        )
        hist = qh.scalars().all()
        hist_lines = []
        for h in hist:
            d = h.date.date().isoformat() if h.date else "—"
            p = f"{float(h.price):,.0f}".replace(",", " ") if h.price is not None else "—"
            v = f"{float(h.volume):,.0f}".replace(",", " ") if h.volume is not None else "—"
            hist_lines.append(f"{d}: {p} ₽/т, vol {v} т")

        text = (
            f"🔎 <b>{code}</b>\n"
            f"🛢️ {pr.name}\n"
            f"📍 {bs.name} ({bs.transport_type})\n"
            f"💰 current_price: <b>{float(row.current_price):,.0f}</b> ₽/т\n"
            f"🕒 updated: {row.last_updated}\n\n"
            f"<b>История (до 10):</b>\n" + ("\n".join(hist_lines) if hist_lines else "—")
        ).replace(",", " ")
        await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    finally:
        await session.close()
        await state.clear()


@admin_router.callback_query(F.data == "adm_check_basis")
async def admin_check_basis_prompt(cb: CallbackQuery, state: FSMContext):
    if not _is_admin_user(cb.from_user):
        await cb.answer()
        return
    await state.clear()
    await cb.message.answer(
        "Введите название базиса (можно частично):",
        reply_markup=get_cancel_keyboard(),
    )
    await state.set_state(AdminStates.waiting_for_basis)
    await cb.answer()


@admin_router.message(AdminStates.waiting_for_basis, F.text)
async def admin_check_basis_run(message: Message, state: FSMContext):
    if not _is_admin_message(message):
        await state.clear()
        await message.answer("❌ Нет доступа.", reply_markup=get_main_keyboard())
        return
    t = (message.text or "").strip()
    if "отмена" in t.lower():
        await state.clear()
        await message.answer("Отменено.", reply_markup=get_main_keyboard())
        return

    session = await get_session()
    try:
        q = await session.execute(
            select(Basis).where(Basis.is_active.is_(True), Basis.name.ilike(f"%{t}%")).limit(1)
        )
        basis = q.scalar_one_or_none()
        if not basis:
            await message.answer("❌ Базис не найден.")
            return

        qp = await session.execute(
            select(ProductBasisPrice, Product)
            .join(Product, Product.id == ProductBasisPrice.product_id)
            .where(ProductBasisPrice.basis_id == basis.id)
            .where(ProductBasisPrice.is_active.is_(True))
            .order_by(Product.name)
        )
        rows = qp.all()
        lines = []
        for pbp, pr in rows[:40]:
            p = f"{float(pbp.current_price):,.0f}".replace(",", " ") if pbp.current_price else "—"
            lines.append(f"{pr.name}: {p} ₽/т ({pbp.instrument_code})")

        text = (
            f"🏷️ <b>{basis.name}</b>\n"
            f"🚚 transport: {basis.transport_type}\n\n"
            f"<b>Цены (до 40):</b>\n" + ("\n".join(lines) if lines else "—")
        )
        await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    finally:
        await session.close()
        await state.clear()

