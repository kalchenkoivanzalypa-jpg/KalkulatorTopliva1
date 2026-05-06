# -*- coding: utf-8 -*-
"""
Свежесть рыночной цены для расчёта: не показывать базис, если котировка старше N торговых дней.
Торговый день = пн–пт (биржевые выходные и праздники не учитываем — только выходные суб/вс).
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Sequence

from sqlalchemy import func, select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# Сколько торговых сессий (бюллетеней) после последней торговой даты допускаем (включительно).
# Торговая сессия = уникальная дата в spimex_prices (т.е. реально импортированный бюллетень/день торгов).
CALC_MAX_STALE_SESSIONS = int(os.getenv("CALC_MAX_STALE_SESSIONS", "5"))

# Фолбек (если в БД нет истории spimex_prices): сколько торговых дней (пн–пт) допускаем.
CALC_MAX_STALE_TRADING_DAYS = int(os.getenv("CALC_MAX_STALE_TRADING_DAYS", "15"))


def trading_days_elapsed_after(last_trade: date, today: date) -> int:
    """
    Число торговых дней (пн–пт) строго после даты last_trade до today включительно.
    Если last_trade == today → 0.
    """
    if today <= last_trade:
        return 0
    n = 0
    d = last_trade + timedelta(days=1)
    while d <= today:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


def today_for_exchange() -> date:
    """Календарная дата «сегодня» (МСК, если доступен zoneinfo)."""
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        return datetime.now().date()


def _as_date(v: object) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # SQLite func.date() часто возвращает строку 'YYYY-MM-DD'
        try:
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except Exception:
            return None
    return None


async def max_spimex_trade_date_by_codes(session: "AsyncSession", codes: set[str]) -> dict[str, date]:
    """По каждому instrument_code — максимальная торговая дата из spimex_prices."""
    if not codes:
        return {}
    from db.database import SpimexPrice

    norm = {c.strip().upper() for c in codes if c and str(c).strip()}
    if not norm:
        return {}

    uc = func.upper(SpimexPrice.exchange_product_id)
    result = await session.execute(
        select(uc.label("cid"), func.max(SpimexPrice.date))
        .where(uc.in_(list(norm)))
        .group_by(uc)
    )
    out: dict[str, date] = {}
    for cid, md in result.all():
        if not cid:
            continue
        d = _as_date(md)
        if d is not None:
            out[str(cid).strip().upper()] = d
    return out


async def load_recent_spimex_trade_dates(session: "AsyncSession", *, limit: int = 120) -> list[date]:
    """
    Последние уникальные торговые даты (desc) из spimex_prices.
    Используем как «календарь сессий»: сколько бюллетеней прошло с момента последней цены по коду.
    """
    from db.database import SpimexPrice

    if limit < 1:
        limit = 1
    q = await session.execute(
        select(func.date(SpimexPrice.date))
        .where(SpimexPrice.date.isnot(None))
        .group_by(func.date(SpimexPrice.date))
        .order_by(func.date(SpimexPrice.date).desc())
        .limit(int(limit))
    )
    out: list[date] = []
    for (d_raw,) in q.all():
        d = _as_date(d_raw)
        if d is not None:
            out.append(d)
    return out


async def max_spimex_market_date_by_codes(session: "AsyncSession", codes: set[str]) -> dict[str, date]:
    """
    По каждому instrument_code — максимальная торговая дата, где была извлечена именно колонка «Рыночная».

    Это защищает от кейса, когда парсер не нашёл «Рыночную» и подставил fallback (лучшее предложение и т.п.):
    такие обновления не должны «омолаживать» цену для расчёта/аналитики.
    """
    if not codes:
        return {}
    from db.database import SpimexPrice

    norm = {c.strip().upper() for c in codes if c and str(c).strip()}
    if not norm:
        return {}

    uc = func.upper(SpimexPrice.exchange_product_id)
    result = await session.execute(
        select(uc.label("cid"), func.max(SpimexPrice.date))
        .where(uc.in_(list(norm)))
        .where(SpimexPrice.price_market.isnot(None))
        .group_by(uc)
    )
    out: dict[str, date] = {}
    for cid, md in result.all():
        if not cid:
            continue
        d = _as_date(md)
        if d is not None:
            out[str(cid).strip().upper()] = d
    return out


async def max_spimex_real_trade_date_by_codes(session: "AsyncSession", codes: set[str]) -> dict[str, date]:
    """
    По каждому instrument_code — максимальная торговая дата, когда есть признаки реальной торговой строки,
    а не “вырванная” fallback-цена.

    Критерии (достаточно одного):
    - есть price_market (рыночная)
    - или есть contracts / volume / volume_rub (в бюллетене есть сделки/объёмы)
    """
    if not codes:
        return {}
    from db.database import SpimexPrice

    norm = {c.strip().upper() for c in codes if c and str(c).strip()}
    if not norm:
        return {}

    uc = func.upper(SpimexPrice.exchange_product_id)
    result = await session.execute(
        select(uc.label("cid"), func.max(SpimexPrice.date))
        .where(uc.in_(list(norm)))
        .where(
            (SpimexPrice.price_market.isnot(None))
            | (SpimexPrice.contracts.isnot(None))
            | (SpimexPrice.volume.isnot(None))
            | (SpimexPrice.volume_rub.isnot(None))
        )
        .group_by(uc)
    )
    out: dict[str, date] = {}
    for cid, md in result.all():
        if not cid:
            continue
        d = _as_date(md)
        if d is not None:
            out[str(cid).strip().upper()] = d
    return out


def last_known_trade_date_for_basis_price(
    pbp: object,
    spimex_max_by_code: dict[str, date],
) -> date | None:
    """
    Дата «последней торговой сессии» для свежести.

    Важно: если у строки есть instrument_code и по нему есть история в spimex_prices,
    свежесть должна опираться на торговую дату из spimex, а не на pbp.last_updated.
    Иначе парсер может «обновить» pbp по лучшему предложению/спросу в PDF, хотя торгов по коду не было.

    Фолбек: если в spimex по коду нет данных — используем pbp.last_updated как слабый сигнал.
    """
    code = str(getattr(pbp, "instrument_code", None) or "").strip().upper()
    if code and code in spimex_max_by_code:
        d = spimex_max_by_code.get(code)
        if d is not None:
            return d
    lu = getattr(pbp, "last_updated", None)
    d_lu = _as_date(lu)
    if d_lu is not None:
        return d_lu
    return None


def is_basis_price_fresh_for_calc(
    pbp: object,
    spimex_max_by_code: dict[str, date],
    *,
    today: date | None = None,
    max_trading_days: int | None = None,
    recent_trade_dates: list[date] | None = None,
    max_sessions: int | None = None,
    spimex_any_by_code: dict[str, date] | None = None,
) -> bool:
    """
    True, если цена считается «свежей».

    При наличии истории spimex_prices свежесть считаем по числу прошедших торговых сессий (бюллетеней).
    Иначе — фолбек по торговым дням (пн–пт) относительно today.
    """
    lim_sessions = max_sessions if max_sessions is not None else CALC_MAX_STALE_SESSIONS
    lim_days = max_trading_days if max_trading_days is not None else CALC_MAX_STALE_TRADING_DAYS
    t = today if today is not None else today_for_exchange()

    # Если spimex_prices живой и по коду в истории есть строки (any),
    # но по “реальным торгам” (spimex_max_by_code) ничего нет — считаем устаревшей.
    code = str(getattr(pbp, "instrument_code", None) or "").strip().upper()
    if (
        code
        and recent_trade_dates
        and spimex_any_by_code
        and code in spimex_any_by_code
        and code not in spimex_max_by_code
    ):
        return False

    last = last_known_trade_date_for_basis_price(pbp, spimex_max_by_code)
    if last is None:
        try:
            if float(getattr(pbp, "current_price", None) or 0) > 0:
                return True
        except Exception:
            pass
        return False

    # Если есть «календарь» торговых дат — считаем по количеству сессий после last.
    if recent_trade_dates:
        try:
            after = sum(1 for d in recent_trade_dates if d > last)
            return after <= int(lim_sessions)
        except Exception:
            pass

    # Фолбек: торговые дни (пн–пт)
    elapsed = trading_days_elapsed_after(last, t)
    return elapsed <= int(lim_days)


def select_best_pbp_among_candidates(
    pbps: Sequence[object],
    *,
    spimex_any_dates: dict[str, date],
    spimex_last_dates: dict[str, date],
    recent_trade_dates: list[date],
    today_d: date | None = None,
) -> object | None:
    """
    Несколько instrument_code на один базис×продукт: оставить только прошедших свежесть;
    затем выбрать по последней дате реальной торговли (spimex_last_dates), затем last_updated и id.

    Совпадает с логикой find_nearest_basises после группировки по basis_id.
    """
    tday = today_d if today_d is not None else today_for_exchange()
    fresh_only = [
        p
        for p in pbps
        if is_basis_price_fresh_for_calc(
            p,
            spimex_last_dates,
            today=tday,
            recent_trade_dates=recent_trade_dates,
            max_sessions=CALC_MAX_STALE_SESSIONS,
            spimex_any_by_code=spimex_any_dates,
        )
    ]
    if not fresh_only:
        return None

    def _rank(pbp: object) -> tuple[int, datetime, int]:
        code = str(getattr(pbp, "instrument_code", None) or "").strip().upper()
        td = spimex_last_dates.get(code) if code else None
        td_ord = td.toordinal() if isinstance(td, date) else 0
        lu = getattr(pbp, "last_updated", None) or datetime.min
        pid = int(getattr(pbp, "id", 0) or 0)
        return (td_ord, lu, pid)

    return max(fresh_only, key=_rank)


async def pick_best_product_basis_price_row(
    session: "AsyncSession",
    *,
    basis_id: int,
    product_ids: list[int],
    exclude_ai100_price_stub: bool = False,
) -> object | None:
    """
    Активная строка цены для базиса и набора product_id (алиасы канонического топлива).
    """
    if not product_ids:
        return None

    uid = sorted({int(x) for x in product_ids})
    from db.database import ProductBasisPrice

    q = (
        select(ProductBasisPrice)
        .where(ProductBasisPrice.basis_id == int(basis_id))
        .where(ProductBasisPrice.product_id.in_(uid))
        .where(ProductBasisPrice.is_active.is_(True))
        .where(ProductBasisPrice.current_price > 0)
    )
    if exclude_ai100_price_stub:
        q = q.where(ProductBasisPrice.current_price != 10000)

    rows = list((await session.execute(q)).scalars().all())
    if not rows:
        return None

    codes = {
        str(getattr(r, "instrument_code", None) or "").strip().upper()
        for r in rows
        if getattr(r, "instrument_code", None)
    }
    sp_any = await max_spimex_trade_date_by_codes(session, codes)
    sp_last = await max_spimex_real_trade_date_by_codes(session, codes)
    recent = await load_recent_spimex_trade_dates(session, limit=180)
    tday = today_for_exchange()

    return select_best_pbp_among_candidates(
        rows,
        spimex_any_dates=sp_any,
        spimex_last_dates=sp_last,
        recent_trade_dates=recent,
        today_d=tday,
    )
