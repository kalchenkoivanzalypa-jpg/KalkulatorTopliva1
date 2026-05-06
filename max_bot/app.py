from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import PlainTextResponse

from max_bot.max_api import MaxApiError, max_api_from_env
from db.database import (
    AnomalyAlert,
    Basis,
    BasisDigestSubscription,
    CityDestination,
    Lead,
    PriceAlert,
    Product,
    ProductBasisPrice,
    TableDigestSubscription,
    User,
    UserRequest,
    get_session,
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from utils import canonical_fuel_display_name, normalize_city_name_key, get_coordinates_from_city
from utils.rail_logistics import (
    find_rail_station_for_destination,
    is_sakhalin_destination,
    sakhalin_ferry_surcharge_per_ton,
)
from bot.handlers import find_nearest_basises
from web.products_util import list_products_for_calc
from web.services.analytics_service import compute_compare_three, compute_trend, search_basises
from web.auth_otp import create_otp, verify_otp
from web.email_util import SMTPNotConfiguredError, send_smtp_email
from web.users_repo import link_max_user_to_email, synthetic_telegram_id_for_max

logger = logging.getLogger(__name__)

class _SlidingWindowLimiter:
    def __init__(self) -> None:
        self._hits: dict[str, list[float]] = {}

    def allow(self, key: str, *, limit: int, window_sec: int) -> bool:
        now = time.time()
        xs = self._hits.get(key)
        if xs is None:
            self._hits[key] = [now]
            return True
        cutoff = now - float(window_sec)
        xs[:] = [t for t in xs if t >= cutoff]
        if len(xs) >= int(limit):
            return False
        xs.append(now)
        return True


class WebhookRateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI) -> None:
        super().__init__(app)
        self.lim = _SlidingWindowLimiter()

    async def dispatch(self, request: Request, call_next):
        if request.method.upper() == "POST" and request.url.path == "/webhook":
            ip = (request.headers.get("x-forwarded-for") or request.client.host or "unknown").split(",")[0].strip()
            # MAX webhook: allow bursts but cap sustained spam
            if not self.lim.allow(f"wh_ip:{ip}", limit=120, window_sec=60):
                return PlainTextResponse("Too many requests", status_code=429)
        return await call_next(request)


async def _max_user_email(sender_id: int) -> str | None:
    """Email, привязанный к MAX user id (если есть)."""
    session = await get_session()
    try:
        u = (await session.execute(select(User).where(User.max_user_id == int(sender_id)).limit(1))).scalar_one_or_none()
        if u is None:
            return None
        em = (getattr(u, "email", None) or "").strip().lower()
        return em or None
    finally:
        await session.close()


async def _require_linked_email(api, *, sender_id: int, after_login: str) -> bool:
    """
    Возвращает True, если email уже привязан.
    Иначе запускает OTP flow (как на сайте) и возвращает False.
    """
    em = await _max_user_email(int(sender_id))
    if em:
        return True
    _state_set(int(sender_id), {"stage": "max_login_wait_email", "after_login": str(after_login or "")})
    await api.send_message(
        user_id=int(sender_id),
        text=(
            "<b>Нужно привязать email</b>\n\n"
            "Подписки и кабинет работают через один аккаунт по email.\n"
            "Введите ваш email — пришлю 6‑значный код (как при входе на сайт)."
        ),
        fmt="html",
    )
    return False


def _inline_keyboard(button_rows: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [
        {
            "type": "inline_keyboard",
            "payload": {"buttons": button_rows},
        }
    ]


def _menu_attachments() -> list[dict[str, Any]]:
    return _inline_keyboard(
        [
            [
                {"type": "callback", "text": "🧮 Расчёт", "payload": "menu:calc"},
                {"type": "callback", "text": "📊 Аналитика", "payload": "menu:analytics"},
            ],
            [
                {"type": "callback", "text": "🔔 Подписки", "payload": "menu:subs"},
                {"type": "callback", "text": "ℹ️ О сервисе", "payload": "menu:about"},
            ],
            [
                {"type": "callback", "text": "📧 Войти по email", "payload": "menu:login"},
                {"type": "callback", "text": "🚪 Выйти", "payload": "menu:logout"},
            ],
            [
                {"type": "link", "text": "🌐 Открыть сайт", "url": "https://calc.nk-vsnp.ru/"},
            ],
        ]
    )


def _get(d: dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _extract_sender_user_id(update: dict[str, Any]) -> Optional[int]:
    """
    MAX Update.message_created -> Message.sender.user_id, text is Message.body.text.
    https://dev.max.ru/docs-api/objects/Update
    https://dev.max.ru/docs-api/objects/Message
    """
    for p in (
        "message.sender.user_id",
        "message.sender.id",
        # fallback for unknown shapes
        "message.user.user_id",
        "message.user_id",
        "sender.user_id",
        "user.user_id",
        "user_id",
    ):
        v = _get(update, p)
        try:
            if v is not None:
                return int(v)
        except Exception:
            continue
    return None


def _extract_callback_user_id(update: dict[str, Any]) -> Optional[int]:
    """
    For update_type=message_callback, the clicking user is in callback.user.user_id.
    """
    for p in ("callback.user.user_id", "callback.user_id", "callback.user.id"):
        v = _get(update, p)
        try:
            if v is not None:
                return int(v)
        except Exception:
            continue
    return None


def _extract_recipient_chat_id(update: dict[str, Any]) -> Optional[int]:
    for p in ("message.recipient.chat_id", "recipient.chat_id", "chat.chat_id", "chat_id"):
        v = _get(update, p)
        try:
            if v is not None:
                return int(v)
        except Exception:
            continue
    return None


def _extract_text(update: dict[str, Any]) -> str:
    for p in ("message.body.text", "message.text", "text"):
        v = _get(update, p, "")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


app = FastAPI(title="Fuel MAX Bot (webhook)")
app.add_middleware(WebhookRateLimitMiddleware)

# ---- In-memory conversation state (MVP) ----
# Keyed by MAX sender user_id. TTL avoids stale stuck flows.
_STATE_TTL_SEC = 2 * 60 * 60
_state: dict[int, dict[str, Any]] = {}
_last_result: dict[int, dict[str, Any]] = {}


def _now() -> float:
    return time.time()


def _state_get(uid: int) -> dict[str, Any]:
    st = _state.get(uid) or {}
    ts = float(st.get("_ts") or 0.0)
    if ts and (_now() - ts) > _STATE_TTL_SEC:
        _state.pop(uid, None)
        return {}
    return st


def _state_set(uid: int, st: dict[str, Any]) -> None:
    st["_ts"] = _now()
    _state[uid] = st


def _state_clear(uid: int) -> None:
    _state.pop(uid, None)
    _last_result.pop(uid, None)


def _why_keyboard() -> list[dict[str, Any]]:
    return _inline_keyboard(
        [
            [
                {"type": "callback", "text": "🧾 Почему так", "payload": "calc:why"},
                {"type": "callback", "text": "🔄 Новый расчёт", "payload": "calc:restart"},
            ],
            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
        ]
    )


def _result_keyboard() -> list[dict[str, Any]]:
    return _inline_keyboard(
        [
            [
                {"type": "callback", "text": "🧾 Почему так", "payload": "calc:why"},
                {"type": "callback", "text": "📩 Заявка на КП", "payload": "calc:kp"},
                {"type": "callback", "text": "🔔 Цена ↓", "payload": "subs:price:new"},
            ],
            [
                {"type": "callback", "text": "⚠️ Аномалия", "payload": "subs:anom:new"},
                {"type": "callback", "text": "🔄 Новый расчёт", "payload": "calc:restart"},
            ],
            [{"type": "callback", "text": "📋 Мои подписки", "payload": "subs:list"}],
            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
        ]
    )


def _trend_result_keyboard(basis_id: int, product_id: int, instrument_code: str) -> list[dict[str, Any]]:
    return _inline_keyboard(
        [
            [
                {"type": "callback", "text": "🔔 Цена ↓ (базис)", "payload": f"a:subs:price:{int(product_id)}:{int(basis_id)}"},
                {"type": "callback", "text": "⚠️ Аномалия", "payload": f"a:subs:anom:{str(instrument_code)}"},
            ],
            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
        ]
    )


def _compare_result_keyboard(basis_ids: list[int], instrument_codes: list[str]) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = []
    for i, bid in enumerate(basis_ids, start=1):
        rows.append(
            [
                {"type": "callback", "text": f"🔔 Цена ↓ #{i}", "payload": f"a:subs:price_basis:{int(bid)}"},
                {"type": "callback", "text": f"⚠️ Аномалия #{i}", "payload": f"a:subs:anom_idx:{i}"},
            ]
        )
    rows.append([{"type": "callback", "text": "📊 Аналитика", "payload": "menu:analytics"}])
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


def _subs_menu_keyboard() -> list[dict[str, Any]]:
    base = (os.getenv("WEB_PUBLIC_URL") or "https://calc.nk-vsnp.ru").rstrip("/")
    return _inline_keyboard(
        [
            [{"type": "callback", "text": "📉 Снижение цены", "payload": "subs:price:start"}],
            [{"type": "callback", "text": "⚠️ Аномалия", "payload": "subs:anom:start"}],
            [{"type": "callback", "text": "📊 Ежедневная сводка", "payload": "subs:digest:start"}],
            [
                {
                    "type": "link",
                    "text": "📋 Таблица (настройка на сайте)",
                    "url": f"{base}/login?next=/cabinet/subscriptions#table-digest",
                }
            ],
            [{"type": "callback", "text": "📋 Мои подписки", "payload": "subs:list"}],
            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
        ]
    )


def _subs_list_keyboard(
    price_alerts: list[PriceAlert],
    anom_alerts: list[AnomalyAlert],
    digest_alerts: list[BasisDigestSubscription],
    table_subs: list[Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = []
    for a in price_alerts:
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"❌ Цена #{int(a.id)}",
                    "payload": f"subs:price:off:{int(a.id)}",
                }
            ]
        )
    for a in anom_alerts:
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"❌ Аномалия #{int(a.id)}",
                    "payload": f"subs:anom:off:{int(a.id)}",
                }
            ]
        )
    for a in digest_alerts:
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"❌ Сводка #{int(a.id)}",
                    "payload": f"subs:digest:off:{int(a.id)}",
                }
            ]
        )
    for a in table_subs or []:
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"❌ Таблица #{int(a.id)}",
                    "payload": f"subs:table:off:{int(a.id)}",
                }
            ]
        )
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


def _digest_scope_keyboard(basis_id: int, products: list[tuple[int, str]]) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = [
        [{"type": "callback", "text": "📦 Все на базисе", "payload": f"digest:all:{int(basis_id)}"}]
    ]
    for pid, name in products[:20]:
        short = (name[:38] + "…") if len(name) > 38 else name
        rows.append(
            [{"type": "callback", "text": short, "payload": f"digest:prod:{int(basis_id)}:{int(pid)}"}]
        )
    if len(products) > 20:
        rows.append([{"type": "link", "text": "Полный список в кабинете на сайте", "url": "https://calc.nk-vsnp.ru/login?next=/cabinet"}])
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


async def _get_or_create_max_user(max_user_id: int, *, first_name: str = "", last_name: str = "") -> User:
    session = await get_session()
    try:
        q = await session.execute(select(User).where(User.max_user_id == int(max_user_id)).limit(1))
        u = q.scalar_one_or_none()
        if u is None:
            u = User(
                telegram_id=synthetic_telegram_id_for_max(max_user_id),
                max_user_id=int(max_user_id),
                first_name=(first_name or None),
                last_name=(last_name or None),
                username=None,
                email=None,
                phone=None,
                is_active=True,
            )
            session.add(u)
            await session.commit()
            await session.refresh(u)
        return u
    finally:
        await session.close()


async def _create_price_alert(user_id: int, *, product_id: int, basis_id: int, target_price: float) -> None:
    session = await get_session()
    try:
        al = PriceAlert(
            user_id=int(user_id),
            product_id=int(product_id),
            basis_id=int(basis_id),
            target_price=float(target_price),
            is_active=True,
        )
        session.add(al)
        await session.commit()
    finally:
        await session.close()


async def _create_anomaly_alert(
    user_id: int,
    *,
    instrument_code: str,
    threshold_pct: float,
    direction: str = "any",
) -> None:
    session = await get_session()
    try:
        dir_s = str(direction or "any").strip().lower()
        if dir_s not in ("any", "up", "down"):
            dir_s = "any"
        al = AnomalyAlert(
            user_id=int(user_id),
            instrument_code=str(instrument_code).strip().upper(),
            threshold_pct=float(threshold_pct),
            direction=dir_s,
            is_active=True,
        )
        session.add(al)
        await session.commit()
    finally:
        await session.close()


async def _create_digest_subscription(
    user_id: int,
    *,
    basis_id: int,
    all_products: bool,
    product_id: Optional[int],
    delivery_mode: str = "prices_only",
    destination_id: Optional[int] = None,
    destination_name: Optional[str] = None,
    destination_key: Optional[str] = None,
) -> None:
    session = await get_session()
    try:
        al = BasisDigestSubscription(
            user_id=int(user_id),
            basis_id=int(basis_id),
            all_products=bool(all_products),
            product_id=None if all_products else int(product_id or 0),
            delivery_mode=str(delivery_mode or "prices_only"),
            destination_id=(int(destination_id) if destination_id is not None else None),
            destination_name=(str(destination_name) if destination_name else None),
            destination_key=(str(destination_key) if destination_key else None),
            is_active=True,
        )
        session.add(al)
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise
    finally:
        await session.close()


async def _list_products_on_basis(basis_id: int) -> list[tuple[int, str]]:
    session = await get_session()
    try:
        q = await session.execute(
            select(Product.id, Product.name)
            .join(ProductBasisPrice, ProductBasisPrice.product_id == Product.id)
            .where(ProductBasisPrice.basis_id == int(basis_id))
            .where(ProductBasisPrice.is_active.is_(True))
            .where(Product.is_active.is_(True))
            .order_by(Product.name)
        )
        seen: set[int] = set()
        out: list[tuple[int, str]] = []
        for pid, name in q.all():
            i = int(pid)
            if i in seen:
                continue
            seen.add(i)
            out.append((i, str(name)))
        return out
    finally:
        await session.close()


def _subs_basis_search_text(kind: str) -> str:
    title = "Снижение цены" if kind == "price" else "Аномалия"
    return f"<b>{title}</b>\n\nВведите часть названия базиса:"


async def _list_subscriptions_for_user(user_id: int) -> dict[str, Any]:
    session = await get_session()
    try:
        q1 = await session.execute(
            select(PriceAlert).where(PriceAlert.user_id == int(user_id)).where(PriceAlert.is_active.is_(True))
        )
        q2 = await session.execute(
            select(AnomalyAlert).where(AnomalyAlert.user_id == int(user_id)).where(AnomalyAlert.is_active.is_(True))
        )
        q3 = await session.execute(
            select(BasisDigestSubscription)
            .where(BasisDigestSubscription.user_id == int(user_id))
            .where(BasisDigestSubscription.is_active.is_(True))
        )
        q4 = await session.execute(
            select(TableDigestSubscription)
            .where(TableDigestSubscription.user_id == int(user_id))
            .where(TableDigestSubscription.is_active.is_(True))
        )
        return {
            "price": list(q1.scalars().all()),
            "anom": list(q2.scalars().all()),
            "digest": list(q3.scalars().all()),
            "table": list(q4.scalars().all()),
        }
    finally:
        await session.close()


def _prod_keyboard(products: list[Any]) -> list[dict[str, Any]]:
    return _prod_keyboard_with_prefix(products, "calc:prod:")


def _prod_keyboard_with_prefix(products: list[Any], prefix: str) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    for p in products:
        if isinstance(p, dict):
            pid = int(p["id"])
            label = str(p.get("name") or "").strip() or str(pid)
        else:
            pid = int(getattr(p, "id", 0))
            label = canonical_fuel_display_name(getattr(p, "name", "") or "")
        cur.append(
            {
                "type": "callback",
                "text": label,
                "payload": f"{prefix}{pid}",
            }
        )
        if len(cur) >= 2:
            rows.append(cur)
            cur = []
    if cur:
        rows.append(cur)
    rows.append([{"type": "callback", "text": "⬅️ В меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


def _basis_keyboard(options: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = []
    for i, opt in enumerate(options, start=1):
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"{i}. {opt['label']}",
                    "payload": f"calc:pick:{i}",
                }
            ]
        )
    rows.append([{"type": "callback", "text": "⬅️ Сменить топливо", "payload": "calc:restart"}])
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


def _menu_text() -> str:
    return "<b>НК · калькулятор топлива</b>\n\nВыберите действие:"


def _calc_start_text() -> str:
    return "<b>Расчёт</b>\n\nВыберите топливо:"


def _analytics_menu_text() -> str:
    return "<b>Аналитика</b>\n\nВыберите раздел:"

def _about_text() -> str:
    return (
        "<b>О сервисе</b>\n\n"
        "Калькулятор считает цену топлива с доставкой (авто/ж/д), а аналитика помогает "
        "оценивать базисы по биржевой истории.\n\n"
        "Подписки:\n"
        "• Ежедневная сводка — раз в день в 14:15 (МСК) актуальные биржевые цены на базисе "
        "(одно топливо или все доступные на базисе)\n"
        "• Цена ↓ — когда цена на базисе станет ≤ заданной\n"
        "• Аномалия — при резком изменении цены по коду инструмента\n\n"
        "Полная версия на сайте: https://calc.nk-vsnp.ru/about"
    )


def _analytics_menu_kb() -> list[dict[str, Any]]:
    base = (os.getenv("WEB_PUBLIC_URL") or "https://calc.nk-vsnp.ru").rstrip("/")
    return _inline_keyboard(
        [
            [
                {"type": "link", "text": "🔥 Рейтинг", "url": "https://calc.nk-vsnp.ru/analytics/rating"},
                {"type": "callback", "text": "📈 Тренд", "payload": "a:trend"},
            ],
            [
                {"type": "callback", "text": "🟦 Сравнить 3 базиса", "payload": "a:compare"},
                {"type": "link", "text": "5 базисов × 5 направлений", "url": f"{base}/analytics/matrix"},
                {"type": "link", "text": "🌐 Открыть аналитику на сайте", "url": "https://calc.nk-vsnp.ru/analytics"},
            ],
            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
        ]
    )


def _trend_basis_query_text() -> str:
    return "<b>Тренд</b>\n\nВведите часть названия базиса (например «Дземги» или «Комбинатская»):"


def _trend_products_text(basis_name: str) -> str:
    return f"<b>Тренд</b>\n\nБазис: <b>{basis_name}</b>\nВыберите топливо:"


def _compare_start_text() -> str:
    return "<b>Сравнить 3 базиса</b>\n\nВыберите топливо:"


def _compare_search_text(selected: list[int]) -> str:
    return (
        "<b>Сравнить 3 базиса</b>\n\n"
        f"Выбрано базисов: <b>{len(selected)}/3</b>\n"
        "Введите часть названия базиса для поиска:"
    )


def _compare_after_pick_text(selected_count: int) -> str:
    if selected_count >= 3:
        return (
            "<b>Сравнить 3 базиса</b>\n\n"
            "✅ Выбрано <b>3/3</b>.\n"
            "Теперь нажмите «Далее: назначение»."
        )
    return (
        "<b>Сравнить 3 базиса</b>\n\n"
        f"✅ Выбрано <b>{int(selected_count)}/3</b>.\n"
        "Введите следующий базис для поиска."
    )


def _compare_after_pick_kb(selected_count: int, *, can_show_list: bool) -> list[dict[str, Any]]:
    rows: list[list[dict[str, Any]]] = []
    if selected_count >= 3:
        rows.append([{"type": "callback", "text": "➡️ Далее: назначение", "payload": "a:compare:dest"}])
    if can_show_list:
        rows.append([{"type": "callback", "text": "📋 Показать список", "payload": "a:compare:showlist"}])
    rows.append(
        [
            {"type": "callback", "text": "🔄 Новый поиск", "payload": "a:compare:search"},
            {"type": "callback", "text": "🧹 Сбросить выбор", "payload": "a:compare:clear"},
        ]
    )
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    return _inline_keyboard(rows)


def _strip_html(s: str) -> str:
    # минимальный strip для наших html_block из analytics_service
    if not s:
        return ""
    out = s.replace("<br/>", "\n").replace("<br>", "\n")
    for tag in ("<b>", "</b>", "<p>", "</p>"):
        out = out.replace(tag, "")
    return "\n".join([ln.strip() for ln in out.splitlines() if ln.strip()])


def _format_num(x: float) -> str:
    return f"{float(x):,.0f}".replace(",", " ")


async def _list_active_products() -> list[Product]:
    session = await get_session()
    try:
        q = await session.execute(select(Product).where(Product.is_active.is_(True)).order_by(Product.name))
        return list(q.scalars().all())
    finally:
        await session.close()


async def _list_compare_products() -> list[dict[str, object]]:
    session = await get_session()
    try:
        return await list_products_for_calc(session)
    finally:
        await session.close()


async def _resolve_destination(text: str) -> Optional[dict[str, Any]]:
    session = await get_session()
    try:
        coords = await get_coordinates_from_city(text, session)
        if coords:
            dest_key = normalize_city_name_key(text)
            # even if coords came from city cache, try to find station for sakhalin detection / ESR
            rs = await find_rail_station_for_destination(session, text, dest_key)
            return {
                "lat": float(coords[0]),
                "lon": float(coords[1]),
                "dest_key": dest_key,
                "station": rs,
                "is_sakhalin": bool(is_sakhalin_destination(text, dest_key, rs)),
            }
        key = normalize_city_name_key(text)
        rs = await find_rail_station_for_destination(session, text, key)
        if rs is None:
            return None
        return {
            "lat": float(rs.latitude),
            "lon": float(rs.longitude),
            "dest_key": key,
            "station": rs,
            "is_sakhalin": bool(is_sakhalin_destination(text, key, rs)),
        }
    finally:
        await session.close()


async def _pick_nearest_options(product_id: int, dest_text: str, lat: float, lon: float, *, limit: int = 5) -> list[dict[str, Any]]:
    session = await get_session()
    try:
        dest_key = normalize_city_name_key(dest_text)
        nearest = await find_nearest_basises(
            session,
            float(lat),
            float(lon),
            int(product_id),
            limit=int(limit),
            destination_name_key=dest_key,
            destination_raw=dest_text,
        )
        options: list[dict[str, Any]] = []
        for it in nearest:
            b = it["basis"]
            price = it["price"]
            total = float(it["total_cost_per_ton"])
            dist = float(it["distance"])
            transport = "Ж/Д" if it["transport_type"] == "rail" else "Авто"
            ro = it.get("rail_origin_station_name")
            rd = it.get("rail_dest_station_name")
            options.append(
                {
                    "basis_id": int(b.id),
                    "price_id": int(getattr(price, "id", 0) or 0),
                    "product_id": int(product_id),
                    "transport_type": str(it["transport_type"]),
                    "total_per_ton": total,
                    "delivery_per_ton": float(it.get("delivery_cost_per_ton", 0.0) or 0.0),
                    "base_per_ton": float(getattr(price, "current_price", 0.0) or 0.0),
                    "distance_km": dist,
                    "rail_origin_station_name": str(ro) if ro else None,
                    "rail_dest_station_name": str(rd) if rd else None,
                    "instrument_code": (getattr(price, "instrument_code", None) or "").strip().upper() or None,
                    "label": f"{transport} {b.name} — {_format_num(total)} ₽/т",
                }
            )
        return options
    finally:
        await session.close()


async def _compare_render_basis_choices(
    session,
    *,
    query: str,
    selected_ids: list[int],
    offset: int = 0,
    page_size: int = 8,
) -> tuple[str, list[dict[str, Any]]]:
    page, total = await search_basises(session, query, offset=offset, page_size=page_size)
    if not page:
        return ("❌ Не нашёл базисы по запросу. Попробуйте другое слово.", [])

    rows: list[list[dict[str, Any]]] = []
    for b in page:
        bid = int(getattr(b, "id", 0) or 0)
        name = str(getattr(b, "name", "") or "Базис")
        mark = "✅ " if bid in selected_ids else ""
        rows.append(
            [
                {
                    "type": "callback",
                    "text": f"{mark}{name}",
                    "payload": f"a:compare:addbasis:{bid}",
                }
            ]
        )

    nav: list[dict[str, Any]] = []
    if offset > 0:
        nav.append({"type": "callback", "text": "⬅️ Назад", "payload": "a:compare:page:prev"})
    if (offset + page_size) < total:
        nav.append({"type": "callback", "text": "➡️ Ещё", "payload": "a:compare:page:next"})
    if nav:
        rows.append(nav)

    if len(selected_ids) >= 3:
        rows.append([{"type": "callback", "text": "➡️ Далее: назначение", "payload": "a:compare:dest"}])
    rows.append([{"type": "callback", "text": "🔄 Новый поиск", "payload": "a:compare:search"}])
    rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
    msg = f"<b>Сравнить 3 базиса</b>\n\nНайдено: {total}. Выбрано: <b>{len(selected_ids)}/3</b>\nВыберите базис:"
    return (msg, _inline_keyboard(rows))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"ok": "true"}


@app.post("/webhook")
async def webhook(
    request: Request,
    x_max_bot_api_secret: Optional[str] = Header(default=None, alias="X-Max-Bot-Api-Secret"),
):
    expected_secret = (os.getenv("MAX_WEBHOOK_SECRET", "") or "").strip()
    if expected_secret:
        if (x_max_bot_api_secret or "").strip() != expected_secret:
            raise HTTPException(status_code=401, detail="bad secret")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    # Update can be either {"updates":[...]} or a single Update object (webhook).
    updates: list[dict[str, Any]] = []
    if isinstance(payload, dict) and isinstance(payload.get("updates"), list):
        updates = [u for u in payload["updates"] if isinstance(u, dict)]
    elif isinstance(payload, dict):
        updates = [payload]
    else:
        raise HTTPException(status_code=400, detail="unexpected payload")

    try:
        api = max_api_from_env()
    except MaxApiError as e:
        logger.warning("MAX bot token not configured: %s (set MAX_BOT_TOKEN in .env)", e)
        # still return 200 to avoid retries storm while configuring
        return JSONResponse({"ok": True, "skipped": "no token"}, status_code=200)

    debug_updates = (os.getenv("MAX_DEBUG_UPDATES", "") or "").strip() in ("1", "true", "yes", "on")

    for upd in updates:
        utype = str(upd.get("update_type") or upd.get("type") or "").strip()
        if debug_updates:
            try:
                logger.info("MAX update_type=%s payload=%s", utype, json.dumps(upd, ensure_ascii=False)[:4000])
            except Exception:
                logger.info("MAX update_type=%s (payload not serializable)", utype)

        # 1) New message -> show menu on /start or first contact
        if utype == "message_created":
            sender_id = _extract_sender_user_id(upd)
            text = _extract_text(upd)
            if not sender_id:
                continue

            t_low = text.lower()

            # Global commands
            if text == "/start" or t_low in ("старт", "start", "меню", "menu", "/menu"):
                try:
                    _state_clear(sender_id)
                    await api.send_message(
                        # Для MVP отправляем в личку пользователю. Групповые чаты подключим позже.
                        user_id=sender_id,
                        text=_menu_text(),
                        attachments=_menu_attachments(),
                        fmt="html",
                    )
                except Exception:
                    logger.exception("Failed to send menu to sender_id=%s", sender_id)
                continue

            if t_low in ("отмена", "/cancel", "cancel"):
                _state_clear(sender_id)
                await api.send_message(
                    user_id=sender_id,
                    text="Ок, отменил. Нажмите «🏠 Меню».",
                    fmt="html",
                    attachments=_menu_attachments(),
                )
                continue

            # Continue calc flow if waiting for destination or volume
            st = _state_get(sender_id)
            stage = str(st.get("stage") or "")
            if stage == "subs_wait_price":
                raw = text.replace(" ", "").replace(",", ".")
                try:
                    target = float(raw)
                    if target <= 0:
                        raise ValueError()
                except Exception:
                    await api.send_message(user_id=sender_id, text="Введите целевую цену числом (₽/т), например 65000", fmt="html")
                    continue
                # Ensure user exists in DB
                await _get_or_create_max_user(sender_id)
                u = await _get_or_create_max_user(sender_id)
                await _create_price_alert(
                    u.id,
                    product_id=int(st.get("product_id") or 0),
                    basis_id=int(st.get("basis_id") or 0),
                    target_price=float(target),
                )
                _state.pop(sender_id, None)
                await api.send_message(user_id=sender_id, text="✅ Подписка на цену создана.", fmt="html", attachments=_menu_attachments())
                continue

            if stage == "subs_wait_anom":
                raw = text.replace(" ", "").replace(",", ".")
                try:
                    thr = float(raw)
                    if thr <= 0:
                        raise ValueError()
                except Exception:
                    await api.send_message(user_id=sender_id, text="Введите порог в процентах, например 3", fmt="html")
                    continue
                # ask direction
                st["threshold_pct"] = float(thr)
                st["stage"] = "subs_wait_anom_dir"
                _state_set(sender_id, st)
                await api.send_message(
                    user_id=sender_id,
                    text="<b>Аномалия</b>\n\nВыберите направление:",
                    fmt="html",
                    attachments=_inline_keyboard(
                        [
                            [
                                {"type": "callback", "text": "Рост или падение (|Δ|)", "payload": "subs:anom:dir:any"},
                            ],
                            [
                                {"type": "callback", "text": "Только рост", "payload": "subs:anom:dir:up"},
                                {"type": "callback", "text": "Только падение", "payload": "subs:anom:dir:down"},
                            ],
                            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
                        ]
                    ),
                )
                continue

            if stage == "subs_price_wait_basis_query":
                q = text.strip()
                if not q:
                    await api.send_message(user_id=sender_id, text=_subs_basis_search_text("price"), fmt="html")
                    continue
                session = await get_session()
                try:
                    page, total = await search_basises(session, q, offset=0, page_size=8)
                finally:
                    await session.close()
                if not page:
                    await api.send_message(user_id=sender_id, text="❌ Не нашёл базисы по запросу. Попробуйте другое слово.", fmt="html")
                    continue
                rows: list[list[dict[str, Any]]] = []
                for b in page:
                    rows.append(
                        [
                            {
                                "type": "callback",
                                "text": str(getattr(b, "name", "") or "Базис"),
                                "payload": f"subs:price:basis:{int(getattr(b, 'id', 0) or 0)}",
                            }
                        ]
                    )
                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "menu:subs"}])
                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                await api.send_message(
                    user_id=sender_id,
                    text=f"<b>Снижение цены</b>\n\nНайдено: {total}. Выберите базис:",
                    fmt="html",
                    attachments=_inline_keyboard(rows),
                )
                continue

            if stage == "subs_price_wait_target":
                raw = text.replace(" ", "").replace(",", ".")
                try:
                    target = float(raw)
                    if target <= 0:
                        raise ValueError()
                except Exception:
                    await api.send_message(user_id=sender_id, text="Введите целевую цену числом (₽/т), например 65000", fmt="html")
                    continue
                bid = int(st.get("basis_id") or 0)
                pid = int(st.get("product_id") or 0)
                if not bid or not pid:
                    _state.pop(sender_id, None)
                    await api.send_message(
                        user_id=sender_id,
                        text="Сессия устарела. Нажмите «🏠 Меню».",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                cb_user = _get(upd, "message.sender", {}) or {}
                u = await _get_or_create_max_user(
                    int(sender_id),
                    first_name=str(cb_user.get("first_name") or ""),
                    last_name=str(cb_user.get("last_name") or ""),
                )
                await _create_price_alert(u.id, product_id=int(pid), basis_id=int(bid), target_price=float(target))
                _state.pop(sender_id, None)
                await api.send_message(user_id=sender_id, text="✅ Подписка на снижение цены создана.", fmt="html", attachments=_menu_attachments())
                continue

            if stage == "subs_anom_wait_basis_query":
                q = text.strip()
                if not q:
                    await api.send_message(user_id=sender_id, text=_subs_basis_search_text("anom"), fmt="html")
                    continue
                session = await get_session()
                try:
                    page, total = await search_basises(session, q, offset=0, page_size=8)
                finally:
                    await session.close()
                if not page:
                    await api.send_message(user_id=sender_id, text="❌ Не нашёл базисы по запросу. Попробуйте другое слово.", fmt="html")
                    continue
                rows = []
                for b in page:
                    rows.append(
                        [
                            {
                                "type": "callback",
                                "text": str(getattr(b, "name", "") or "Базис"),
                                "payload": f"subs:anom:basis:{int(getattr(b, 'id', 0) or 0)}",
                            }
                        ]
                    )
                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "menu:subs"}])
                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                await api.send_message(
                    user_id=sender_id,
                    text=f"<b>Аномалия</b>\n\nНайдено: {total}. Выберите базис:",
                    fmt="html",
                    attachments=_inline_keyboard(rows),
                )
                continue

            if stage == "subs_anom_wait_thr":
                raw = text.replace(" ", "").replace(",", ".")
                try:
                    thr = float(raw)
                    if thr <= 0:
                        raise ValueError()
                except Exception:
                    await api.send_message(user_id=sender_id, text="Введите порог в процентах, например 3", fmt="html")
                    continue
                code = str(st.get("instrument_code") or "").strip().upper()
                if not code:
                    _state.pop(sender_id, None)
                    await api.send_message(
                        user_id=sender_id,
                        text="Сессия устарела. Нажмите «🏠 Меню».",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                st["threshold_pct"] = float(thr)
                st["stage"] = "subs_anom_wait_dir"
                _state_set(sender_id, st)
                await api.send_message(
                    user_id=sender_id,
                    text="<b>Аномалия</b>\n\nВыберите направление:",
                    fmt="html",
                    attachments=_inline_keyboard(
                        [
                            [
                                {"type": "callback", "text": "Рост или падение (|Δ|)", "payload": "subs:anom:dir:any"},
                            ],
                            [
                                {"type": "callback", "text": "Только рост", "payload": "subs:anom:dir:up"},
                                {"type": "callback", "text": "Только падение", "payload": "subs:anom:dir:down"},
                            ],
                            [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
                        ]
                    ),
                )
                continue

            if stage == "max_login_wait_email":
                email = (text or "").strip().lower()
                if "@" not in email or "." not in email:
                    await api.send_message(user_id=sender_id, text="Введите корректный email (например name@domain.ru)", fmt="html")
                    continue
                session = await get_session()
                try:
                    code = await create_otp(session, email)
                finally:
                    await session.close()
                try:
                    await send_smtp_email(
                        subject="Код входа — НК калькулятор топлива",
                        body=f"Ваш код входа: {code}\n\nЕсли вы не запрашивали код — проигнорируйте письмо.",
                        to_addrs=[email],
                        require_smtp=True,
                    )
                except SMTPNotConfiguredError:
                    _state.pop(sender_id, None)
                    await api.send_message(
                        user_id=sender_id,
                        text="❌ На сервере не настроена отправка почты (SMTP). Напишите администратору.",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                except Exception:
                    logger.exception("SMTP: failed to send OTP to %s", email)
                    await api.send_message(
                        user_id=sender_id,
                        text="❌ Не удалось отправить письмо с кодом. Попробуйте позже.",
                        fmt="html",
                    )
                    continue
                st["stage"] = "max_login_wait_code"
                st["otp_email"] = email
                _state_set(sender_id, st)
                await api.send_message(
                    user_id=sender_id,
                    text=f"✅ Код отправлен на <b>{email}</b>.\nВведите 6‑значный код из письма:",
                    fmt="html",
                )
                continue

            if stage == "max_login_wait_code":
                code = (text or "").strip()
                email = str(st.get("otp_email") or "").strip().lower()
                if not email:
                    _state.pop(sender_id, None)
                    await api.send_message(
                        user_id=sender_id,
                        text="Сессия устарела. Нажмите «🏠 Меню».",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                session = await get_session()
                try:
                    ok = await verify_otp(session, email, code)
                    if not ok:
                        await api.send_message(user_id=sender_id, text="❌ Неверный или просроченный код. Попробуйте ещё раз.", fmt="html")
                        continue
                    # Link+merge MAX-only into email account (единый аккаунт)
                    try:
                        await link_max_user_to_email(session, max_user_id=int(sender_id), email=email)
                    except Exception:
                        logger.exception("MAX link_max_user_to_email failed for sender_id=%s email=%s", sender_id, email)
                        await api.send_message(
                            user_id=sender_id,
                            text="❌ Не удалось привязать email (ошибка БД). Попробуйте позже.",
                            fmt="html",
                        )
                        _state.pop(sender_id, None)
                        continue
                finally:
                    await session.close()
                after = str(st.get("after_login") or "")
                _state.pop(sender_id, None)
                await api.send_message(user_id=sender_id, text="✅ Вы вошли по email.", fmt="html", attachments=_menu_attachments())
                if after == "kp":
                    # trigger KP flow again
                    await api.send_message(user_id=sender_id, text="Теперь нажмите «📩 Заявка на КП» в последнем расчёте.", fmt="html")
                if after == "subs":
                    await api.send_message(user_id=sender_id, text="Теперь откройте «🔔 Подписки».", fmt="html", attachments=_subs_menu_keyboard())
                continue

            if stage == "calc_wait_dest":
                dest = text.strip()
                d = await _resolve_destination(dest)
                if not d:
                    await api.send_message(
                        user_id=sender_id,
                        text="❌ Не смог определить назначение. Попробуйте другое название или 6‑значный ЕСР станции.",
                        fmt="html",
                    )
                    continue
                opts = await _pick_nearest_options(int(st["product_id"]), dest, float(d["lat"]), float(d["lon"]), limit=5)
                if not opts:
                    await api.send_message(
                        user_id=sender_id,
                        text="❌ Не нашёл базисы/цены под этот запрос. Попробуйте другое назначение.",
                        fmt="html",
                    )
                    continue
                st.update(
                    {
                        "stage": "calc_wait_pick",
                        "destination": dest,
                        "dest_lat": float(d["lat"]),
                        "dest_lon": float(d["lon"]),
                        "is_sakhalin": bool(d.get("is_sakhalin")),
                        "options": opts,
                    }
                )
                _state_set(sender_id, st)
                await api.send_message(
                    user_id=sender_id,
                    text=f"<b>Назначение:</b> {dest}\nВыберите базис:",
                    attachments=_basis_keyboard(opts),
                    fmt="html",
                )
                continue

            if stage == "a_trend_wait_query":
                q = text.strip()
                if not q:
                    await api.send_message(user_id=sender_id, text=_trend_basis_query_text(), fmt="html")
                    continue
                session = await get_session()
                try:
                    page, total = await search_basises(session, q, offset=0, page_size=8)
                finally:
                    await session.close()
                if not page:
                    await api.send_message(user_id=sender_id, text="❌ Не нашёл базисы по запросу. Попробуйте другое слово.", fmt="html")
                    continue
                rows: list[list[dict[str, Any]]] = []
                for b in page:
                    rows.append(
                        [
                            {
                                "type": "callback",
                                "text": str(getattr(b, "name", "") or "Базис"),
                                "payload": f"a:trend:basis:{int(getattr(b, 'id', 0) or 0)}",
                            }
                        ]
                    )
                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "a:trend"}])
                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                await api.send_message(
                    user_id=sender_id,
                    text=f"<b>Тренд</b>\n\nНайдено: {total}. Выберите базис:",
                    fmt="html",
                    attachments=_inline_keyboard(rows),
                )
                continue

            if stage == "digest_wait_basis_query":
                q = text.strip()
                if not q:
                    await api.send_message(
                        user_id=sender_id,
                        text="<b>Ежедневная сводка</b>\n\nВведите часть названия базиса:",
                        fmt="html",
                    )
                    continue
                session = await get_session()
                try:
                    page, total = await search_basises(session, q, offset=0, page_size=8)
                finally:
                    await session.close()
                if not page:
                    await api.send_message(user_id=sender_id, text="❌ Не нашёл базисы по запросу. Попробуйте другое слово.", fmt="html")
                    continue
                rows: list[list[dict[str, Any]]] = []
                for b in page:
                    rows.append(
                        [
                            {
                                "type": "callback",
                                "text": str(getattr(b, "name", "") or "Базис"),
                                "payload": f"digest:basis:{int(getattr(b, 'id', 0) or 0)}",
                            }
                        ]
                    )
                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "subs:digest:start"}])
                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                await api.send_message(
                    user_id=sender_id,
                    text=f"<b>Ежедневная сводка</b>\n\nНайдено: {total}. Выберите базис:",
                    fmt="html",
                    attachments=_inline_keyboard(rows),
                )
                continue

            if stage == "digest_wait_destination":
                dest_text = text.strip()
                if not dest_text:
                    await api.send_message(
                        user_id=sender_id,
                        text="<b>Ежедневная сводка с доставкой</b>\n\nВведите назначение (город/посёлок или «ст. ...»):",
                        fmt="html",
                    )
                    continue
                # Используем ту же логику, что и веб-калькулятор (создаст/переиспользует CityDestination).
                session = await get_session()
                try:
                    from web.services.calc_service import resolve_destination_to_id

                    dest_id, lat, lon, dest_key, _is_station = await resolve_destination_to_id(session, dest_text)
                finally:
                    await session.close()
                if not dest_id or float(lat) == 0.0 or float(lon) == 0.0:
                    await api.send_message(
                        user_id=sender_id,
                        text="❌ Не смог определить назначение. Попробуйте другое написание или 6‑значный ЕСР станции.",
                        fmt="html",
                    )
                    continue
                st.update(
                    {
                        "stage": "digest_wait_basis_query",
                        "delivery_mode": "with_delivery",
                        "destination_id": int(dest_id),
                        "destination_name": dest_text,
                        "destination_key": str(dest_key or ""),
                    }
                )
                _state_set(sender_id, st)
                await api.send_message(
                    user_id=sender_id,
                    text="<b>Ежедневная сводка с доставкой</b>\n\nВведите часть названия базиса:",
                    fmt="html",
                )
                continue

            if stage == "a_compare_wait_query":
                q = text.strip()
                if not q:
                    await api.send_message(
                        user_id=sender_id,
                        text=_compare_search_text(list(st.get("basis_ids") or [])),
                        fmt="html",
                    )
                    continue
                st["last_query"] = q
                st["offset"] = 0
                _state_set(sender_id, st)
                session = await get_session()
                try:
                    msg, kb = await _compare_render_basis_choices(
                        session,
                        query=q,
                        selected_ids=list(st.get("basis_ids") or []),
                        offset=0,
                        page_size=8,
                    )
                finally:
                    await session.close()
                if not kb:
                    await api.send_message(user_id=sender_id, text=msg, fmt="html")
                else:
                    await api.send_message(user_id=sender_id, text=msg, fmt="html", attachments=kb)
                continue

            if stage == "a_compare_wait_dest":
                dest = text.strip()
                pid = int(st.get("product_id") or 0)
                basis_ids = list(st.get("basis_ids") or [])
                if not pid or len(basis_ids) != 3:
                    _state_clear(sender_id)
                    await api.send_message(
                        user_id=sender_id,
                        text="Сессия устарела. Нажмите «🏠 Меню».",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                session = await get_session()
                try:
                    res = await compute_compare_three(
                        session,
                        product_id=int(pid),
                        basis_ids=[int(x) for x in basis_ids],
                        destination_text=dest,
                    )
                finally:
                    await session.close()
                if not res:
                    await api.send_message(user_id=sender_id, text="❌ Не смог сравнить: проверьте базисы/назначение.", fmt="html")
                    continue
                lines: list[str] = [
                    "<b>🟦 Сравнить 3 базиса</b>",
                    "Полная версия: https://calc.nk-vsnp.ru/analytics",
                    "",
                    f"Топливо: <b>{res.title_product}</b>",
                    f"Назначение: <b>{res.destination}</b>",
                    "",
                ]
                if res.best_line:
                    lines.append(res.best_line)
                    lines.append("")
                for row in res.rows:
                    lines.append(f"<b>{row.basis_name}</b> ({row.transport})")
                    lines.append(_strip_html(row.html_block))
                    lines.append("")
                st_for_subs = {
                    "product_id": int(pid),
                    "basis_ids": [int(x) for x in basis_ids],
                    "instrument_codes": [
                        str(getattr(r, "instrument_code", "") or "").strip().upper() for r in (res.rows or [])
                    ],
                }
                _state.pop(sender_id, None)
                _state_set(sender_id, {"stage": "", **st_for_subs})
                await api.send_message(
                    user_id=sender_id,
                    text="\n".join(lines).strip(),
                    fmt="html",
                    attachments=_compare_result_keyboard(st_for_subs["basis_ids"], st_for_subs["instrument_codes"]),
                )
                continue

            if stage == "calc_wait_volume":
                raw = text.replace(" ", "").replace(",", ".")
                try:
                    vol = float(raw)
                    if vol <= 0:
                        raise ValueError()
                except Exception:
                    await api.send_message(user_id=sender_id, text="Введите объём числом в тоннах, например 20 или 45.5", fmt="html")
                    continue
                opt = st.get("picked")
                if not isinstance(opt, dict):
                    _state_clear(sender_id)
                    await api.send_message(
                        user_id=sender_id,
                        text="Сессия устарела. Нажмите «🏠 Меню».",
                        fmt="html",
                        attachments=_menu_attachments(),
                    )
                    continue
                base = float(opt.get("base_per_ton") or 0.0)
                delivery = float(opt.get("delivery_per_ton") or 0.0)
                comm_pt = 150.0 if float(vol) >= 1000.0 else 200.0
                comm_total = comm_pt * float(vol)
                total_pt = float(opt.get("total_per_ton") or (base + delivery)) + float(comm_pt)
                total_sum = total_pt * vol
                rate = (delivery / float(opt.get("distance_km") or 0.0)) if float(opt.get("distance_km") or 0.0) > 0 else 0.0

                # Save for "Почему так"
                _last_result[sender_id] = {
                    "destination": st.get("destination"),
                    "basis_label": opt.get("label"),
                    "transport_type": opt.get("transport_type"),
                    "distance_km": float(opt.get("distance_km") or 0.0),
                    "rate": float(rate),
                    "volume_tons": float(vol),
                    "base_per_ton": float(base),
                    "delivery_per_ton": float(delivery),
                    "broker_commission_per_ton": float(comm_pt),
                    "broker_commission_total": float(comm_total),
                    "total_per_ton": float(total_pt),
                    "total_sum": float(total_sum),
                    "rail_origin_station_name": opt.get("rail_origin_station_name"),
                    "rail_dest_station_name": opt.get("rail_dest_station_name"),
                    "is_sakhalin": bool(st.get("is_sakhalin")),
                    "product_id": int(opt.get("product_id") or 0),
                    "basis_id": int(opt.get("basis_id") or 0),
                    "price_id": int(opt.get("price_id") or 0),
                    "instrument_code": opt.get("instrument_code"),
                }
                # Persist UserRequest in DB (so we can attach leads/notifications)
                try:
                    session = await get_session()
                    try:
                        u = await _get_or_create_max_user(sender_id)
                        # destination -> CityDestination row (same logic as web)
                        dest_text = str(st.get("destination") or "").strip()
                        dest_key = normalize_city_name_key(dest_text)
                        coords = await get_coordinates_from_city(dest_text, session)
                        if coords:
                            dest_lat, dest_lon = float(coords[0]), float(coords[1])
                        else:
                            rs = await find_rail_station_for_destination(session, dest_text, dest_key)
                            if rs is None:
                                raise ValueError("destination not resolved")
                            dest_lat, dest_lon = float(rs.latitude), float(rs.longitude)
                        # find or create CityDestination by exact coords (like web)
                        from sqlalchemy import func

                        dest_obj = (
                            await session.execute(
                                select(CityDestination)
                                .where(
                                    func.abs(CityDestination.latitude - dest_lat) < 0.000001,
                                    func.abs(CityDestination.longitude - dest_lon) < 0.000001,
                                )
                                .limit(1)
                            )
                        ).scalar_one_or_none()
                        if dest_obj is None:
                            dest_obj = (
                                await session.execute(select(CityDestination).where(CityDestination.name == dest_text).limit(1))
                            ).scalar_one_or_none()
                        if dest_obj is None:
                            dest_obj = CityDestination(name=dest_text, latitude=dest_lat, longitude=dest_lon)
                            session.add(dest_obj)
                            await session.commit()
                            await session.refresh(dest_obj)

                        ur = UserRequest(
                            user_id=int(u.id),
                            product_id=int(opt.get("product_id") or 0),
                            basis_id=int(opt.get("basis_id") or 0),
                            price_id=int(opt.get("price_id") or 0) or None,
                            city_destination_id=int(dest_obj.id),
                            volume=float(vol),
                            base_price=float(base),
                            distance_km=float(opt.get("distance_km") or 0.0),
                            transport_type=str(opt.get("transport_type") or ""),
                            delivery_cost=float(delivery) * float(vol),
                            total_price=float(total_sum),
                        )
                        session.add(ur)
                        await session.commit()
                        await session.refresh(ur)
                        _last_result[sender_id]["request_id"] = int(ur.id)
                    finally:
                        await session.close()
                except Exception:
                    logger.exception("Failed to persist UserRequest for MAX calculation")
                msg = "\n".join(
                    [
                        "<b>✅ Результат</b>",
                        f"Назначение: <b>{st.get('destination','—')}</b>",
                        f"Базис: <b>{opt.get('label','—')}</b>",
                        f"Объём: <b>{vol:g} т</b>",
                        "",
                        f"Цена на базисе: <b>{_format_num(base)} ₽/т</b>",
                        f"Доставка: <b>{_format_num(delivery)} ₽/т</b>",
                        f"Комиссия брокера: <b>{_format_num(comm_pt)} ₽/т</b>",
                        f"Итого: <b>{_format_num(total_pt)} ₽/т</b>",
                        f"Сумма за объём: <b>{_format_num(total_sum)} ₽</b>",
                        "",
                        "⚠️ Данные расчёта <b>ориентировочные</b> (предварительный характер). Точное коммерческое предложение — после согласования с менеджером.",
                        "",
                        "Чтобы <b>оформить заявку на КП прямо здесь</b>, нажмите кнопку <b>«📩 Заявка на КП»</b> под этим сообщением. Если вы ещё не входили по email, бот попросит email для отправки заявки.",
                    ]
                )
                # Keep last_result for "Почему так", clear only flow state
                _state.pop(sender_id, None)
                await api.send_message(user_id=sender_id, text=msg, fmt="html", attachments=_result_keyboard())
                continue

            # default: if user is outside any flow, show menu (for those who don't know /start)
            if not stage:
                await api.send_message(
                    user_id=sender_id,
                    text="Не понял сообщение. Нажмите «🏠 Меню» и выберите действие.",
                    fmt="html",
                    attachments=_menu_attachments(),
                )
                continue

            # default: ignore inside flows
            continue

        # 2) Bot started event -> greet
        if utype == "bot_started":
            sender_id = _extract_sender_user_id(upd)
            if sender_id:
                try:
                    await api.send_message(
                        user_id=sender_id,
                        text=(
                            "<b>Привет!</b>\n\n"
                            "Здесь можно за минуту получить <b>цену топлива с доставкой</b> и понять, "
                            "<b>какой базис выгоднее</b> по данным торгов.\n\n"
                            "В «Подписках» — в том числе <b>ежедневная сводка по базису</b> (раз в день в 14:15 МСК), "
                            "подписка на цену и аномалии.\n\n"
                            "Лучше сразу <b>войти по email</b> — тогда вы сможете <b>оформлять КП</b> и получать "
                            "уведомления без перехода на сайт."
                        ),
                        attachments=_menu_attachments(),
                        fmt="html",
                    )
                except Exception:
                    logger.exception("Failed to greet sender_id=%s", sender_id)
            continue

        # 3) Callback buttons
        if utype == "message_callback":
            callback_id = str(_get(upd, "callback.callback_id", "") or "").strip()
            data = str(_get(upd, "callback.payload", "") or "").strip()
            sender_id = _extract_callback_user_id(upd) or _extract_sender_user_id(upd)
            if callback_id:
                note = None
                if data == "menu:home":
                    _state_clear(int(sender_id or 0) or 0)
                    note = "Меню"
                    try:
                        await api.answer_callback(callback_id=callback_id, notification=note)
                        if sender_id:
                            await api.send_message(user_id=int(sender_id), text=_menu_text(), attachments=_menu_attachments(), fmt="html")
                    except Exception:
                        logger.exception("Failed to show home menu")
                    continue

                if data == "menu:calc":
                    if sender_id:
                        session = await get_session()
                        try:
                            prods = await list_products_for_calc(session)
                        finally:
                            await session.close()
                        _state_set(int(sender_id), {"stage": "calc_wait_prod"})
                        await api.send_message(user_id=int(sender_id), text=_calc_start_text(), attachments=_prod_keyboard(prods), fmt="html")
                    note = "Выбор топлива"
                elif data == "menu:login":
                    if sender_id:
                        _state_set(int(sender_id), {"stage": "max_login_wait_email"})
                        await api.send_message(user_id=int(sender_id), text="Введите ваш email для входа:", fmt="html")
                    note = "Ок"
                elif data == "menu:logout":
                    if sender_id:
                        session = await get_session()
                        try:
                            u = (await session.execute(select(User).where(User.max_user_id == int(sender_id)).limit(1))).scalar_one_or_none()
                            if u is not None:
                                u.email = None
                                await session.commit()
                        finally:
                            await session.close()
                        await api.send_message(user_id=int(sender_id), text="✅ Вы вышли.", fmt="html", attachments=_menu_attachments())
                    note = "Ок"
                elif data == "menu:analytics":
                    if sender_id:
                        await api.send_message(
                            user_id=int(sender_id),
                            text=_analytics_menu_text(),
                            fmt="html",
                            attachments=_analytics_menu_kb(),
                        )
                    note = "Ок"
                elif data == "menu:about":
                    if sender_id:
                        await api.send_message(
                            user_id=int(sender_id),
                            text=_about_text(),
                            fmt="html",
                            attachments=_menu_attachments(),
                        )
                    note = "Ок"
                elif data == "menu:subs":
                    if sender_id:
                        if not await _require_linked_email(api, sender_id=int(sender_id), after_login="subs"):
                            note = "Ок"
                            continue
                        await api.send_message(
                            user_id=int(sender_id),
                            text=(
                                "<b>Подписки</b>\n\n"
                                "• <b>Ежедневная сводка</b> — один раз в день в <b>14:15 (МСК)</b> пришлём актуальные "
                                "биржевые цены на выбранном базисе (одно топливо или все доступные на базисе). "
                                "\n"
                                "• <b>Цена ↓</b> — когда цена станет не выше заданной.\n"
                                "• <b>Аномалия</b> — при резком движении цены по коду инструмента\n\n"
                                "Список и отключение — в «Мои подписки»."
                            ),
                            fmt="html",
                            attachments=_subs_menu_keyboard(),
                        )
                    note = "Ок"
                elif data == "subs:price:start":
                    if sender_id:
                        if not await _require_linked_email(api, sender_id=int(sender_id), after_login="subs"):
                            note = "Ок"
                            continue
                        _state_set(int(sender_id), {"stage": "subs_price_wait_basis_query"})
                        await api.send_message(user_id=int(sender_id), text=_subs_basis_search_text("price"), fmt="html")
                    note = "Ок"
                elif data == "subs:anom:start":
                    if sender_id:
                        if not await _require_linked_email(api, sender_id=int(sender_id), after_login="subs"):
                            note = "Ок"
                            continue
                        _state_set(int(sender_id), {"stage": "subs_anom_wait_basis_query"})
                        await api.send_message(user_id=int(sender_id), text=_subs_basis_search_text("anom"), fmt="html")
                    note = "Ок"
                elif data == "subs:digest:start":
                    if sender_id:
                        if not await _require_linked_email(api, sender_id=int(sender_id), after_login="subs"):
                            note = "Ок"
                            continue
                        await api.send_message(
                            user_id=int(sender_id),
                            fmt="html",
                            text="<b>Ежедневная сводка</b>\n\nКак присылать?",
                            attachments=_inline_keyboard(
                                [
                                    [
                                        {
                                            "type": "callback",
                                            "text": "1) Просто цены",
                                            "payload": "subs:digest:mode:prices",
                                        }
                                    ],
                                    [
                                        {
                                            "type": "callback",
                                            "text": "2) Цены с доставкой",
                                            "payload": "subs:digest:mode:delivery",
                                        }
                                    ],
                                    [{"type": "callback", "text": "⬅️ Назад", "payload": "menu:subs"}],
                                    [{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}],
                                ]
                            ),
                        )
                        _state_set(int(sender_id), {"stage": "digest_wait_mode"})
                    note = "Ок"
                elif data == "subs:digest:mode:prices":
                    if sender_id:
                        _state_set(int(sender_id), {"stage": "digest_wait_basis_query", "delivery_mode": "prices_only"})
                        await api.send_message(
                            user_id=int(sender_id),
                            text="<b>Ежедневная сводка</b>\n\nВведите часть названия базиса:",
                            fmt="html",
                        )
                    note = "Ок"
                elif data == "subs:digest:mode:delivery":
                    if sender_id:
                        _state_set(int(sender_id), {"stage": "digest_wait_destination", "delivery_mode": "with_delivery"})
                        await api.send_message(
                            user_id=int(sender_id),
                            text="<b>Ежедневная сводка с доставкой</b>\n\nВведите назначение (город/посёлок или «ст. ...»):",
                            fmt="html",
                        )
                    note = "Ок"
                elif data.startswith("subs:price:basis:"):
                    if sender_id:
                        try:
                            bid = int(data.split(":")[3])
                        except Exception:
                            bid = 0
                        if bid:
                            session = await get_session()
                            try:
                                basis = await session.get(Basis, int(bid))
                            finally:
                                await session.close()
                            bname = str(getattr(basis, "name", "") or "Базис")
                            prods = await _list_products_on_basis(int(bid))
                            if not prods:
                                await api.send_message(user_id=int(sender_id), text=f"❌ На базисе «{bname}» нет активных цен.", fmt="html")
                            else:
                                rows: list[list[dict[str, Any]]] = []
                                for pid, name in prods[:20]:
                                    short = (name[:38] + "…") if len(name) > 38 else name
                                    rows.append(
                                        [{"type": "callback", "text": short, "payload": f"subs:price:prod:{int(bid)}:{int(pid)}"}]
                                    )
                                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "subs:price:start"}])
                                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text=f"<b>Снижение цены</b>\n\nБазис: <b>{bname}</b>\nВыберите топливо:",
                                    fmt="html",
                                    attachments=_inline_keyboard(rows),
                                )
                    note = "Ок"
                elif data.startswith("subs:price:prod:"):
                    if sender_id:
                        parts = data.split(":")
                        try:
                            bid = int(parts[3])
                            pid = int(parts[4])
                        except Exception:
                            bid = 0
                            pid = 0
                        if bid and pid:
                            _state_set(int(sender_id), {"stage": "subs_price_wait_target", "basis_id": int(bid), "product_id": int(pid)})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите целевую цену ₽/т (когда цена станет ≤ неё — уведомлю):",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("subs:anom:basis:"):
                    if sender_id:
                        try:
                            bid = int(data.split(":")[3])
                        except Exception:
                            bid = 0
                        if bid:
                            session = await get_session()
                            try:
                                basis = await session.get(Basis, int(bid))
                            finally:
                                await session.close()
                            bname = str(getattr(basis, "name", "") or "Базис")
                            prods = await _list_products_on_basis(int(bid))
                            if not prods:
                                await api.send_message(user_id=int(sender_id), text=f"❌ На базисе «{bname}» нет активных цен.", fmt="html")
                            else:
                                rows = []
                                for pid, name in prods[:20]:
                                    short = (name[:38] + "…") if len(name) > 38 else name
                                    rows.append([{"type": "callback", "text": short, "payload": f"subs:anom:prod:{int(bid)}:{int(pid)}"}])
                                rows.append([{"type": "callback", "text": "⬅️ Назад", "payload": "subs:anom:start"}])
                                rows.append([{"type": "callback", "text": "🏠 Меню", "payload": "menu:home"}])
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text=f"<b>Аномалия</b>\n\nБазис: <b>{bname}</b>\nВыберите топливо:",
                                    fmt="html",
                                    attachments=_inline_keyboard(rows),
                                )
                    note = "Ок"
                elif data.startswith("subs:anom:prod:"):
                    if sender_id:
                        parts = data.split(":")
                        try:
                            bid = int(parts[3])
                            pid = int(parts[4])
                        except Exception:
                            bid = 0
                            pid = 0
                        if bid and pid:
                            session = await get_session()
                            try:
                                row = (
                                    await session.execute(
                                        select(ProductBasisPrice.instrument_code)
                                        .where(ProductBasisPrice.basis_id == int(bid))
                                        .where(ProductBasisPrice.product_id == int(pid))
                                        .where(ProductBasisPrice.is_active.is_(True))
                                        .limit(1)
                                    )
                                ).scalar_one_or_none()
                            finally:
                                await session.close()
                            code = (row or "").strip().upper()
                            if not code:
                                await api.send_message(user_id=int(sender_id), text="❌ Не нашёл код инструмента для этой пары.", fmt="html")
                            else:
                                _state_set(int(sender_id), {"stage": "subs_anom_wait_thr", "instrument_code": code})
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="Введите порог аномалии в % (например 3). Уведомлю при |изменении| ≥ порога.",
                                    fmt="html",
                                )
                    note = "Ок"
                elif data.startswith("subs:anom:dir:"):
                    if not sender_id:
                        continue
                    direction = data.split("subs:anom:dir:", 1)[1].strip().lower()
                    if direction not in ("any", "up", "down"):
                        direction = "any"
                    st = _state_get(int(sender_id))
                    stage = str(st.get("stage") or "")
                    if stage not in ("subs_anom_wait_dir", "subs_wait_anom_dir"):
                        continue
                    code = str(st.get("instrument_code") or "").strip().upper()
                    thr = float(st.get("threshold_pct") or 0.0)
                    if not code or thr <= 0:
                        _state.pop(int(sender_id), None)
                        await api.send_message(
                            user_id=int(sender_id),
                            text="Сессия устарела. Нажмите «🏠 Меню».",
                            fmt="html",
                            attachments=_menu_attachments(),
                        )
                        continue
                    cb_user = _get(upd, "callback.user", {}) or {}
                    u = await _get_or_create_max_user(
                        int(sender_id),
                        first_name=str(cb_user.get("first_name") or ""),
                        last_name=str(cb_user.get("last_name") or ""),
                    )
                    await _create_anomaly_alert(
                        u.id,
                        instrument_code=code,
                        threshold_pct=float(thr),
                        direction=direction,
                    )
                    _state.pop(int(sender_id), None)
                    await api.send_message(user_id=int(sender_id), text="✅ Подписка на аномалию создана.", fmt="html", attachments=_menu_attachments())
                    note = "Ок"
                elif data.startswith("digest:basis:"):
                    if sender_id:
                        try:
                            bid = int(data.split(":")[2])
                        except Exception:
                            bid = 0
                        if bid:
                            session = await get_session()
                            try:
                                basis = await session.get(Basis, int(bid))
                            finally:
                                await session.close()
                            bname = str(getattr(basis, "name", "") or "Базис")
                            prods = await _list_products_on_basis(int(bid))
                            if not prods:
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text=f"❌ На базисе «{bname}» нет активных цен в базе. Попробуйте другой базис или позже после импорта.",
                                    fmt="html",
                                )
                            else:
                                _state_set(int(sender_id), {"stage": ""})
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text=f"<b>Ежедневная сводка</b>\n\nБазис: <b>{bname}</b>\nВыберите охват:",
                                    fmt="html",
                                    attachments=_digest_scope_keyboard(int(bid), prods),
                                )
                    note = "Ок"
                elif data.startswith("digest:all:"):
                    if sender_id:
                        try:
                            bid = int(data.split(":")[2])
                        except Exception:
                            bid = 0
                        if bid:
                            u = await _get_or_create_max_user(
                                int(sender_id),
                                first_name=str((cb_user := _get(upd, "callback.user", {}) or {}).get("first_name") or ""),
                                last_name=str(cb_user.get("last_name") or ""),
                            )
                            try:
                                await _create_digest_subscription(
                                    u.id,
                                    basis_id=int(bid),
                                    all_products=True,
                                    product_id=None,
                                    delivery_mode=str(_state_get(int(sender_id)).get("delivery_mode") or "prices_only"),
                                    destination_id=_state_get(int(sender_id)).get("destination_id"),
                                    destination_name=_state_get(int(sender_id)).get("destination_name"),
                                    destination_key=_state_get(int(sender_id)).get("destination_key"),
                                )
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="✅ Подписка на ежедневную сводку создана (все продукты на базисе).",
                                    fmt="html",
                                    attachments=_menu_attachments(),
                                )
                            except IntegrityError:
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="ℹ️ Такая подписка уже есть.",
                                    fmt="html",
                                    attachments=_menu_attachments(),
                                )
                    note = "Ок"
                elif data.startswith("digest:prod:"):
                    if sender_id:
                        parts = data.split(":")
                        try:
                            bid = int(parts[2])
                            pid = int(parts[3])
                        except Exception:
                            bid = 0
                            pid = 0
                        if bid and pid:
                            u = await _get_or_create_max_user(
                                int(sender_id),
                                first_name=str((cb_user := _get(upd, "callback.user", {}) or {}).get("first_name") or ""),
                                last_name=str(cb_user.get("last_name") or ""),
                            )
                            try:
                                await _create_digest_subscription(
                                    u.id,
                                    basis_id=int(bid),
                                    all_products=False,
                                    product_id=int(pid),
                                    delivery_mode=str(_state_get(int(sender_id)).get("delivery_mode") or "prices_only"),
                                    destination_id=_state_get(int(sender_id)).get("destination_id"),
                                    destination_name=_state_get(int(sender_id)).get("destination_name"),
                                    destination_key=_state_get(int(sender_id)).get("destination_key"),
                                )
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="✅ Подписка на ежедневную сводку создана.",
                                    fmt="html",
                                    attachments=_menu_attachments(),
                                )
                            except IntegrityError:
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="ℹ️ Такая подписка уже есть.",
                                    fmt="html",
                                    attachments=_menu_attachments(),
                                )
                    note = "Ок"
                elif data == "calc:restart":
                    if sender_id:
                        session = await get_session()
                        try:
                            prods = await list_products_for_calc(session)
                        finally:
                            await session.close()
                        _state_set(int(sender_id), {"stage": "calc_wait_prod"})
                        await api.send_message(user_id=int(sender_id), text=_calc_start_text(), attachments=_prod_keyboard(prods), fmt="html")
                    note = "Ок"
                elif data.startswith("calc:prod:"):
                    if sender_id:
                        try:
                            pid = int(data.split(":")[2])
                        except Exception:
                            pid = 0
                        if pid:
                            _state_set(int(sender_id), {"stage": "calc_wait_dest", "product_id": pid})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите назначение (город/станция) или 6‑значный ЕСР станции.\nНапример: Москва, ст. Тында, 910000",
                                fmt="html",
                            )
                    note = "Ок"
                elif data == "a:trend":
                    if sender_id:
                        _state_set(int(sender_id), {"stage": "a_trend_wait_query"})
                        await api.send_message(user_id=int(sender_id), text=_trend_basis_query_text(), fmt="html")
                    note = "Ок"
                elif data.startswith("a:trend:basis:"):
                    if sender_id:
                        try:
                            bid = int(data.split(":")[3])
                        except Exception:
                            bid = 0
                        if bid:
                            session = await get_session()
                            try:
                                basis = await session.get(Basis, int(bid))
                            finally:
                                await session.close()
                            bname = str(getattr(basis, "name", "") or "Базис")
                            prods = await _list_compare_products()
                            _state_set(int(sender_id), {"stage": "a_trend_wait_prod", "basis_id": int(bid), "basis_name": bname})
                            await api.send_message(
                                user_id=int(sender_id),
                                text=_trend_products_text(bname),
                                attachments=_prod_keyboard_with_prefix(prods, "a:trend:prod:"),
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("a:trend:prod:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        try:
                            pid = int(data.split(":")[3])
                        except Exception:
                            pid = 0
                        bid = int(st.get("basis_id") or 0)
                        if pid and bid:
                            session = await get_session()
                            try:
                                tr = await compute_trend(session, int(bid), int(pid))
                            finally:
                                await session.close()
                            if not tr:
                                await api.send_message(user_id=int(sender_id), text="❌ Нет данных по этому базису/топливу.", fmt="html")
                            else:
                                msg_lines: list[str] = [
                                    "<b>📈 Тренд на базисе</b>",
                                    "Полная версия: https://calc.nk-vsnp.ru/analytics",
                                    "",
                                    f"Базис: <b>{tr.basis_name}</b>",
                                    f"Топливо: <b>{tr.product_name}</b>",
                                    f"Код: <b>{tr.instrument_code}</b>",
                                    "",
                                ]
                                msg_lines.extend(tr.metrics30_text or [])
                                if tr.basis_quality_text:
                                    msg_lines.append("")
                                    msg_lines.append(tr.basis_quality_text)
                                if tr.lines:
                                    msg_lines.append("")
                                    msg_lines.append("<b>Последние торги:</b>")
                                    msg_lines.extend(tr.lines[:5])
                                if tr.pmin or tr.pmax or tr.pforecast:
                                    msg_lines.append("")
                                    msg_lines.append(f"30д: min {tr.pmin}, max {tr.pmax}, прогноз {tr.pforecast}")
                                _state_set(
                                    int(sender_id),
                                    {
                                        "stage": "",
                                        "trend_product_id": int(pid),
                                        "trend_basis_id": int(bid),
                                        "trend_instrument_code": str(tr.instrument_code or "").strip().upper(),
                                    },
                                )
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="\n".join(msg_lines),
                                    fmt="html",
                                    attachments=_trend_result_keyboard(int(bid), int(pid), str(tr.instrument_code or "")),
                                )
                    note = "Ок"
                elif data == "a:compare":
                    if sender_id:
                        prods = await _list_compare_products()
                        _state_set(int(sender_id), {"stage": "a_compare_wait_prod"})
                        await api.send_message(
                            user_id=int(sender_id),
                            text=_compare_start_text(),
                            attachments=_prod_keyboard_with_prefix(prods, "a:compare:prod:"),
                            fmt="html",
                        )
                    note = "Ок"
                elif data.startswith("a:compare:prod:"):
                    if sender_id:
                        try:
                            pid = int(data.split(":")[3])
                        except Exception:
                            pid = 0
                        if pid:
                            _state_set(
                                int(sender_id),
                                {
                                    "stage": "a_compare_wait_query",
                                    "product_id": int(pid),
                                    "basis_ids": [],
                                    "last_query": "",
                                    "offset": 0,
                                },
                            )
                            await api.send_message(
                                user_id=int(sender_id),
                                text=_compare_search_text([]),
                                fmt="html",
                            )
                    note = "Ок"
                elif data == "a:compare:search":
                    if sender_id:
                        st = _state_get(int(sender_id))
                        st["stage"] = "a_compare_wait_query"
                        _state_set(int(sender_id), st)
                        await api.send_message(
                            user_id=int(sender_id),
                            text=_compare_search_text(list(st.get("basis_ids") or [])),
                            fmt="html",
                        )
                    note = "Ок"
                elif data.startswith("a:compare:addbasis:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        basis_ids = list(st.get("basis_ids") or [])
                        try:
                            bid = int(data.split(":")[3])
                        except Exception:
                            bid = 0
                        if bid:
                            if bid in basis_ids:
                                basis_ids = [x for x in basis_ids if int(x) != int(bid)]
                            else:
                                if len(basis_ids) < 3:
                                    basis_ids.append(int(bid))
                            st["basis_ids"] = basis_ids
                            _state_set(int(sender_id), st)
                            q = str(st.get("last_query") or "").strip()
                            await api.send_message(
                                user_id=int(sender_id),
                                text=_compare_after_pick_text(len(basis_ids)),
                                fmt="html",
                                attachments=_compare_after_pick_kb(len(basis_ids), can_show_list=bool(q)),
                            )
                    note = "Ок"
                elif data.startswith("a:compare:page:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        q = str(st.get("last_query") or "").strip()
                        if not q:
                            await api.send_message(user_id=int(sender_id), text=_compare_search_text(list(st.get("basis_ids") or [])), fmt="html")
                        else:
                            off = int(st.get("offset") or 0)
                            if data.endswith(":next"):
                                off += 8
                            elif data.endswith(":prev"):
                                off = max(0, off - 8)
                            st["offset"] = off
                            _state_set(int(sender_id), st)
                            session = await get_session()
                            try:
                                msg, kb = await _compare_render_basis_choices(
                                    session,
                                    query=q,
                                    selected_ids=list(st.get("basis_ids") or []),
                                    offset=off,
                                    page_size=8,
                                )
                            finally:
                                await session.close()
                            await api.send_message(user_id=int(sender_id), text=msg, fmt="html", attachments=kb)
                    note = "Ок"
                elif data == "a:compare:showlist":
                    if sender_id:
                        st = _state_get(int(sender_id))
                        q = str(st.get("last_query") or "").strip()
                        if not q:
                            await api.send_message(
                                user_id=int(sender_id),
                                text=_compare_search_text(list(st.get("basis_ids") or [])),
                                fmt="html",
                            )
                        else:
                            off = int(st.get("offset") or 0)
                            session = await get_session()
                            try:
                                msg, kb = await _compare_render_basis_choices(
                                    session,
                                    query=q,
                                    selected_ids=list(st.get("basis_ids") or []),
                                    offset=off,
                                    page_size=8,
                                )
                            finally:
                                await session.close()
                            await api.send_message(user_id=int(sender_id), text=msg, fmt="html", attachments=kb)
                    note = "Ок"
                elif data == "a:compare:clear":
                    if sender_id:
                        st = _state_get(int(sender_id))
                        st["basis_ids"] = []
                        _state_set(int(sender_id), st)
                        await api.send_message(
                            user_id=int(sender_id),
                            text=_compare_search_text([]),
                            fmt="html",
                        )
                    note = "Ок"
                elif data == "a:compare:dest":
                    if sender_id:
                        st = _state_get(int(sender_id))
                        basis_ids = list(st.get("basis_ids") or [])
                        if len(basis_ids) != 3:
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Выберите ровно 3 базиса, затем нажмите «Далее: назначение».",
                                fmt="html",
                            )
                        else:
                            st["stage"] = "a_compare_wait_dest"
                            _state_set(int(sender_id), st)
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите назначение (город/станция) или 6‑значный ЕСР станции.\nНапример: Москва, ст. Тында, 910000",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("a:subs:price:"):
                    if sender_id:
                        # a:subs:price:<product_id>:<basis_id>
                        try:
                            _, _, _, pid_s, bid_s = data.split(":", 4)
                            pid = int(pid_s)
                            bid = int(bid_s)
                        except Exception:
                            pid = 0
                            bid = 0
                        if not pid or not bid:
                            await api.send_message(user_id=int(sender_id), text="❌ Не смог понять базис/топливо.", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_price", "product_id": int(pid), "basis_id": int(bid)})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите целевую цену на <b>базисе</b> ₽/т (когда цена станет ≤ неё — уведомлю).\nНапример: <b>65000</b>",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("a:subs:anom:"):
                    if sender_id:
                        code = data.split("a:subs:anom:", 1)[1].strip().upper()
                        if not code:
                            await api.send_message(user_id=int(sender_id), text="❌ Нет кода инструмента.", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_anom", "instrument_code": code})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите порог аномалии в % (например 3). Уведомлю, если изменение за день будет ≥ порога.",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("a:subs:price_basis:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        try:
                            bid = int(data.split(":")[3])
                        except Exception:
                            bid = 0
                        pid = int(st.get("product_id") or 0)
                        if not pid or not bid:
                            await api.send_message(user_id=int(sender_id), text="❌ Сессия сравнения устарела. Запустите сравнение заново.", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_price", "product_id": int(pid), "basis_id": int(bid)})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите целевую цену на <b>базисе</b> ₽/т (когда цена станет ≤ неё — уведомлю).\nНапример: <b>65000</b>",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("a:subs:anom_idx:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        try:
                            idx = int(data.split(":")[3])
                        except Exception:
                            idx = 0
                        codes = list(st.get("instrument_codes") or [])
                        code = ""
                        if 1 <= idx <= len(codes):
                            code = str(codes[idx - 1] or "").strip().upper()
                        if not code:
                            await api.send_message(user_id=int(sender_id), text="❌ Сессия сравнения устарела. Запустите сравнение заново.", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_anom", "instrument_code": code})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите порог аномалии в % (например 3). Уведомлю, если изменение за день будет ≥ порога.",
                                fmt="html",
                            )
                    note = "Ок"
                elif data.startswith("calc:pick:"):
                    if sender_id:
                        st = _state_get(int(sender_id))
                        opts = st.get("options")
                        try:
                            idx = int(data.split(":")[2])
                        except Exception:
                            idx = 0
                        if isinstance(opts, list) and 1 <= idx <= len(opts):
                            picked = opts[idx - 1]
                            st["picked"] = picked
                            st["stage"] = "calc_wait_volume"
                            _state_set(int(sender_id), st)
                            await api.send_message(
                                user_id=int(sender_id),
                                text=f"Выбрано: <b>{picked.get('label','—')}</b>\n\nВведите объём в тоннах (например 20):",
                                fmt="html",
                            )
                    note = "Ок"
                elif data == "calc:why":
                    if sender_id:
                        lr = _last_result.get(int(sender_id))
                        if not lr:
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Нет последнего расчёта. Нажмите «🏠 Меню» → «🧮 Расчёт».",
                                fmt="html",
                                attachments=_menu_attachments(),
                            )
                        else:
                            dist = float(lr.get("distance_km") or 0.0)
                            rate = float(lr.get("rate") or 0.0)
                            transport_type = str(lr.get("transport_type") or "")
                            is_sak = bool(lr.get("is_sakhalin"))
                            lines = [
                                "<b>🧾 Почему так</b>",
                                f"Назначение: <b>{lr.get('destination') or '—'}</b>",
                                f"Базис: <b>{lr.get('basis_label') or '—'}</b>",
                            ]
                            if transport_type == "rail":
                                ro = lr.get("rail_origin_station_name")
                                rd = lr.get("rail_dest_station_name")
                                if ro or rd:
                                    lines.append(f"🚉 Станция отпр.: <b>{ro or '—'}</b>")
                                    lines.append(f"🚉 Станция назн.: <b>{rd or '—'}</b>")
                                lines.append(f"📏 Расстояние (ж/д оценка/ТР4): <b>{_format_num(dist)} км</b>")
                                if is_sak:
                                    lines.append(
                                        f"⛴️ Паром Сахалин: <b>+{_format_num(sakhalin_ferry_surcharge_per_ton(True))} ₽/т</b> (включён в доставку)"
                                    )
                            else:
                                lines.append(f"📏 Расстояние (авто): <b>{_format_num(dist)} км</b>")
                                if is_sak:
                                    lines.append("🚛 Авто: <b>недоступно</b> — Сахалин считаем только по Ж/Д")
                            lines.extend(
                                [
                                    "",
                                    f"Цена на базисе: <b>{_format_num(float(lr.get('base_per_ton') or 0.0))} ₽/т</b>",
                                    f"Доставка: <b>{_format_num(float(lr.get('delivery_per_ton') or 0.0))} ₽/т</b>",
                                    f"Комиссия брокера: <b>{_format_num(float(lr.get('broker_commission_per_ton') or 0.0))} ₽/т</b>",
                                    f"Итого: <b>{_format_num(float(lr.get('total_per_ton') or 0.0))} ₽/т</b>",
                                    "",
                                    (
                                        f"Ставка доставки: <b>{rate:.2f} ₽/т·км</b>"
                                        if dist > 0 and rate > 0
                                        else "Ставка доставки: <b>—</b>"
                                    ),
                                    f"Объём: <b>{float(lr.get('volume_tons') or 0.0):g} т</b>",
                                    f"Сумма: <b>{_format_num(float(lr.get('total_sum') or 0.0))} ₽</b>",
                                ]
                            )
                            await api.send_message(
                                user_id=int(sender_id),
                                text="\n".join(lines),
                                fmt="html",
                                attachments=_why_keyboard(),
                            )
                    note = "Ок"
                elif data == "calc:kp":
                    if sender_id:
                        lr = _last_result.get(int(sender_id)) or {}
                        req_id = int(lr.get("request_id") or 0)
                        session = await get_session()
                        try:
                            u = (await session.execute(select(User).where(User.max_user_id == int(sender_id)).limit(1))).scalar_one_or_none()
                            email = (getattr(u, "email", None) or "").strip().lower() if u else ""
                            if not email:
                                _state_set(int(sender_id), {"stage": "max_login_wait_email", "after_login": "kp"})
                                await api.send_message(
                                    user_id=int(sender_id),
                                    text="Чтобы отправить КП, нужно войти по email.\nВведите email:",
                                    fmt="html",
                                )
                            else:
                                if not req_id:
                                    await api.send_message(user_id=int(sender_id), text="❌ Не нашёл расчёт в базе. Сделайте расчёт заново.", fmt="html")
                                else:
                                    ur = await session.get(UserRequest, int(req_id))
                                    if ur is None:
                                        await api.send_message(user_id=int(sender_id), text="❌ Запрос не найден в базе. Сделайте расчёт заново.", fmt="html")
                                    else:
                                        # Create lead if missing
                                        lead = (
                                            await session.execute(
                                                select(Lead).where(Lead.user_id == int(u.id), Lead.request_id == int(req_id)).limit(1)
                                            )
                                        ).scalar_one_or_none()
                                        if lead is None:
                                            lead = Lead(user_id=int(u.id), request_id=int(req_id), status="email_pending", source="max")
                                            session.add(lead)
                                            await session.commit()
                                        # send email (client + sales if configured)
                                        from bot.handlers import send_order_to_email
                                        await send_order_to_email(email, ur, session)
                                        lead.email = email
                                        lead.status = "sent"
                                        await session.commit()
                                        await api.send_message(
                                            user_id=int(sender_id),
                                            text=f"✅ Заявка на КП отправлена на <b>{email}</b>.\nМенеджер свяжется с вами в ближайшее время.",
                                            fmt="html",
                                            attachments=_menu_attachments(),
                                        )
                        finally:
                            await session.close()
                    note = "Ок"
                elif data == "subs:price:new":
                    if sender_id:
                        lr = _last_result.get(int(sender_id)) or {}
                        pid = int(lr.get("product_id") or 0)
                        bid = int(lr.get("basis_id") or 0)
                        cur = float(lr.get("total_per_ton") or 0.0)
                        if not pid or not bid:
                            await api.send_message(user_id=int(sender_id), text="Сначала сделайте расчёт, потом нажмите «Цена ↓».", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_price", "product_id": pid, "basis_id": bid})
                            await api.send_message(
                                user_id=int(sender_id),
                                text=f"Введите целевую цену ₽/т (когда цена станет ≤ неё — уведомлю).\nНапример: <b>{_format_num(cur)}</b>",
                                fmt="html",
                            )
                    note = "Ок"
                elif data == "subs:anom:new":
                    if sender_id:
                        lr = _last_result.get(int(sender_id)) or {}
                        code = str(lr.get("instrument_code") or "").strip().upper()
                        if not code:
                            await api.send_message(user_id=int(sender_id), text="Сначала сделайте расчёт (нужен код инструмента), потом «Аномалия».", fmt="html")
                        else:
                            _state_set(int(sender_id), {"stage": "subs_wait_anom", "instrument_code": code})
                            await api.send_message(
                                user_id=int(sender_id),
                                text="Введите порог аномалии в % (например 3). Уведомлю, если изменение за день будет ≥ порога.",
                                fmt="html",
                            )
                    note = "Ок"
                elif data == "subs:list":
                    if sender_id:
                        if not await _require_linked_email(api, sender_id=int(sender_id), after_login="subs"):
                            note = "Ок"
                            continue
                        # ensure user exists
                        cb_user = _get(upd, "callback.user", {}) or {}
                        u = await _get_or_create_max_user(
                            int(sender_id),
                            first_name=str(cb_user.get("first_name") or ""),
                            last_name=str(cb_user.get("last_name") or ""),
                        )
                        subs = await _list_subscriptions_for_user(u.id)
                        pa = subs["price"]
                        aa = subs["anom"]
                        dg = subs.get("digest") or []
                        tg = subs.get("table") or []
                        lines: list[str] = ["<b>Мои подписки</b>", ""]
                        if not pa and not aa and not dg and not tg:
                            lines.append("Пока нет активных подписок.")
                        if dg:
                            lines.append("<b>Ежедневная сводка</b>:")
                            sess = await get_session()
                            try:
                                for d in dg:
                                    b = await sess.get(Basis, int(d.basis_id))
                                    pr = (
                                        await sess.get(Product, int(d.product_id))
                                        if getattr(d, "product_id", None) and not getattr(d, "all_products", False)
                                        else None
                                    )
                                    sc = "все на базисе" if getattr(d, "all_products", False) else (pr.name if pr else "—")
                                    lines.append(f"• #{int(d.id)} {b.name if b else '—'} — {sc}")
                            finally:
                                await sess.close()
                            lines.append("")
                        if tg:
                            lines.append("<b>Таблица (биржа + доставка)</b>:")
                            for tsub in tg:
                                lines.append(f"• #{int(tsub.id)}")
                            lines.append("")
                        if pa:
                            lines.append("<b>Цена ↓</b>:")
                            for a in pa:
                                lines.append(
                                    f"• #{int(a.id)} product_id={int(a.product_id)} basis_id={int(a.basis_id or 0)} цель={float(a.target_price):,.0f} ₽/т".replace(",", " ")
                                )
                            lines.append("")
                        if aa:
                            lines.append("<b>Аномалия</b>:")
                            for a in aa:
                                lines.append(f"• #{int(a.id)} код={a.instrument_code} порог={float(a.threshold_pct):g}%")
                        await api.send_message(
                            user_id=int(sender_id),
                            text="\n".join(lines),
                            fmt="html",
                            attachments=_subs_list_keyboard(pa, aa, dg, tg),
                        )
                    note = "Ок"
                elif data.startswith("subs:price:off:"):
                    if sender_id:
                        aid = int(data.split(":")[3])
                        session = await get_session()
                        try:
                            al = await session.get(PriceAlert, aid)
                            if al:
                                al.is_active = False
                                await session.commit()
                        finally:
                            await session.close()
                        await api.send_message(user_id=int(sender_id), text="✅ Подписка на цену отключена.", fmt="html")
                    note = "Ок"
                elif data.startswith("subs:anom:off:"):
                    if sender_id:
                        aid = int(data.split(":")[3])
                        session = await get_session()
                        try:
                            al = await session.get(AnomalyAlert, aid)
                            if al:
                                al.is_active = False
                                await session.commit()
                        finally:
                            await session.close()
                        await api.send_message(user_id=int(sender_id), text="✅ Подписка на аномалию отключена.", fmt="html")
                    note = "Ок"
                elif data.startswith("subs:table:off:"):
                    if sender_id:
                        aid = int(data.split(":")[3])
                        cb_user = _get(upd, "callback.user", {}) or {}
                        u = await _get_or_create_max_user(
                            int(sender_id),
                            first_name=str(cb_user.get("first_name") or ""),
                            last_name=str(cb_user.get("last_name") or ""),
                        )
                        session = await get_session()
                        try:
                            al = await session.get(TableDigestSubscription, aid)
                            if al and int(al.user_id) == int(u.id):
                                al.is_active = False
                                await session.commit()
                                msg_off = "✅ Подписка на таблицу отключена."
                            else:
                                msg_off = "❌ Подписка не найдена."
                        finally:
                            await session.close()
                        await api.send_message(user_id=int(sender_id), text=msg_off, fmt="html")
                    note = "Ок"
                elif data.startswith("subs:digest:off:"):
                    if sender_id:
                        aid = int(data.split(":")[3])
                        cb_user = _get(upd, "callback.user", {}) or {}
                        u = await _get_or_create_max_user(
                            int(sender_id),
                            first_name=str(cb_user.get("first_name") or ""),
                            last_name=str(cb_user.get("last_name") or ""),
                        )
                        session = await get_session()
                        try:
                            al = await session.get(BasisDigestSubscription, aid)
                            if al and int(al.user_id) == int(u.id):
                                al.is_active = False
                                await session.commit()
                                msg_off = "✅ Подписка на сводку отключена."
                            else:
                                msg_off = "❌ Подписка не найдена."
                        finally:
                            await session.close()
                        await api.send_message(user_id=int(sender_id), text=msg_off, fmt="html")
                    note = "Ок"
                else:
                    note = "Ок."
                try:
                    await api.answer_callback(callback_id=callback_id, notification=note)
                except Exception:
                    logger.exception("Failed to answer callback sender_id=%s data=%s", sender_id, data)
            continue

    return {"ok": True}

