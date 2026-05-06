"""Расчёт стоимости для веб (та же логика, что у бота)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
import asyncio

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError

from bot.handlers import calculate_distance, find_nearest_basises
from db.database import Basis, CityDestination, Product, RailStation, User, UserRequest
from rail_tariff import (
    calculate_delivery_cost as calculate_rail_delivery_cost,
    compute_rail_tariff_distance_km_cached,
)
from utils import canonical_fuel_display_name, get_coordinates_from_city, get_delivery_rate, normalize_city_name_key
from utils.rail_logistics import (
    basis_rail_origin_coords,
    basis_rail_origin_label,
    coords_from_rail_station,
    extract_first_esr_code,
    fixed_delivery_per_ton_override,
    find_rail_station_by_esr_code,
    find_rail_station_for_destination,
    is_sakhalin_geo_point,
    sakhalin_ferry_surcharge_total,
)

logger = logging.getLogger(__name__)

async def _bump_city_destination_usage_best_effort(session, dest_id: int) -> None:
    """
    Увеличить request_count/last_used, но не ломать расчёт при высокой параллельности SQLite.
    При нагрузке SQLite легко ловит database is locked на UPDATE — это не критично.
    """
    try:
        await session.execute(
            update(CityDestination)
            .where(CityDestination.id == int(dest_id))
            .values(request_count=CityDestination.request_count + 1)
        )
        await session.commit()
    except OperationalError:
        await session.rollback()
    except Exception:
        await session.rollback()


@dataclass
class CalcResult:
    request_id: int
    product_name: str
    basis_name: str
    destination_name: str
    volume: float
    transport: str
    distance_km: float
    base_price_per_ton: float
    base_total: float
    delivery_cost: float
    broker_commission_per_ton: float
    broker_commission_total: float
    total_price: float
    rate: float
    rail_leg_html: str
    wagons_info: str
    dist_str: str
    why_lines: list[str]


def broker_commission_per_ton(volume_tons: float) -> float:
    try:
        v = float(volume_tons)
    except Exception:
        v = 0.0
    if v >= 1000.0:
        return 150.0
    return 200.0


def serialize_basis_item(item: dict) -> dict[str, Any]:
    b = item["basis"]
    p = item["price"]
    return {
        "basis_id": b.id,
        "distance": float(item["distance"]),
        "total_cost_per_ton": float(item["total_cost_per_ton"]),
        "transport_type": item["transport_type"],
        "delivery_cost_per_ton": float(item["delivery_cost_per_ton"]),
        "rate": float(item["rate"]),
        "price_id": p.id,
        "rail_dest_station_id": item.get("rail_dest_station_id"),
        "rail_dest_station_name": item.get("rail_dest_station_name"),
        "rail_origin_station_name": item.get("rail_origin_station_name"),
        "is_sakhalin_destination": bool(item.get("is_sakhalin_destination")),
        "ferry_surcharge_per_ton": float(item.get("ferry_surcharge_per_ton") or 0),
    }


async def rebuild_selected(session, product_id: int, data: dict[str, Any]) -> Optional[dict]:
    """Восстанавливает структуру selected из сериализованного словаря."""
    from db.database import Basis, ProductBasisPrice

    basis = await session.get(Basis, int(data["basis_id"]))
    price = await session.get(ProductBasisPrice, int(data["price_id"]))
    if not basis or not price:
        return None
    return {
        "distance": float(data["distance"]),
        "basis": basis,
        "price": price,
        "transport_type": data["transport_type"],
        "rate": float(data["rate"]),
        "delivery_cost_per_ton": float(data["delivery_cost_per_ton"]),
        "total_cost_per_ton": float(data["total_cost_per_ton"]),
        "rail_dest_station_id": data.get("rail_dest_station_id"),
        "rail_dest_station_name": data.get("rail_dest_station_name"),
        "rail_origin_station_name": data.get("rail_origin_station_name"),
        "is_sakhalin_destination": data.get("is_sakhalin_destination"),
        "ferry_surcharge_per_ton": float(data.get("ferry_surcharge_per_ton") or 0),
    }


async def resolve_destination_to_id(
    session,
    destination: str,
) -> tuple[Optional[int], float, float, str, bool]:
    """
    Определяет CityDestination id и координаты (как в боте).
    Возвращает (dest_id, lat, lon, destination_key, is_station_input) или (None,...) при ошибке.
    """
    raw = (destination or "").strip()
    destination_key = normalize_city_name_key(raw)

    # Жёсткие канонические названия, чтобы не цеплять одноимённые пункты из кэша.
    canonical_display_name = None
    if destination_key == "свободный":
        canonical_display_name = "Свободный (Амурская область)"
    if destination_key == "ванино":
        canonical_display_name = "Ванино (Хабаровский край)"
    if destination_key == "артем":
        canonical_display_name = "Артём (Приморский край)"

    esr_hint = extract_first_esr_code(raw, destination_key)

    def _station_like(s: str) -> bool:
        ss = (s or "").strip().lower().replace("ё", "е")
        return any(ss.startswith(p) for p in ("ст.", "ст ", "станция ", "жд ", "ж/д "))

    # Пользователь может вводить станцию с префиксом "ст." — попробуем извлечь координаты по НП без префикса,
    # чтобы не привязываться к случайной станции из справочника.
    stripped = raw
    for prefix in ("ст.", "ст ", "станция ", "жд станция ", "ж/д станция "):
        low = stripped.lower()
        if low.startswith(prefix):
            stripped = stripped[len(prefix) :].strip()
            break

    # 1) Шестизначный ЕСР — до геокодера (избегаем ложных совпадений по city_destinations).
    coords: Optional[tuple[float, float]] = None
    is_station_input = False
    if esr_hint:
        st_esr = await find_rail_station_by_esr_code(session, esr_hint)
        c_esr = coords_from_rail_station(st_esr)
        if c_esr is not None:
            coords = c_esr
            is_station_input = True
        elif st_esr is not None:
            logger.warning(
                "ЕСР %s: станция id=%s в БД без latitude/longitude",
                esr_hint,
                getattr(st_esr, "id", None),
            )

    # 2) Если ввод похож на станцию — справочник по названию.
    if not coords and _station_like(raw):
        st_first = await find_rail_station_for_destination(session, raw, destination_key)
        c_st = coords_from_rail_station(st_first)
        if c_st is not None:
            coords = c_st
            is_station_input = True

    if not coords:
        coords = await get_coordinates_from_city(raw, session)
    if not coords and stripped and stripped != raw:
        coords = await get_coordinates_from_city(stripped, session)

    canonical_coords = None
    # Если координаты пришли из эталонного набора — запомним их как якорь,
    # чтобы случайная станция с тем же названием не «увела» в другой регион.
    try:
        from utils.utils import CANONICAL_CITY_COORDS

        if destination_key in CANONICAL_CITY_COORDS:
            canonical_coords = CANONICAL_CITY_COORDS[destination_key]
    except Exception:
        canonical_coords = None

    # 3) Даже если координаты нашлись по НП, но в справочнике есть станция с таким же названием —
    # приоритет отдаём станции (иначе "Пурпе" может привязаться к другому одноимённому пункту).
    st = await find_rail_station_for_destination(session, raw, destination_key)
    if st is not None and normalize_city_name_key(getattr(st, "name", "") or "") == destination_key:
        c_nm = coords_from_rail_station(st)
        if c_nm is not None:
            if canonical_coords is not None:
                # Защита от неверных координат станций (пример: Ванино в старой БД могло иметь координаты не ДВ).
                try:
                    gap_km = float(
                        calculate_distance(
                            float(canonical_coords[0]),
                            float(canonical_coords[1]),
                            float(c_nm[0]),
                            float(c_nm[1]),
                        )
                    )
                except Exception:
                    gap_km = 10**9
                if gap_km <= 80.0:
                    coords = c_nm
                    is_station_input = True
            else:
                coords = c_nm
                is_station_input = True

    if not coords:
        dest_station_for_coords = await find_rail_station_for_destination(
            session,
            raw,
            destination_key,
        )
        if dest_station_for_coords is None:
            return None, 0.0, 0.0, destination_key, False
        c_dest = coords_from_rail_station(dest_station_for_coords)
        if c_dest is None:
            logger.warning(
                "Станция для назначения без координат в БД (id=%s)",
                getattr(dest_station_for_coords, "id", None),
            )
            return None, 0.0, 0.0, destination_key, False
        dest_lat, dest_lon = c_dest
        is_station_input = True
    else:
        dest_lat, dest_lon = coords

    # Для канонических ключей предпочитаем уже существующую запись по имени.
    # Важно: не переименовываем "чужие" записи, чтобы не словить UNIQUE(name).
    if canonical_display_name:
        by_name = await session.execute(
            select(CityDestination).where(CityDestination.name == canonical_display_name).limit(1)
        )
        dest_obj = by_name.scalar_one_or_none()
        if dest_obj is not None:
            dest_id = int(dest_obj.id)
            await _bump_city_destination_usage_best_effort(session, dest_id)
            return dest_id, dest_lat, dest_lon, destination_key, is_station_input

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

    # Важно: для канонических ключей (например «Свободный») НЕ делаем матч по имени,
    # иначе можем подтянуть «Свободный (Кемеровская область)» с другими координатами.
    if not dest_obj and canonical_display_name is None:
        result = await session.execute(
            select(CityDestination).where(
                func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е") == destination_key
            )
        )
        dest_obj = result.scalar_one_or_none()

    if not dest_obj and canonical_display_name is None:
        # ilike «%архангельск%» может подтянуть чужую строку (область, село).
        # Берём только записи с тем же нормализованным именем и ближайшие к уже найденным координатам.
        result = await session.execute(
            select(CityDestination)
            .where(
                func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                    f"%{destination_key}%"
                )
            )
            .order_by(CityDestination.request_count.desc())
            .limit(80)
        )
        cands = [c for c in result.scalars().all() if c is not None and c.name]
        exact = [c for c in cands if normalize_city_name_key(c.name) == destination_key]
        if exact:

            def _gap_m(c: CityDestination) -> float:
                try:
                    return float(
                        calculate_distance(
                            float(dest_lat),
                            float(dest_lon),
                            float(c.latitude),
                            float(c.longitude),
                        )
                    )
                except Exception:
                    return 1e9

            dest_obj = min(
                exact,
                key=lambda c: (_gap_m(c), -int(getattr(c, "request_count", 0) or 0)),
            )

    if dest_obj:
        dest_id = dest_obj.id
        await _bump_city_destination_usage_best_effort(session, int(dest_id))
    else:
        new_dest = CityDestination(
            name=canonical_display_name or destination,
            latitude=dest_lat,
            longitude=dest_lon,
        )
        session.add(new_dest)
        try:
            await session.commit()
            await session.refresh(new_dest)
            dest_id = new_dest.id
            logger.info("Добавлен новый город (web): %s", destination)
        except IntegrityError:
            await session.rollback()
            # При UNIQUE(name) конфликте: берём существующую запись.
            # Важно: в SQLite lower()/ilike плохо работают с кириллицей, поэтому:
            # 1) сначала пробуем точное совпадение имени, которое пытались вставить,
            # 2) затем безопасный поиск через case-sensitive LIKE + Python-нормализацию.
            probe_name = canonical_display_name or destination
            again_obj = None

            # 1) Точное совпадение имени
            again = await session.execute(
                select(CityDestination).where(CityDestination.name == probe_name).limit(1)
            )
            again_obj = again.scalar_one_or_none()

            # 2) Фолбек: ищем кандидатов по LIKE и фильтруем в Python по normalize_city_name_key
            if again_obj is None and destination_key:
                like_probe = probe_name.strip()
                again2 = await session.execute(
                    select(CityDestination)
                    .where(CityDestination.name.like(f"%{like_probe}%"))
                    .order_by(CityDestination.request_count.desc())
                    .limit(200)
                )
                cands = [c for c in again2.scalars().all() if c is not None and getattr(c, "name", None)]
                for c in cands:
                    if normalize_city_name_key(getattr(c, "name", "") or "") == destination_key:
                        again_obj = c
                        break

            if not again_obj:
                # Если всё равно не нашли — пусть падает, чтобы увидеть редкий кейс.
                raise

            dest_id = int(again_obj.id)

    return dest_id, dest_lat, dest_lon, destination_key, is_station_input


async def finalize_calculation(
    session,
    *,
    user: User,
    product_id: int,
    destination_id: int,
    selected: dict,
    volume: float,
) -> CalcResult:
    """Создаёт UserRequest и возвращает данные для страницы результата."""
    product = await session.get(Product, product_id)
    basis = selected["basis"]
    destination = await session.get(CityDestination, destination_id)
    product_price = selected["price"]
    if not product or not destination:
        raise ValueError("Нет продукта или назначения")

    distance_km = selected["distance"]
    final_transport = selected["transport_type"]

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
        try:
            distance_km = await asyncio.to_thread(
                compute_rail_tariff_distance_km_cached,
                o_lat,
                o_lon,
                d_lat,
                d_lon,
                (str(basis.rail_esr).strip() if getattr(basis, "rail_esr", None) else None),
                (
                    str(dest_station.esr_code).strip()
                    if dest_station and getattr(dest_station, "esr_code", None)
                    else None
                ),
            )
        except Exception as exc:
            # TR4_STRICT: this should not happen if the basis list was built successfully; keep selection distance.
            logger.warning("TR4 finalize failed for basis=%s: %s", getattr(basis, "name", None), exc)
            distance_km = float(selected.get("distance") or 0.0)
            if distance_km <= 0:
                raise

    if final_transport == "rail":
        rail_result = calculate_rail_delivery_cost(
            distance_km,
            volume,
            (product.name if product else None),
        )
        fixed_pt = fixed_delivery_per_ton_override(
            getattr(basis, "name", "") or "",
            normalize_city_name_key(getattr(destination, "name", "") or ""),
        )
        if fixed_pt is not None:
            delivery_cost = float(fixed_pt) * float(volume)
            rate = float(fixed_pt) / float(distance_km) if float(distance_km) > 0 else 0.0
        else:
            delivery_cost = rail_result["total_cost"]
        ferry_surcharge_total = sakhalin_ferry_surcharge_total(
            volume,
            bool(selected.get("is_sakhalin_destination")),
        )
        delivery_cost += ferry_surcharge_total
        if fixed_pt is None:
            rate = rail_result["rate_per_ton_km"]
        wagons_info = (
            f"Вагонов: {rail_result['wagons_needed']} (по {rail_result['tons_per_wagon']} т)"
        )
    else:
        rate = await get_delivery_rate(distance_km, final_transport, session)
        delivery_cost = distance_km * volume * rate
        wagons_info = ""

    base_total = product_price.current_price * volume
    comm_pt = broker_commission_per_ton(volume)
    comm_total = comm_pt * volume
    total_price = base_total + delivery_cost + comm_total

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
        total_price=total_price,
    )
    session.add(user_request)
    await session.commit()
    await session.refresh(user_request)

    if distance_km < 10:
        dist_str = f"{distance_km:.1f}"
    else:
        dist_str = f"{distance_km:.0f}"

    rail_leg_html = ""
    if final_transport == "rail":
        rs_name = selected.get("rail_dest_station_name")
        ro_name = selected.get("rail_origin_station_name") or basis_rail_origin_label(basis)
        if rs_name or ro_name:
            rail_leg_html = (
                f"<p><b>Станция отпр.:</b> {ro_name or '—'}<br/>"
                f"<b>Станция назн.:</b> {rs_name or '—'}</p>"
            )

    # Почему так?
    why_lines: list[str] = []
    if final_transport == "rail":
        why_lines.append("Расчёт по Ж/Д: стоимость доставки зависит от расстояния и объёма (вагонная модель).")
        if bool(selected.get("is_sakhalin_destination")):
            try:
                per_ton = float(ferry_surcharge_total) / float(volume)
                why_lines.append(
                    f"Сахалин: к Ж/Д доставке добавлена паромная надбавка {per_ton:,.0f} ₽/т.".replace(",", " ")
                )
                why_lines.append("Для Сахалина авто-доставка не предлагается.")
            except Exception:
                why_lines.append("Сахалин: к Ж/Д доставке добавлена паромная надбавка.")
    else:
        why_lines.append("Авто-доставка: расстояние по прямой, доставка = расстояние × ставка.")
    why_lines.append(
        f"Комиссия брокера ООО «НК-Востокнефтепродукт»: {comm_pt:,.0f} ₽/т.".replace(",", " ")
    )

    return CalcResult(
        request_id=user_request.id,
        product_name=canonical_fuel_display_name(product.name),
        basis_name=basis.name,
        destination_name=destination.name,
        volume=volume,
        transport="Ж/Д" if final_transport == "rail" else "Авто",
        distance_km=float(distance_km),
        base_price_per_ton=float(product_price.current_price),
        base_total=float(base_total),
        delivery_cost=float(delivery_cost),
        broker_commission_per_ton=float(comm_pt),
        broker_commission_total=float(comm_total),
        total_price=float(total_price),
        rate=float(rate),
        rail_leg_html=rail_leg_html,
        wagons_info=wagons_info,
        dist_str=dist_str,
        why_lines=why_lines,
    )


async def build_calc_result_from_user_request(session, ur: UserRequest) -> CalcResult:
    """
    Восстановление CalcResult для GET /calc/result (PRG).
    Мы используем сохранённые поля UserRequest и подтягиваем имена сущностей.
    """
    product = await session.get(Product, int(ur.product_id))
    basis = await session.get(Basis, int(ur.basis_id))
    destination = await session.get(CityDestination, int(ur.city_destination_id))

    volume = float(getattr(ur, "volume", 0.0) or 0.0)
    distance_km = float(getattr(ur, "distance_km", 0.0) or 0.0)
    delivery_cost = float(getattr(ur, "delivery_cost", 0.0) or 0.0)
    total_price = float(getattr(ur, "total_price", 0.0) or 0.0)
    base_price = float(getattr(ur, "base_price", 0.0) or 0.0)
    base_total = base_price * volume
    comm_pt = broker_commission_per_ton(volume)
    comm_total = comm_pt * volume
    transport_type = str(getattr(ur, "transport_type", "") or "")

    # примерная ставка (чтобы UI не ломался). Для Ж/Д это не «официальная» ставка, но даст ориентир.
    rate = 0.0
    if distance_km > 0 and volume > 0:
        try:
            rate = float(delivery_cost) / float(distance_km * volume)
        except Exception:
            rate = 0.0

    dist_str = f"{distance_km:.1f}" if distance_km < 10 else f"{distance_km:.0f}"

    why_lines: list[str] = []
    if (transport_type or "").lower() == "rail":
        why_lines.append("Расчёт по Ж/Д: стоимость доставки зависит от расстояния и объёма (вагонная модель).")
        # При восстановлении результата (PRG) не всегда есть исходные флаги selected,
        # поэтому определяем Сахалин по координатам назначения.
        try:
            if destination is not None and is_sakhalin_geo_point(float(destination.latitude), float(destination.longitude)):
                ferry_total = sakhalin_ferry_surcharge_total(volume, True)
                if ferry_total and volume > 0:
                    per_ton = float(ferry_total) / float(volume)
                    why_lines.append(
                        f"Сахалин: к Ж/Д доставке добавлена паромная надбавка {per_ton:,.0f} ₽/т.".replace(
                            ",", " "
                        )
                    )
                else:
                    why_lines.append("Сахалин: к Ж/Д доставке добавлена паромная надбавка.")
                why_lines.append("Для Сахалина авто-доставка не предлагается.")
        except Exception:
            pass
    else:
        why_lines.append("Авто-доставка: расстояние по прямой, доставка = расстояние × ставка.")
    why_lines.append(
        f"Комиссия брокера ООО «НК-Востокнефтепродукт»: {comm_pt:,.0f} ₽/т.".replace(",", " ")
    )

    return CalcResult(
        request_id=int(getattr(ur, "id", 0) or 0),
        product_name=canonical_fuel_display_name(product.name) if product else "—",
        basis_name=basis.name if basis else "—",
        destination_name=destination.name if destination else "—",
        volume=volume,
        transport="Ж/Д" if (transport_type or "").lower() == "rail" else "Авто",
        distance_km=distance_km,
        base_price_per_ton=base_price,
        base_total=float(base_total),
        delivery_cost=delivery_cost,
        broker_commission_per_ton=float(comm_pt),
        broker_commission_total=float(comm_total),
        total_price=total_price,
        rate=float(rate),
        rail_leg_html="",
        wagons_info="",
        dist_str=dist_str,
        why_lines=why_lines,
    )
