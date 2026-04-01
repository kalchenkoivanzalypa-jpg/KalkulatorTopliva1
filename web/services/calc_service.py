"""Расчёт стоимости для веб (та же логика, что у бота)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from bot.handlers import find_nearest_basises
from db.database import CityDestination, Product, RailStation, User, UserRequest
from rail_tariff import calculate_delivery_cost as calculate_rail_delivery_cost, compute_rail_tariff_distance_km
from utils import canonical_fuel_display_name, get_coordinates_from_city, get_delivery_rate, normalize_city_name_key
from utils.rail_logistics import (
    basis_rail_origin_coords,
    basis_rail_origin_label,
    find_rail_station_for_destination,
    is_sakhalin_geo_point,
    sakhalin_ferry_surcharge_total,
)

logger = logging.getLogger(__name__)


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
    total_price: float
    rate: float
    rail_leg_html: str
    wagons_info: str
    dist_str: str


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
) -> tuple[Optional[int], float, float, str]:
    """
    Определяет CityDestination id и координаты (как в боте).
    Возвращает (dest_id, lat, lon, destination_key) или (None,...) при ошибке.
    """
    destination_key = normalize_city_name_key(destination)
    coords = await get_coordinates_from_city(destination, session)

    if not coords:
        dest_station_for_coords = await find_rail_station_for_destination(
            session,
            destination,
            destination_key,
        )
        if dest_station_for_coords is None:
            return None, 0.0, 0.0, destination_key
        dest_lat = float(dest_station_for_coords.latitude)
        dest_lon = float(dest_station_for_coords.longitude)
    else:
        dest_lat, dest_lon = coords

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

    if not dest_obj:
        result = await session.execute(
            select(CityDestination).where(
                func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е") == destination_key
            )
        )
        dest_obj = result.scalar_one_or_none()

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
        await session.execute(
            update(CityDestination)
            .where(CityDestination.id == dest_id)
            .values(request_count=CityDestination.request_count + 1)
        )
        await session.commit()
    else:
        new_dest = CityDestination(
            name=destination,
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
            again = await session.execute(
                select(CityDestination).where(
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е") == destination_key
                ).limit(1)
            )
            again_obj = again.scalar_one_or_none()
            if not again_obj:
                raise
            dest_id = again_obj.id

    return dest_id, dest_lat, dest_lon, destination_key


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
        distance_km = compute_rail_tariff_distance_km(
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

    if final_transport == "rail":
        rail_result = calculate_rail_delivery_cost(distance_km, volume)
        delivery_cost = rail_result["total_cost"]
        ferry_surcharge_total = sakhalin_ferry_surcharge_total(
            volume,
            bool(selected.get("is_sakhalin_destination")),
        )
        delivery_cost += ferry_surcharge_total
        rate = rail_result["rate_per_ton_km"]
        wagons_info = (
            f"Вагонов: {rail_result['wagons_needed']} (по {rail_result['tons_per_wagon']} т)"
        )
    else:
        rate = await get_delivery_rate(distance_km, final_transport, session)
        delivery_cost = distance_km * volume * rate
        wagons_info = ""

    base_total = product_price.current_price * volume
    total_price = base_total + delivery_cost

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
        total_price=float(total_price),
        rate=float(rate),
        rail_leg_html=rail_leg_html,
        wagons_info=wagons_info,
        dist_str=dist_str,
    )
