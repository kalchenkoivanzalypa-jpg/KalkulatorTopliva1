#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Универсальный калькулятор расстояний
Объединяет OSRM для авто и Chaika для Ж/Д
"""

import logging
import math
from typing import Tuple, Optional, Dict, Any
from datetime import datetime

from road_calculator import RoadCalculator
from rail_calculator import RailCalculator
from station_codes import StationCodeManager
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_air_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Расчет расстояния по прямой (формула гаверсинуса)
    Используется как fallback
    """
    R = 6371  # радиус Земли в км
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


class DistanceCalculator:
    """
    Универсальный калькулятор расстояний
    Поддерживает:
    - Реальные дороги через OSRM
    - Реальные Ж/Д через Chaika
    - Fallback с коэффициентами
    """
    
    def __init__(self, session=None):
        self.session = session
        self.road_calc = RoadCalculator()
        self.rail_calc = RailCalculator(
            host=config.CHAIKA_HOST,
            port=config.CHAIKA_PORT
        ) if config.USE_REAL_RAIL else None
        self.station_manager = StationCodeManager(session)
        
        # Коэффициенты для fallback
        self.rail_coeffs = [
            (500, config.RAIL_COEFF_500),
            (1000, config.RAIL_COEFF_1000),
            (2000, config.RAIL_COEFF_2000),
            (3000, config.RAIL_COEFF_3000),
            (4000, config.RAIL_COEFF_4000),
            (float('inf'), config.RAIL_COEFF_MAX),
        ]
        self.auto_coeff = config.AUTO_COEFF
    
    def get_rail_coefficient(self, air_distance: float) -> float:
        """Получает коэффициент для ж/д на основе расстояния по прямой"""
        for limit, coeff in self.rail_coeffs:
            if air_distance < limit:
                return coeff
        return 1.75
    
    async def get_road_distance(
        self,
        lat1: float, lon1: float,
        lat2: float, lon2: float
    ) -> Tuple[float, str]:
        """
        Получает расстояние по автодорогам
        
        Returns:
            (расстояние в км, источник)
            источник: 'osrm', 'estimated'
        """
        air_distance = calculate_air_distance(lat1, lon1, lat2, lon2)
        
        if not config.USE_REAL_ROADS:
            # Используем только коэффициенты
            estimated = air_distance * self.auto_coeff
            logger.info(f"🚛 Коэффициентный режим: {air_distance:.0f} * {self.auto_coeff} = {estimated:.0f} км")
            return estimated, 'estimated'
        
        # Пробуем OSRM
        distance, source = await self.road_calc.get_road_distance_with_fallback(lat1, lon1, lat2, lon2)
        
        if distance:
            logger.info(f"🚛 OSRM: {distance:.0f} км (по прямой: {air_distance:.0f} км, коэфф: {distance/air_distance:.2f})")
            return distance, source
        
        # Fallback: используем коэффициенты
        estimated = air_distance * self.auto_coeff
        logger.warning(f"⚠️ OSRM недоступен, fallback: {air_distance:.0f} * {self.auto_coeff} = {estimated:.0f} км")
        return estimated, 'estimated'
    
    async def get_rail_distance(
        self,
        basis_name: str,
        dest_city_name: str,
        product_name: str,
        volume: float,
        lat1: float, lon1: float,
        lat2: float, lon2: float
    ) -> Tuple[float, str, Optional[Dict]]:
        """
        Получает расстояние по Ж/Д
        
        Returns:
            (расстояние в км, источник, дополнительная информация)
            источник: 'chaika', 'estimated'
        """
        air_distance = calculate_air_distance(lat1, lon1, lat2, lon2)
        
        if not config.USE_REAL_RAIL or not self.rail_calc:
            # Используем только коэффициенты
            coeff = self.get_rail_coefficient(air_distance)
            estimated = air_distance * coeff
            logger.info(f"🚂 Коэффициентный режим: {air_distance:.0f} * {coeff} = {estimated:.0f} км")
            return estimated, 'estimated', None
        
        # Получаем коды станций
        from_code = self.station_manager.get_code(basis_name)
        to_code = self.station_manager.get_code(dest_city_name)
        
        # Если нет кодов, пробуем найти в БД
        if not from_code and self.session:
            from_code = await self.station_manager.search_station_in_db(basis_name)
        
        if not to_code and self.session:
            to_code = await self.station_manager.search_station_in_db(dest_city_name)
        
        if not from_code or not to_code:
            logger.warning(f"⚠️ Нет кодов станций: {basis_name} ({from_code}) → {dest_city_name} ({to_code})")
            # Fallback на коэффициенты
            coeff = self.get_rail_coefficient(air_distance)
            estimated = air_distance * coeff
            return estimated, 'estimated_no_codes', None
        
        # Проверяем доступность Chaika сервера
        server_ok = await self.rail_calc.check_server_status()
        if not server_ok:
            logger.warning("⚠️ Chaika сервер недоступен, fallback")
            coeff = self.get_rail_coefficient(air_distance)
            estimated = air_distance * coeff
            return estimated, 'estimated_no_server', None
        
        # Получаем информацию о маршруте
        route_info = await self.rail_calc.get_route_info(from_code, to_code)
        
        if route_info and route_info.get('distance'):
            distance = route_info['distance']
            logger.info(f"🚂 Chaika: {distance} км")
            
            # Пробуем получить тариф
            cargo_code = self.station_manager.get_cargo_code(product_name)
            tariff = await self.rail_calc.get_tariff(from_code, to_code, cargo_code, volume)
            
            extra_info = {
                'route_info': route_info,
                'tariff': tariff
            }
            
            return distance, 'chaika', extra_info
        
        # Fallback на коэффициенты
        coeff = self.get_rail_coefficient(air_distance)
        estimated = air_distance * coeff
        logger.warning(f"⚠️ Chaika не вернул расстояние, fallback: {air_distance:.0f} * {coeff} = {estimated:.0f} км")
        return estimated, 'estimated_no_data', None


async def test_calculator():
    """Тестирование калькулятора"""
    
    calc = DistanceCalculator()
    
    print("\n" + "=" * 60)
    print("🧪 ТЕСТИРОВАНИЕ DISTANCE CALCULATOR")
    print("=" * 60)
    
    # Тестовые маршруты
    routes = [
        ("Москва → Питер", 55.7558, 37.6176, 59.9343, 30.3351),
        ("Пурпе → Хабаровск", 64.4869, 76.6809, 48.4802, 135.0719),
    ]
    
    for name, lat1, lon1, lat2, lon2 in routes:
        print(f"\n📍 {name}")
        
        # Тест для авто
        road_dist, road_src = await calc.get_road_distance(lat1, lon1, lat2, lon2)
        print(f"   🚛 Авто ({road_src}): {road_dist:.0f} км")
        
        # Тест для ж/д
        rail_dist, rail_src, extra = await calc.get_rail_distance(
            "Пурпе", "Хабаровск", "ДТ", 60,
            lat1, lon1, lat2, lon2
        )
        print(f"   🚂 Ж/Д ({rail_src}): {rail_dist:.0f} км")
        
        # По прямой для сравнения
        air = calculate_air_distance(lat1, lon1, lat2, lon2)
        print(f"   ✈️ По прямой: {air:.0f} км")


if __name__ == "__main__":
    asyncio.run(test_calculator())