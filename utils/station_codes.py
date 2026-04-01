#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Справочник кодов станций ЕСР
Нужно постепенно наполнять по мере использования
"""

import logging

logger = logging.getLogger(__name__)

# Коды станций в формате ЕСР (Единая сетевая разметка)
STATION_CODES = {
    # Крупные города
    "Москва": "2000000",
    "Санкт-Петербург": "2004000",
    "Новосибирск": "2044000",
    "Екатеринбург": "2030000",
    "Казань": "2060000",
    "Нижний Новгород": "2060000",
    "Самара": "2024000",
    "Омск": "2044000",
    "Челябинск": "2040000",
    "Ростов-на-Дону": "2064000",
    "Уфа": "2025000",
    "Красноярск": "2038000",
    "Пермь": "2031000",
    "Воронеж": "2014000",
    "Волгоград": "2020000",
    
    # Ваши базисы
    "Кстово": "2060090",
    "Ярославль": "2010000",
    "Рязань": "2018000",
    "Кириши": "2004680",
    "Дземги": "2034090",  # код нужно уточнить
    "Новая Еловка": "2034091",  # код нужно уточнить
    "Уфа-группа": "2025000",
    "Омск": "2044000",
    "Сургут": "2036000",
    "Ангарск": "2038000",
    "Самара-группа": "2024000",
    "Саратов-группа": "2020000",
    "Волгоград-группа": "2020000",
}

# Коды грузов по ЕТСНГ
CARGO_CODES = {
    "ДТ": "211008",  # Дизельное топливо
    "АИ-92": "211001",  # Бензин
    "АИ-95": "211001",  # Бензин
    "Мазут": "211015",  # Мазут
    "Реактивное": "211004",  # Керосин
}


class StationCodeManager:
    """Менеджер для работы с кодами станций"""
    
    def __init__(self, session=None):
        self.session = session
    
    def get_code(self, station_name: str) -> str:
        """Получить код станции по названию"""
        # Точное совпадение
        if station_name in STATION_CODES:
            return STATION_CODES[station_name]
        
        # Поиск по подстроке
        for name, code in STATION_CODES.items():
            if name.lower() in station_name.lower():
                logger.info(f"✅ Найден код для '{station_name}' через '{name}'")
                return code
        
        logger.warning(f"⚠️ Код не найден для станции: {station_name}")
        return None
    
    def get_cargo_code(self, product_name: str) -> str:
        """Получить код груза по названию продукта"""
        for key, code in CARGO_CODES.items():
            if key.lower() in product_name.lower():
                return code
        return CARGO_CODES["ДТ"]  # по умолчанию
    
    async def search_station_in_db(self, station_name: str):
        """Поиск станции в базе данных (если есть своя таблица)"""
        if not self.session:
            return None
        
        from sqlalchemy import select
        from db.database import Basis
        
        result = await self.session.execute(
            select(Basis).where(Basis.city.ilike(f"%{station_name}%"))
        )
        basis = result.scalar_one_or_none()
        
        if basis and hasattr(basis, 'railway_code'):
            return basis.railway_code
        
        return None


# Тестирование
def test_codes():
    print("📋 ТЕСТИРОВАНИЕ КОДОВ СТАНЦИЙ")
    print("=" * 60)
    
    manager = StationCodeManager()
    
    test_stations = [
        "Москва",
        "ст. Кириши",
        "Дземги",
        "Уфа-группа",
        "Неизвестная станция"
    ]
    
    for station in test_stations:
        code = manager.get_code(station)
        if code:
            print(f"✅ {station}: {code}")
        else:
            print(f"❌ {station}: не найден")


if __name__ == "__main__":
    test_codes()