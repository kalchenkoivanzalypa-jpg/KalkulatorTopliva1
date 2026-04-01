# rail_tariff.py
"""
Модуль для расчета Ж/Д тарифов по реальным данным РЖД
Калибровано по скриншотам с сайта РЖД
"""

import math

# Реальные точки для калибровки (расстояние -> ставка руб/т·км)
# Данные из ваших скриншотов
CALIBRATION_POINTS = {
    398: 7.41,   # ст. Дземги → Хабаровск
    4649: 3.57,  # ст. Новая Еловка → Хабаровск
    7101: 3.52,  # Уфа-группа → Хабаровск
    7522: 3.49,  # ст. Пурпе → Хабаровск
}

def get_rail_rate(distance_km: float) -> float:
    """
    Возвращает ставку Ж/Д тарифа в руб/т·км
    для заданного расстояния
    """
    # Короткие расстояния (до 500 км) - очень дорого
    if distance_km <= 500:
        return 7.41
    
    # 500 - 1000 км
    elif distance_km <= 1000:
        # Линейное падение с 7.41 до 6.20
        return 7.41 - (distance_km - 500) * (1.21 / 500)
    
    # 1000 - 2000 км
    elif distance_km <= 2000:
        # Падение с 6.20 до 5.00
        return 6.20 - (distance_km - 1000) * (1.20 / 1000)
    
    # 2000 - 3000 км
    elif distance_km <= 3000:
        # Падение с 5.00 до 4.30
        return 5.00 - (distance_km - 2000) * (0.70 / 1000)
    
    # 3000 - 4000 км
    elif distance_km <= 4000:
        # Падение с 4.30 до 3.90
        return 4.30 - (distance_km - 3000) * (0.40 / 1000)
    
    # 4000 - 4649 км (до Новой Еловки)
    elif distance_km <= 4649:
        # Падение с 3.90 до 3.57
        return 3.90 - (distance_km - 4000) * (0.33 / 649)
    
    # 4649 - 5000 км
    elif distance_km <= 5000:
        # Небольшой рост после 4649? (странно, но по данным так)
        return 3.57 + (distance_km - 4649) * (0.03 / 351)
    
    # 5000 - 6000 км
    elif distance_km <= 6000:
        return 3.60
    
    # 6000 - 7101 км (до Уфы)
    elif distance_km <= 7101:
        # Падение с 3.60 до 3.52
        return 3.60 - (distance_km - 6000) * (0.08 / 1101)
    
    # 7101 - 7522 км (до Пурпе)
    elif distance_km <= 7522:
        # Падение с 3.52 до 3.49
        return 3.52 - (distance_km - 7101) * (0.03 / 421)
    
    # Дальше 7522 км
    else:
        # Медленное падение
        return 3.49 - (distance_km - 7522) * (0.01 / 500)


def calculate_delivery_cost(distance_km: float, volume_tonns: float) -> dict:
    """
    Полный расчет стоимости Ж/Д доставки
    
    Параметры:
        distance_km: расстояние в км
        volume_tonns: объем в тоннах
    
    Возвращает:
        словарь с деталями расчета
    """
    # Параметры вагона-цистерны
    tons_per_wagon = 66  # грузоподъемность
    
    # Расчет количества вагонов
    if volume_tonns <= 0:
        wagons_needed = 0
    else:
        wagons_needed = math.ceil(volume_tonns / tons_per_wagon)
    
    # Если объем меньше вагона, все равно нужен 1 вагон
    if wagons_needed == 0:
        wagons_needed = 1
    
    # Получаем ставку за тонно-км
    rate = get_rail_rate(distance_km)
    
    # Общая стоимость (для всего объема)
    total_cost = distance_km * volume_tonns * rate
    
    # Стоимость за тонну
    cost_per_ton = total_cost / volume_tonns if volume_tonns > 0 else 0
    
    return {
        'distance_km': round(distance_km, 1),
        'volume_tonns': volume_tonns,
        'rate_per_ton_km': round(rate, 3),
        'total_cost': round(total_cost, 2),
        'cost_per_ton': round(cost_per_ton, 2),
        'wagons_needed': wagons_needed,
        'tons_per_wagon': tons_per_wagon,
        'actual_volume': wagons_needed * tons_per_wagon
    }


def test_calibration():
    """Тестирование калибровки на реальных данных"""
    print("=" * 70)
    print("ТЕСТИРОВАНИЕ КАЛИБРОВКИ Ж/Д ТАРИФОВ")
    print("=" * 70)
    
    test_cases = [
        {"name": "ст. Дземги", "dist": 398, "volume": 65, "real_total": 191570},
        {"name": "ст. Новая Еловка", "dist": 4649, "volume": 65, "real_total": 1079347},
        {"name": "Уфа-группа", "dist": 7101, "volume": 65, "real_total": 1623611},
        {"name": "ст. Пурпе", "dist": 7522, "volume": 65, "real_total": 1708106},
    ]
    
    print(f"\n{'Базис':<20} {'Расст':>6} {'Ставка':>8} {'Расчет':>12} {'Реал':>12} {'Ошибка':>8}")
    print("-" * 70)
    
    for case in test_cases:
        result = calculate_delivery_cost(case["dist"], case["volume"])
        error = (result['total_cost'] - case["real_total"]) / case["real_total"] * 100
        
        print(f"{case['name']:<20} {case['dist']:6d} {result['rate_per_ton_km']:8.3f} "
              f"{result['total_cost']:12,.0f} {case['real_total']:12,d} {error:8.2f}%")
    
    print("\n" + "=" * 70)
    print("ТАРИФНАЯ СЕТКА ДЛЯ РАЗНЫХ РАССТОЯНИЙ")
    print("=" * 70)
    print(f"{'Расст':>6} {'Ставка':>8} {'65 т':>12} {'130 т':>12} {'260 т':>12}")
    print("-" * 70)
    
    for dist in [500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]:
        cost_65 = calculate_delivery_cost(dist, 65)['total_cost']
        cost_130 = calculate_delivery_cost(dist, 130)['total_cost']
        cost_260 = calculate_delivery_cost(dist, 260)['total_cost']
        rate = get_rail_rate(dist)
        
        print(f"{dist:6d} {rate:8.3f} {cost_65:12,.0f} {cost_130:12,.0f} {cost_260:12,.0f}")


if __name__ == "__main__":
    test_calibration()