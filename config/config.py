import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Класс конфигурации"""
    
    # Telegram
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN не найден в .env файле!")
    
    # База данных
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///fuel_bot.db')
    
    # Настройки проверки цен
    CHECK_INTERVAL_MINUTES = int(os.getenv('CHECK_INTERVAL_MINUTES', 60))
    
    # API ключи
    YANDEX_MAPS_API_KEY = os.getenv('YANDEX_MAPS_API_KEY', None)
    GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', None)
    
    # Настройки бота
    BOT_USERNAME = os.getenv('BOT_USERNAME', 'fuel_price_bot')
    
    # Цены по умолчанию для доставки
    DEFAULT_AUTO_RATE = 5.5
    DEFAULT_RAIL_RATE = 2.8
    
    # Настройки калькулятора расстояний
    USE_REAL_ROADS = True           # Использовать OSRM для авто
    USE_REAL_RAIL = True            # Использовать Chaika для Ж/Д
    CHAIKA_HOST = os.getenv('CHAIKA_HOST', 'localhost')
    CHAIKA_PORT = int(os.getenv('CHAIKA_PORT', '50051'))
    
    # Коэффициенты для fallback (если API не работают)
    RAIL_COEFF_500 = 1.2    # до 500 км: +20%
    RAIL_COEFF_1000 = 1.35  # до 1000 км: +35%
    RAIL_COEFF_2000 = 1.45  # до 2000 км: +45%
    RAIL_COEFF_3000 = 1.55  # до 3000 км: +55%
    RAIL_COEFF_4000 = 1.65  # до 4000 км: +65%
    RAIL_COEFF_MAX = 1.75   # дальше: +75%
    
    AUTO_COEFF = 1.3  # для авто запас 30%

config = Config()