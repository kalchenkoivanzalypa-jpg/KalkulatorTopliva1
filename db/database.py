# -*- coding: utf-8 -*-

import os
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, BigInteger, Text, Index, UniqueConstraint
from sqlalchemy.sql import func
from dotenv import load_dotenv

load_dotenv()

# Создание движка базы данных
engine = create_async_engine(
    os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///fuel_bot.db'),
    echo=False,
)

# Фабрика сессий
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Базовый класс для моделей
Base = declarative_base()


class User(Base):
    """Модель пользователя Telegram"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    # Для веб-пользователей после входа по email может быть NULL; гостям — отрицательный synthetic id
    telegram_id = Column(BigInteger, unique=True, nullable=True, index=True)
    username = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_activity = Column(DateTime(timezone=True), onupdate=func.now())


class Product(Base):
    """Справочник продуктов (видов топлива)"""
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), nullable=True)
    unit = Column(String(20), default="тонна")
    last_updated = Column(DateTime(timezone=True), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<Product {self.name}>"


class Basis(Base):
    __tablename__ = 'basis'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    city = Column(String, nullable=False)
    # Координаты площадки/НПП (для поиска «ближайший базис», авто, ориентиры)
    latitude = Column(Float)
    longitude = Column(Float)
    is_active = Column(Boolean, default=True)
    transport_type = Column(String(10), default='rail')  # 'auto' или 'rail'
    # Ж/Д: станция отгрузки (если в Excel/PDF не было — можно оставить NULL и использовать lat/lon базиса)
    rail_station_name = Column(String(300), nullable=True)
    rail_esr = Column(String(32), nullable=True, index=True)  # код ЕСР/станции для ТР №4
    rail_latitude = Column(Float, nullable=True)
    rail_longitude = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RailStation(Base):
    """Справочник ж/д станций (для привязки населённого пункта к сети и расчёта ТР №4)."""
    __tablename__ = 'rail_stations'

    id = Column(Integer, primary_key=True)
    name = Column(String(300), nullable=False)
    esr_code = Column(String(32), nullable=True, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    # Населённый пункт рядом со станцией (для поиска по названию города)
    settlement_name = Column(String(200), nullable=True, index=True)
    region = Column(String(200), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<RailStation {self.name}>"


class ProductBasisPrice(Base):
    """Цена продукта на конкретном базисе (одна строка = один код инструмента бюллетеня)."""
    __tablename__ = 'product_basis_prices'
    
    id = Column(Integer, primary_key=True)
    # Код инструмента СПбМТСБ (например A001CUU025A) — ключ обновления цены из PDF
    instrument_code = Column(String(48), nullable=True, unique=True, index=True)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    basis_id = Column(Integer, ForeignKey('basis.id'), nullable=False)
    current_price = Column(Float, nullable=False)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<Price {self.instrument_code or '-'} {self.product_id}@{self.basis_id}: {self.current_price}>"


class DeliveryCoefficient(Base):
    """Коэффициенты доставки (руб/тонна-км)"""
    __tablename__ = 'delivery_coefficients'
    
    id = Column(Integer, primary_key=True)
    transport_type = Column(String(50), nullable=False)
    rate_per_ton_km = Column(Float, nullable=False)
    min_distance = Column(Float, default=0)
    max_distance = Column(Float, nullable=True)
    valid_from = Column(DateTime, nullable=True)
    valid_to = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<DeliveryCoeff {self.transport_type}: {self.rate_per_ton_km}>"


class CityDestination(Base):
    """Кэш городов назначения с координатами"""
    __tablename__ = 'city_destinations'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, unique=True, index=True)
    region = Column(String(100), nullable=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    request_count = Column(Integer, default=0)
    last_used = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<City {self.name}>"


class UserRequest(Base):
    """История запросов пользователей"""
    __tablename__ = 'user_requests'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    basis_id = Column(Integer, ForeignKey('basis.id'), nullable=False)
    price_id = Column(Integer, ForeignKey('product_basis_prices.id'), nullable=True)
    city_destination_id = Column(Integer, ForeignKey('city_destinations.id'), nullable=False)
    volume = Column(Float, nullable=False)
    
    base_price = Column(Float, nullable=False)
    distance_km = Column(Float, nullable=False)
    transport_type = Column(String(50), nullable=False)
    delivery_cost = Column(Float, nullable=False)
    total_price = Column(Float, nullable=False)
    
    is_converted = Column(Boolean, default=False)
    converted_at = Column(DateTime, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('idx_user_requests_created', 'created_at'),
        Index('idx_user_requests_user', 'user_id'),
    )


class Lead(Base):
    """
    Лид/заявка на коммерческое предложение.
    Создаётся при нажатии «Оставить заявку» и привязывается к конкретному расчёту (UserRequest).
    """

    __tablename__ = "leads"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    request_id = Column(Integer, ForeignKey("user_requests.id"), nullable=True, index=True)

    email = Column(String(255), nullable=True, index=True)
    phone = Column(String(50), nullable=True)
    company = Column(String(255), nullable=True)
    comment = Column(Text, nullable=True)

    # new/email_pending/sent/contacted/won/lost/cancelled
    status = Column(String(32), nullable=False, default="new", index=True)
    source = Column(String(32), nullable=True, default="bot")  # calc/analytics/alert/bot

    email_sent_at = Column(DateTime(timezone=True), nullable=True)
    messenger_sent_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index("idx_leads_status_created", "status", "created_at"),
    )


class PriceAlert(Base):
    """Подписки на снижение цены"""
    __tablename__ = 'price_alerts'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    
    target_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    city_destination_id = Column(Integer, ForeignKey('city_destinations.id'), nullable=True)
    
    email = Column(String(255), nullable=True)
    
    is_active = Column(Boolean, default=True)
    triggered_at = Column(DateTime, nullable=True)
    notification_sent = Column(Boolean, default=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('idx_price_alerts_active', 'is_active', 'product_id'),
    )


class NotificationLog(Base):
    """Лог отправленных уведомлений"""
    __tablename__ = 'notification_logs'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    alert_id = Column(Integer, ForeignKey('price_alerts.id'), nullable=True)
    notification_type = Column(String(50), nullable=False)
    message = Column(Text, nullable=True)
    sent_at = Column(DateTime(timezone=True), server_default=func.now())
    is_success = Column(Boolean, default=True)


class SpimexPrice(Base):
    """Цены с СПбМТСБ для аналитики"""
    __tablename__ = "spimex_prices"
    
    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String(50), nullable=True)  # НОВОЕ ПОЛЕ для кода инструмента
    fuel = Column(String(100))
    basis = Column(String(100))
    price = Column(Float)
    volume = Column(Float, nullable=True)
    date = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<SpimexPrice {self.fuel} @ {self.basis}: {self.price} руб/т>"


class EmailOtp(Base):
    """Одноразовый код входа на сайт по email."""

    __tablename__ = "email_otps"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), nullable=False, index=True)
    code_hash = Column(String(128), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    attempts = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class AnomalyAlert(Base):
    """Подписка на аномальные изменения цены (day-to-day) по instrument_code."""
    __tablename__ = "anomaly_alerts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    # Код инструмента (как в product_basis_prices.instrument_code / spimex_prices.exchange_product_id)
    instrument_code = Column(String(50), nullable=False, index=True)

    # Порог в процентах (например 3.0 = 3%)
    threshold_pct = Column(Float, nullable=False, default=3.0)

    is_active = Column(Boolean, default=True)

    # Чтобы не спамить: на какую торговую дату уже уведомили (берём date из spimex_prices)
    last_notified_date = Column(DateTime, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("user_id", "instrument_code", name="uq_anomaly_alert_user_code"),
        Index("idx_anomaly_alerts_active_code", "is_active", "instrument_code"),
    )


async def init_db():
    """Инициализация базы данных (создание таблиц)"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ База данных инициализирована")


async def get_session() -> AsyncSession:
    """
    Новая сессия БД. Вызывающий код обязан закрыть её: await session.close()
    (нельзя return из async with — контекст закрывал бы сессию до использования).
    """
    return AsyncSessionLocal()