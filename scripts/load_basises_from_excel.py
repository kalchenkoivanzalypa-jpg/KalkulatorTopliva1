# load_basises_from_excel.py
import asyncio
import logging
import pandas as pd
from db.database import Basis, Product, get_session
from sqlalchemy import select, update
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_basis_name(basis_name: str) -> str:
    """Очистка названия базиса от лишних символов"""
    # Убираем <br> и лишние пробелы
    clean_name = basis_name.replace('<br>', ' ').strip()
    clean_name = re.sub(r'\s+', ' ', clean_name)
    return clean_name

async def load_basises_from_excel(file_path: str = "Базисы поставок.xlsx"):
    """Загрузка базисов из Excel файла"""
    
    logger.info(f"📂 Чтение файла: {file_path}")
    
    try:
        # Читаем Excel файл
        df = pd.read_excel(file_path)
        logger.info(f"✅ Прочитано строк: {len(df)}")
        
        # Проверяем наличие нужных колонок
        required_columns = ['basis', 'transport']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"❌ В файле нет колонки '{col}'")
                return
        
        session = await get_session()
        try:
            # Получаем существующие базисы
            result = await session.execute(
                select(Basis)
            )
            existing_basises = {b.name: b for b in result.scalars().all()}
            logger.info(f"📊 В БД уже есть базисов: {len(existing_basises)}")
            
            # Собираем уникальные базисы из Excel
            unique_basises = {}
            for _, row in df.iterrows():
                basis_name = parse_basis_name(row['basis'])
                transport = row['transport']
                
                if pd.isna(basis_name) or not basis_name:
                    continue
                    
                if basis_name not in unique_basises:
                    unique_basises[basis_name] = {
                        'name': basis_name,
                        'transport': transport if not pd.isna(transport) else 'rail'
                    }
            
            logger.info(f"📊 Уникальных базисов в Excel: {len(unique_basises)}")
            
            # Добавляем или обновляем базисы
            added = 0
            updated = 0
            skipped = 0
            
            for basis_name, data in unique_basises.items():
                if basis_name in existing_basises:
                    # Базис уже существует, обновляем transport_type
                    basis = existing_basises[basis_name]
                    if basis.transport_type != data['transport']:
                        await session.execute(
                            update(Basis)
                            .where(Basis.id == basis.id)
                            .values(transport_type=data['transport'])
                        )
                        updated += 1
                        logger.info(f"🔄 Обновлен: {basis_name} -> {data['transport']}")
                    else:
                        skipped += 1
                else:
                    # Новый базис
                    new_basis = Basis(
                        name=basis_name,
                        city=basis_name.split()[0] if basis_name else basis_name,
                        latitude=0.0,  # Нужно будет потом добавить координаты
                        longitude=0.0,
                        is_active=True,
                        transport_type=data['transport']
                    )
                    session.add(new_basis)
                    added += 1
                    logger.info(f"➕ Добавлен: {basis_name} ({data['transport']})")
            
            await session.commit()
            
            logger.info("=" * 50)
            logger.info(f"✅ Добавлено новых базисов: {added}")
            logger.info(f"🔄 Обновлено базисов: {updated}")
            logger.info(f"⏭️ Пропущено (без изменений): {skipped}")
            logger.info(f"📊 Всего базисов в БД теперь: {len(unique_basises)}")
            logger.info("=" * 50)
            
            # Покажем примеры базисов с разным транспортом
            logger.info("\n📋 Примеры базисов по типам транспорта:")
            
            # Получаем обновленный список
            result = await session.execute(
                select(Basis).where(Basis.is_active == True)
            )
            all_basises = result.scalars().all()
            
            auto_basises = [b for b in all_basises if b.transport_type == 'auto'][:5]
            rail_basises = [b for b in all_basises if b.transport_type == 'rail'][:5]
            
            logger.info("🚛 Авто:")
            for b in auto_basises:
                logger.info(f"  - {b.name}")
            
            logger.info("🚂 Ж/Д:")
            for b in rail_basises:
                logger.info(f"  - {b.name}")
            
        finally:
            await session.close()
            
    except FileNotFoundError:
        logger.error(f"❌ Файл {file_path} не найден!")
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении файла: {e}")

async def main():
    """Основная функция"""
    logger.info("=" * 50)
    logger.info("ЗАГРУЗКА БАЗИСОВ ИЗ EXCEL")
    logger.info("=" * 50)
    
    await load_basises_from_excel("Базисы поставок.xlsx")
    
    logger.info("\n✅ Загрузка завершена!")

if __name__ == "__main__":
    asyncio.run(main())