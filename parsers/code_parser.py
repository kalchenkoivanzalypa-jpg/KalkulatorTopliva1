#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер, который ищет в PDF только заданные коды инструментов
и извлекает рыночные цены (11-я колонка)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pdfplumber
import re
import logging
from instruments import INSTRUMENTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_bulletin_by_codes(pdf_path):
    """
    Парсит бюллетень и возвращает цены только для кодов из INSTRUMENTS
    Рыночная цена находится в 11-й колонке (индекс 10) таблицы
    """
    logger.info(f"📄 Парсинг файла: {pdf_path}")
    logger.info(f"🔍 Ищем {len(INSTRUMENTS)} кодов инструментов")
    
    # Создаём копию словаря с обнулёнными ценами
    results = {}
    for code, data in INSTRUMENTS.items():
        results[code] = data.copy()
        results[code]["market_price"] = 0
        results[code]["found"] = False
    
    found_count = 0
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"📑 Всего страниц: {len(pdf.pages)}")
            
            for page_num, page in enumerate(pdf.pages, 1):
                # Извлекаем таблицы со страницы
                tables = page.extract_tables()
                
                for table_num, table in enumerate(tables):
                    if not table:
                        continue
                    
                    # Проходим по строкам таблицы
                    for row in table:
                        if not row or len(row) < 11:  # минимум 11 колонок для рыночной цены
                            continue
                        
                        # Первая колонка - код инструмента
                        code_cell = str(row[0]) if row[0] else ""
                        
                        # Проверяем каждый код из нашего списка
                        for code in INSTRUMENTS.keys():
                            if code in code_cell and not results[code]["found"]:
                                # 11-я колонка (индекс 10) - рыночная цена
                                price_cell = row[10] if len(row) > 10 else None
                                
                                if price_cell:
                                    try:
                                        # Очищаем цену от лишних символов
                                        price_str = str(price_cell).replace(' ', '').replace(',', '.').replace('—', '0')
                                        if price_str and price_str != 'nan' and price_str != '-':
                                            price = float(price_str)
                                            if 1000 < price < 200000:  # проверка что это цена
                                                results[code]["market_price"] = price
                                                results[code]["found"] = True
                                                found_count += 1
                                                logger.info(f"  ✅ Страница {page_num}, таблица {table_num}: {code} = {price} руб/т (рыночная)")
                                    except (ValueError, TypeError):
                                        # Если не удалось преобразовать, пробуем найти число в строке
                                        numbers = re.findall(r'(\d+[\.,]?\d*)', str(price_cell))
                                        if numbers:
                                            try:
                                                price = float(numbers[0].replace(',', '.'))
                                                if 1000 < price < 200000:
                                                    results[code]["market_price"] = price
                                                    results[code]["found"] = True
                                                    found_count += 1
                                                    logger.info(f"  ✅ Страница {page_num}, таблица {table_num}: {code} = {price} руб/т (рыночная из текста)")
                                            except:
                                                pass
                                
                                break  # код найден, переходим к следующему
    
    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге: {e}")
    
    logger.info(f"✅ Найдено {found_count} позиций из {len(INSTRUMENTS)}")
    return results

def print_results(results):
    """Красивый вывод результатов"""
    print("\n" + "=" * 80)
    print("📊 РЕЗУЛЬТАТЫ ПАРСИНГА (РЫНОЧНЫЕ ЦЕНЫ):")
    print("=" * 80)
    
    # Группировка по типам топлива
    fuels = {}
    for code, data in results.items():
        if data["market_price"] > 0:
            fuel = data["fuel"]
            if fuel not in fuels:
                fuels[fuel] = []
            fuels[fuel].append((code, data))
    
    # Выводим по группам
    for fuel, items in sorted(fuels.items()):
        print(f"\n📦 {fuel}:")
        print("-" * 40)
        for code, data in sorted(items):
            print(f"  • {data['basis']}: {data['market_price']:,.0f} руб/т ({data['transport']})")
            print(f"    [{code}]")
    
    # Статистика
    total_found = sum(1 for d in results.values() if d["market_price"] > 0)
    print(f"\n✅ Всего найдено: {total_found} из {len(results)}")
    print(f"⚠️  Не найдено: {len(results) - total_found}")
    
    return total_found

def save_results_to_file(results, filename="prices_output.txt"):
    """Сохраняет результаты в файл"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("РЕЗУЛЬТАТЫ ПАРСИНГА РЫНОЧНЫХ ЦЕН\n")
        f.write("=" * 80 + "\n")
        
        for code, data in results.items():
            if data["market_price"] > 0:
                f.write(f"{code}: {data['fuel']} @ {data['basis']} = {data['market_price']:,.0f} руб/т ({data['transport']})\n")
        
        total = sum(1 for d in results.values() if d["market_price"] > 0)
        f.write(f"\nВсего найдено: {total}\n")
    
    logger.info(f"💾 Результаты сохранены в {filename}")

if __name__ == "__main__":
    import glob
    
    pdf_files = glob.glob("data/bulletins/*.pdf")
    if pdf_files:
        latest_pdf = max(pdf_files, key=os.path.getctime)
        results = parse_bulletin_by_codes(latest_pdf)
        print_results(results)
        save_results_to_file(results)
    else:
        print("❌ Нет PDF файлов в папке data/bulletins/")