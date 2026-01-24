"""
Тестовый скрипт для проверки API запроса получения данных директора
"""
import asyncio
import json
import logging
from word_processor import get_director_data, format_director_string
from config import conf_logger

# Настраиваем логирование
conf_logger()
logger = logging.getLogger(__name__)


async def test_get_director_data():
    """Тестирует получение данных директора из задачи лида"""
    
    # Берем task_id из payload.json (поле "Новый Лид")
    # В payload.json есть task_id: 332305665
    test_task_id = 332305665
    
    logger.info(f"Testing API request for task {test_task_id}")
    
    try:
        director_fio, is_general_director = await get_director_data(test_task_id)
        
        logger.info(f"✅ Successfully retrieved director data:")
        logger.info(f"   FIO: {director_fio}")
        logger.info(f"   Is General Director: {is_general_director}")
        
        # Формируем строку директора
        director_string = format_director_string(director_fio, is_general_director, max_length=42)
        logger.info(f"   Formatted string: {director_string}")
        logger.info(f"   String length: {len(director_string)}")
        
        # Выводим результат
        print("\n" + "="*50)
        print("РЕЗУЛЬТАТ ТЕСТА:")
        print("="*50)
        print(f"ФИО директора: {director_fio}")
        print(f"Генеральный директор: {is_general_director}")
        print(f"Отформатированная строка: {director_string}")
        print(f"Длина строки: {len(director_string)} символов")
        print("="*50 + "\n")
        
        return director_fio, is_general_director, director_string
        
    except Exception as e:
        logger.error(f"❌ Error during test: {e}", exc_info=True)
        print(f"\n❌ ОШИБКА: {e}\n")
        raise


async def test_format_director_string():
    """Тестирует функцию форматирования строки директора"""
    
    test_cases = [
        ("Иванов И.И.", True, "Генеральный директор______________ / Иванов И.И."),
        ("Иванов И.И.", False, "Директор______________ / Иванов И.И."),
        ("Очень Длинное ФИО Директора Организации", True, None),  # Проверим автоматическое сокращение
        ("А.Б. В.", False, None),
    ]
    
    print("\n" + "="*50)
    print("ТЕСТ ФОРМАТИРОВАНИЯ СТРОКИ ДИРЕКТОРА:")
    print("="*50)
    
    for fio, is_general, expected in test_cases:
        result = format_director_string(fio, is_general, max_length=42)
        print(f"\nВходные данные:")
        print(f"  ФИО: {fio}")
        print(f"  Генеральный: {is_general}")
        print(f"Результат: {result}")
        print(f"Длина: {len(result)} символов")
        if expected:
            print(f"Ожидалось: {expected}")
            print(f"✓ Совпадает: {result == expected}")
    
    print("="*50 + "\n")


if __name__ == "__main__":
    print("Запуск тестов API запроса для получения данных директора...\n")
    
    # Тест форматирования строки
    asyncio.run(test_format_director_string())
    
    # Тест API запроса
    try:
        asyncio.run(test_get_director_data())
        print("✅ Все тесты завершены успешно!")
    except Exception as e:
        print(f"❌ Тесты завершились с ошибкой: {e}")
