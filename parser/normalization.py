# parser/normalization.py
from typing import Dict, Optional
from .imports_and_settings import re, logger, SUBDIVISIONS, COMPOSITE_CITIES, CITY_NOMINATIVE, SMALL_WORDS

def normalize_passport_data(data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Нормализует данные паспорта."""
    if "Паспорт_код_подразделения" in data and data["Паспорт_код_подразделения"]:
        code = data["Паспорт_код_подразделения"]
        if code in SUBDIVISIONS:
            subdivision_info = SUBDIVISIONS[code]
            subdivision = subdivision_info["subdivision"]
            region = subdivision_info.get("region", "")
            # Добавляем регион в скобках, если он отсутствует
            if region and not subdivision.endswith(f"({region})"):
                subdivision = f"{subdivision} ({region})"
            data["Паспорт_место_выдачи"] = subdivision
            logger.debug(f"Нормализованное место выдачи: {data['Паспорт_место_выдачи']}")
        else:
            logger.warning(f"Код подразделения {code} не найден в SUBDIVISIONS, оставляем оригинальное место выдачи")
            # Сохраняем оригинальное место выдачи и регистр
            if "Паспорт_место_выдачи" in data and data["Паспорт_место_выдачи"]:
                data["Паспорт_место_выдачи"] = data["Паспорт_место_выдачи"]
    return data

def normalize_vehicle_data(data: Dict[str, Optional[str]], key: str) -> Dict[str, Optional[str]]:
    """Нормализует данные автомобиля или прицепа."""
    if key in data and data[key]:
        vehicle = data[key].strip()
        # Приводим бренд к верхнему регистру
        parts = vehicle.split(maxsplit=1)
        if len(parts) > 1:
            brand, number = parts
            vehicle = f"{brand.upper()} {number}"
        data[key] = vehicle
    return data

def normalize_data(data: Dict[str, Optional[str]], text: str) -> Dict[str, Optional[str]]:
    """Нормализует данные."""
    data = normalize_passport_data(data)
    data = normalize_vehicle_data(data, "Автомобиль")
    data = normalize_vehicle_data(data, "Прицеп")

    if "Адрес_регистрации" in data and data["Адрес_регистрации"]:
        address = data["Адрес_регистрации"]
        for key, value in COMPOSITE_CITIES.items():
            address = re.sub(rf'\b{key}\b', value, address, flags=re.IGNORECASE)
        for key, value in CITY_NOMINATIVE.items():
            address = re.sub(rf'\b{key}\b', value, address, flags=re.IGNORECASE)
        data["Адрес_регистрации"] = address

    return data