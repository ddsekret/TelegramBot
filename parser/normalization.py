# parser/normalization.py
from typing import Dict, Optional
from .imports_and_settings import re, logger, SUBDIVISIONS, COMPOSITE_CITIES, CITY_NOMINATIVE, SMALL_WORDS

def normalize_passport_data(data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    """Нормализует данные паспорта."""
    if "Паспорт_код_подразделения" in data and data["Паспорт_код_подразделения"]:
        code = data["Паспорт_код_подразделения"]
        if code in SUBDIVISIONS:
            subdivision_info = SUBDIVISIONS[code]
            region = subdivision_info["region"]
            subdivision = subdivision_info["subdivision"]
            data["Паспорт_место_выдачи"] = f"{subdivision} ({region})"
            logger.debug(f"Нормализованное место выдачи: {data['Паспорт_место_выдачи']}")
        else:
            logger.warning(f"Код подразделения {code} не найден в SUBDIVISIONS, оставляем оригинальное место выдачи")
            # Сохраняем оригинальное место выдачи, но нормализуем регистр
            if "Паспорт_место_выдачи" in data and data["Паспорт_место_выдачи"]:
                place = data["Паспорт_место_выдачи"]
                words = place.split()
                formatted_place = []
                for word in words:
                    if word.lower() in SMALL_WORDS:
                        formatted_place.append(word.lower())
                    else:
                        formatted_place.append(word)
                data["Паспорт_место_выдачи"] = ' '.join(formatted_place)
    return data

def normalize_vehicle_data(data: Dict[str, Optional[str]], key: str) -> Dict[str, Optional[str]]:
    """Нормализует данные автомобиля или прицепа."""
    if key in data and data[key]:
        vehicle = data[key].strip()
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