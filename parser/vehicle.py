# parser/vehicle.py
from typing import Tuple, Optional
from .imports_and_settings import re, logger

def parse_vehicle_data(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Извлекает данные автомобиля и прицепа из текста."""
    logger.debug(f"Поиск данных автомобиля в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text).strip()

    vehicle = None
    trailer = None

    # Поиск автомобиля и прицепа по ключевым словам
    vehicle_pattern = re.compile(
        r"(?:автомобиль|автомашина|а/м|машина|т/с|гос\s*номер\s*на\s*тягач|авто)\s*(?::|\s|-)*\s*([A-Za-zА-ЯЁа-яё-]+(?:\s+[A-Za-zА-ЯЁа-яё-]+)*\s+[А-ЯЁ\d\s/]+)(?=\s|$|\n|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|$)",
        re.IGNORECASE
    )
    vehicle_match = vehicle_pattern.search(text)
    if vehicle_match:
        vehicle = vehicle_match.group(1).strip()
        # Нормализуем формат номера
        vehicle = re.sub(r'\s+', ' ', vehicle)
        logger.debug(f"Автомобиль: {vehicle}")
    else:
        logger.warning(f"Не удалось разобрать данные автомобиля из текста: {text[:100]}...")

    trailer_pattern = re.compile(
        r"(?:прицеп|полуприцеп|p/п|п/пр\.?|гос\s*номер\s*на\s*п/п)\s*(?::|\s|-)*\s*([A-Za-zА-ЯЁа-яё-]+(?:\s+[A-Za-zА-ЯЁа-яё-]+)*\s*[А-ЯЁ\d\s/]+|[А-ЯЁ\d\s/]+)(?=\s|$|\n|перевозчик|$)",
        re.IGNORECASE
    )
    trailer_match = trailer_pattern.search(text)
    if trailer_match:
        trailer = trailer_match.group(1).strip()
        # Нормализуем формат номера прицепа
        trailer = re.sub(r'\s+', ' ', trailer)
        # Если прицеп состоит только из номера, форматируем его
        if re.match(r'^[А-ЯЁ\d\s/]+$', trailer):
            trailer = re.sub(r'(\b[А-ЯЁ]{2})(\d{4})(\s*/\s*|\s+)(\d{1,3})', r'\1 \2 \3', trailer)
        logger.debug(f"Прицеп: {trailer}")
    else:
        logger.debug("Прицеп не найден")

    return vehicle, trailer