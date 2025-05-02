# parser/driver_license.py
from typing import Dict, Optional
from datetime import datetime
from .imports_and_settings import re, logger

def validate_date(date_str: str) -> bool:
    """Проверяет, является ли строка валидной датой в формате ДД.ММ.ГГГГ."""
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def parse_driver_license(text: str) -> Optional[str]:
    """Извлекает серию и номер водительского удостоверения из текста."""
    text = re.sub(r'\s+', ' ', text).strip()
    logger.debug(f"Поиск ВУ в тексте: {text[:100]}...")

    # Основной шаблон: XX XX XXXXXX или XXXXXXXXXX
    vu_match = re.search(
        r"(?:в/у|ву|водительское\s*удостоверение|права|вод\.?\s*уд\.?|вод\.?\s*удост\.?)\s*(?::|\s|-)*\s*(\d{2,4}\s*\d{2,3}\s*\d{3,6}|\d{10})",
        text,
        re.IGNORECASE
    )
    if vu_match:
        vu_number = vu_match.group(1).strip()
        # Удаляем пробелы для формата без пробелов (например, 1024744024)
        if vu_number.replace(" ", "").isdigit() and len(vu_number.replace(" ", "")) == 10:
            return vu_number.replace(" ", "")
        # Сохраняем пробелы для формата с пробелами (например, 99 31 849596)
        logger.debug(f"Найден ВУ: {vu_number}")
        return vu_number

    logger.debug("ВУ не найден")
    return None

def parse_driver_license_data(text: str) -> Dict[str, Optional[str]]:
    """Извлекает данные водительского удостоверения (серия, номер, дата выдачи)."""
    logger.debug(f"Поиск данных водительского удостоверения в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    data = {}

    vu_series_number = parse_driver_license(text)
    if vu_series_number:
        data["ВУ_серия_и_номер"] = vu_series_number
        logger.debug(f"Извлечённый номер ВУ: {vu_series_number}")
    else:
        logger.debug("Номер ВУ не найден, дальнейший поиск даты невозможен")
        return data

    birth_date_pattern = re.compile(
        r"(?:д\.?р\.?|дата\s*р\s*ождения|г\.?р\.?)\s*(?:[:\s-]*\s*)?(\d{2}\.\d{2}\.\d{4}(?:\s*г\.?)?)",
        re.IGNORECASE)
    birth_date_match = birth_date_pattern.search(text)
    birth_date = birth_date_match.group(1) if birth_date_match else None
    if birth_date:
        birth_date = re.sub(r'\s*г\.?$', '', birth_date).strip()
        logger.debug(f"Извлечённая дата рождения: {birth_date}")

    passport_date_pattern = re.compile(
        r"(?:паспорт|пасп|п/п|серия\s*и\s*номer|серия|данные\s*водителя|выдан).*?"
        r"(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)
    passport_date_match = passport_date_pattern.search(text)
    passport_date = passport_date_match.group(1) if passport_date_match and validate_date(passport_date_match.group(1)) else None
    if passport_date:
        logger.debug(f"Извлечённая дата выдачи паспорта: {passport_date}")

    vu_section_pattern = re.compile(
        r"(?:в/у|ву|водительское\s*удостоверение|права|вод\.?\s*уд\.?|вод\.?\s*удост\.?)\s*(?::|\s|-)*\s*(?:\d{2,4}\s*\d{2,3}\s*\d{3,6}|\d{10})\s*.*?(?=\s*(?:телефон|автомобиль|машина|а/м|прицеп|перевозчик|$))",
        re.IGNORECASE
    )
    vu_section_match = vu_section_pattern.search(text)
    if vu_section_match:
        vu_section = vu_section_match.group(0).strip()
        logger.debug(f"Извлечённая подстрока ВУ: {vu_section}")
    else:
        logger.debug("Подстрока ВУ не найдена, пробуем запасной вариант")
        vu_section = text

    date_pattern = re.compile(
        r"(?:выдано|дата\s*выдачи|от)\s*(?::\s*)?[^0-9]*(\d{2}\.\d{2}\.\d{4})(?:\s*г\.?)?",
        re.IGNORECASE
    )
    date_match = date_pattern.search(vu_section)
    vu_date = date_match.group(1) if date_match and validate_date(date_match.group(1)) else None
    if vu_date:
        logger.debug(f"Извлечённая дата из подстроки: {vu_date}")
        if birth_date and vu_date == birth_date:
            logger.debug("Дата совпадает с датой рождения, исключаем")
            vu_date = None
        if passport_date and vu_date == passport_date:
            logger.debug("Дата совпадает с датой выдачи паспорта, исключаем")
            vu_date = None

    if not vu_date:
        logger.debug("Дата не найдена с ключевыми словами, пробуем общий поиск в подстроке")
        date_fallback_pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4})(?:\s*г\.?)?", re.IGNORECASE)
        date_fallback_match = date_fallback_pattern.search(vu_section)
        if date_fallback_match:
            vu_date = date_fallback_match.group(1)
            if validate_date(vu_date) and (not birth_date or vu_date != birth_date) and (not passport_date or vu_date != passport_date):
                logger.debug(f"Дата выдачи ВУ (запасной паттерн): {vu_date}")
            else:
                vu_date = None
                logger.debug(f"Дата (запасной паттерн) не прошла валидацию или совпадает с датой рождения/паспорта: {vu_date}")

    if not vu_date:
        logger.debug("Дата всё ещё не найдена, пробуем искать после номера ВУ")
        vu_number_pattern = re.compile(
            r"(?:в/у|ву|водительское\s*удостоверение|права|вод\.?\s*уд\.?|вод\.?\s*удост\.?)\s*(?::|\s|-)*\s*(\d{2,4}\s*\d{2,3}\s*\d{3,6}|\d{10})",
            re.IGNORECASE)
        vu_number_match = vu_number_pattern.search(text)
        if vu_number_match:
            start_pos = vu_number_match.end()
            remaining_text = text[start_pos:].strip()
            logger.debug(f"Текст после номера ВУ: {remaining_text}")
            date_match = date_pattern.search(remaining_text)
            if date_match:
                vu_date = date_match.group(1)
                if validate_date(vu_date) and (not birth_date or vu_date != birth_date) and (not passport_date or vu_date != passport_date):
                    logger.debug(f"Дата выдачи ВУ (после номера): {vu_date}")
                else:
                    vu_date = None
                    logger.debug(f"Дата (после номера) не прошла валидацию или совпадает с датой рождения/паспорта: {vu_date}")
            if not vu_date:
                date_fallback_match = date_fallback_pattern.search(remaining_text)
                if date_fallback_match:
                    vu_date = date_fallback_match.group(1)
                    if validate_date(vu_date) and (not birth_date or vu_date != birth_date) and (not passport_date or vu_date != passport_date):
                        logger.debug(f"Дата выдачи ВУ (запасной паттерн после номера): {vu_date}")
                    else:
                        vu_date = None
                        logger.debug(f"Дата (запасной паттерн после номера) не прошла валидацию или совпадает с датой рождения/паспорта: {vu_date}")

    if vu_date:
        logger.debug(f"Дата выдачи ВУ: {vu_date}")
        data["В/У_дата_срок"] = vu_date
    else:
        logger.debug("Дата выдачи ВУ не найдена")

    return data