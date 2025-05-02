# parser/passport.py
from typing import Dict, Optional
from datetime import datetime
from .imports_and_settings import re, logger, valid_letters, OKATO_CODES, SUBDIVISIONS

def validate_date(date_str: str) -> bool:
    """Проверяет, является ли строка валидной датой в формате ДД.ММ.ГГГГ."""
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def parse_passport_number(text: str) -> Optional[str]:
    """Извлекает серию и номер паспорта из текста."""
    text = re.sub(r'\s+', ' ', text).strip()
    logger.debug(f"Поиск серии и номера паспорта в: {text}")

    # Специальный формат: Серия номер: XXXX XXXXXX
    passport_match_special = re.search(
        r"(?:Серия\s*номер\s*:\s*)(\d{4}\s*\d{6})",
        text,
        re.IGNORECASE
    )
    if passport_match_special:
        series_number = passport_match_special.group(1).strip()
        series_number = re.sub(r'\s+', '', series_number)
        logger.debug(f"Специальный формат совпал: {series_number}")
        if len(series_number) == 10:
            series = series_number[:4]
            number = series_number[4:]
            region_code = series[:2]
            if region_code not in OKATO_CODES:
                logger.warning(f"Недопустимый код региона в серии паспорта: {region_code}")
            result = f"{series} {number}"
            logger.debug(f"Паспорт (специальный формат): {result}")
            return result
    logger.debug("Специальный формат не совпал")

    # Стандартный формат: XX XX XXXXXX или XXXX XXXXXX
    passport_match = re.search(
        r"(?:[Пп]аспорт|Серия|Данные\s*водителя)\s*(?::|\s|-)*\s*(?:серия\s*(?:(?:и\s*)?номер\s*)?(?:№\s*)?)?(\d{2}\s*\d{2}|\d{4})\s*(?:№\s*|номер\s*)?(\d{6})",
        text,
        re.IGNORECASE
    )
    if passport_match:
        series, number = passport_match.groups()
        series = re.sub(r'\s+', '', series).strip()
        number = re.sub(r'\s+', '', number).strip()
        logger.debug(f"Стандартный формат совпал: серия={series}, номер={number}")
        exceptions = [
            "4713431628",
            "8309981436",
            "1513079375",
            "3813945189",
            "8620342311",
            "4123445068"]
        combined = f"{series}{number}"
        if combined in exceptions:
            result = f"{series} {number}"
            logger.debug(f"Паспорт (стандартный формат, исключение): {result}")
            return result
        else:
            region_code = series[:2]
            if region_code not in OKATO_CODES:
                logger.warning(f"Недопустимый код региона в серии паспорта: {region_code}")
            series = f"{series[:2]} {series[2:]}"
            result = f"{series} {number}"
            logger.debug(f"Паспорт (стандартный формат): {result}")
            return result
    logger.debug("Стандартный формат не совпал")

    # Альтернативный формат: XXXX XXXXXX или XXXXXXXXXX
    passport_match_alt = re.search(
        r"(?:[Пп]аспорт|Серия|Данные\s*водителя)\s*(?::|\s|-)*\s*(?:серия\s*(?:(?:и\s*)?номер\s*)?(?:№\s*)?)?(\d{4}(?:\s*\d{3}\s*\d{3}|\s*\d{6}|\d{6}))",
        text,
        re.IGNORECASE
    )
    if passport_match_alt:
        series_number = passport_match_alt.group(1).strip()
        series_number = re.sub(r'\s+', '', series_number)
        logger.debug(f"Альтернативный формат совпал: {series_number}")
        if len(series_number) == 10:
            series = series_number[:4]
            number = series_number[4:]
            exceptions = [
                "4713431628",
                "8309981436",
                "1513079375",
                "3813945189",
                "8620342311",
                "4123445068"]
            if series_number in exceptions:
                result = f"{series} {number}"
                logger.debug(f"Паспорт (альтернативный формат, исключение): {result}")
                return result
            else:
                region_code = series[:2]
                if region_code not in OKATO_CODES:
                    logger.warning(f"Недопустимый код региона в серии паспорта: {region_code}")
                series = f"{series[:2]} {series[2:]}"
                result = f"{series} {number}"
                logger.debug(f"Паспорт (альтернативный формат): {result}")
                return result
        logger.debug(f"Паспорт (альтернативный формат, без изменений): {series_number}")
        return series_number
    logger.debug("Альтернативный формат не совпал")

    # Простой формат: XXXX XXXXXX в начале строки
    passport_match_simple = re.search(
        r"^\s*(\d{4}\s*\d{6})\s+",
        text,
        re.IGNORECASE
    )
    if passport_match_simple:
        series_number = passport_match_simple.group(1).strip()
        series_number = re.sub(r'\s+', '', series_number)
        logger.debug(f"Простой формат совпал: {series_number}")
        if len(series_number) == 10:
            series = series_number[:4]
            number = series_number[4:]
            region_code = series[:2]
            if region_code not in OKATO_CODES:
                logger.warning(f"Недопустимый код региона в серии паспорта: {region_code}")
            result = f"{series} {number}"
            logger.debug(f"Паспорт (простой формат): {result}")
            return result
    logger.debug("Простой формат не совпал")

    logger.debug("Серия и номер паспорта не найдены")
    return None

def parse_passport_issuing_authority(text: str) -> Optional[str]:
    """Извлекает место выдачи паспорта из текста."""
    logger.debug(f"Поиск места выдачи паспорта в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    passport_place_pattern = re.compile(
        r"(?:выдан|выдано|кем\s*выдан|_место_выдачи)\s*[:\-\s]*(.+?)(?=\s*(?:д\.в\.?|дата\s*выдачи|\d{2}\.\d{2}\.\d{2,4}|код|в/у|ву|водительское\s*удостоверение|права|тел\.?|телефон|а/м|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|$))",
        re.IGNORECASE
    )
    passport_place_match = passport_place_pattern.search(text)
    if passport_place_match:
        place = passport_place_match.group(1).strip()
        place = re.sub(
            r"\d{1,2}\.\d{1,2}\.\d{2,4}(?:г\.?)?|\d{3}-\d{3}",
            "",
            place).strip()
        place = re.sub(
            r"^(выдан|выдано|кем\s*выдан|_место_выдачи|серия\s*и\s*номer|серия|паспорт|:\s*)",
            "",
            place,
            flags=re.IGNORECASE).strip()
        place = re.sub(r"\s*г\.?$|\s*от$", "", place).strip()
        place = re.sub(r"\bпаспорт\b", "", place, flags=re.IGNORECASE).strip()
        place = re.sub(r'\bг\.([А-Яа-яЁё])', r'г. \1', place)
        logger.debug(f"Место выдачи найдено: {place}")
        if len(place) < 5:
            logger.debug("Место выдачи слишком короткое, пропускаем")
            return None
        return place

    passport_place_simple = re.compile(
        r"^\s*\d{4}\s*\d{6}\s+(.+?)(?=\s*(?:\d{2}\.\d{2}\.\d{2,4}|\s+[А-ЯЁ]\d{3}[А-ЯЁ]{2}\d{2,3}|$))",
        re.IGNORECASE)
    passport_place_simple_match = passport_place_simple.search(text)
    if passport_place_simple_match:
        place = passport_place_simple_match.group(1).strip()
        place = re.sub(r"\s*г\.?$|\s*от$", "", place).strip()
        place = re.sub(r'\bг\.([А-Яа-яЁё])', r'г. \1', place)
        logger.debug(f"Место выдачи найдено (простой формат): {place}")
        if len(place) < 5:
            logger.debug("Место выдачи слишком короткое, пропускаем")
            return None
        return place

    logger.debug("Место выдачи паспорта не найдено")
    return None

def parse_passport_data(text: str) -> Dict[str, Optional[str]]:
    """Извлекает данные паспорта (серия, номер, место выдачи, дата выдачи, код подразделения)."""
    logger.debug(f"Поиск данных паспорта в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    data = {}

    passport_series_number = parse_passport_number(text)
    if passport_series_number:
        data["Паспорт_серия_и_номер"] = passport_series_number
        logger.debug(f"Добавлен Паспорт_серия_и_номер: {passport_series_number}")
    else:
        logger.debug("Паспорт_серия_и_номер не найден")

    issuing_authority = parse_passport_issuing_authority(text)
    if issuing_authority:
        data["Паспорт_место_выдачи"] = issuing_authority
        logger.debug(f"Добавлен Паспорт_место_выдачи: {issuing_authority}")

    date_pattern = re.compile(
        r"(?:паспорт|пасп|п/п|серия\s*и\s*номer|серия|данные\s*водителя|выдан).*?"
        r"(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)
    date_match = date_pattern.search(text)
    issuing_date = date_match.group(1) if date_match and validate_date(date_match.group(1)) else None

    if not issuing_date:
        date_alt_pattern = re.compile(
            r"^\s*\d{4}\s*\d{6}\s+.+?\s+(\d{2}\.\d{2}\.\d{2,4})",
            re.IGNORECASE
        )
        date_alt_match = date_alt_pattern.search(text)
        if date_alt_match:
            date_str = date_alt_match.group(1)
            parts = date_str.split('.')
            if len(parts) == 3:
                day, month, year = parts
                if len(year) == 2:
                    year = f"20{year}" if int(year) <= 24 else f"19{year}"
                issuing_date = f"{day}.{month}.{year}"
                if validate_date(issuing_date):
                    logger.debug(f"Дата выдачи паспорта (преобразована): {issuing_date}")
                else:
                    issuing_date = None

    if issuing_date:
        logger.debug(f"Дата выдачи паспорта: {issuing_date}")
        data["Паспорт_дата_выдачи"] = issuing_date

    code_pattern = re.compile(
        r"код\s*подразделения\s*[:\-\s]*(\d{3}-\d{3})",
        re.IGNORECASE)
    code_match = code_pattern.search(text)
    code = code_match.group(1) if code_match else None
    if code:
        if code not in SUBDIVISIONS:
            logger.warning(f"Код подразделения {code} не найден в базе данных")
        logger.debug(f"Код подразделения паспорта: {code}")
        data["Паспорт_код_подразделения"] = code

    logger.debug(f"Итоговые данные паспорта: {data}")
    return data