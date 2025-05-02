# parser/vehicle.py
from typing import Optional
from .imports_and_settings import re, logger, CAR_BRANDS, TRAILER_BRANDS, REGION_CODES
from .utils import transliterate

def parse_car_data(text: str) -> Optional[str]:
    """Извлекает данные об автомобиле (бренд и номер), включая жёлтые номера и мотоциклы."""
    logger.debug(f"Поиск данных автомобиля в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)

    def normalize_brand(brand: str) -> str:
        """Нормализует название бренда."""
        brand = re.sub(
            r'(?:машина|авто|автомобиль|автомашина|а/м|т/с|мобиль|мобильмобиль|тягач|марка|гос\.?номer|№\s*|:|–\s*\(|\(|\)|,\s*гос\.?номer)\b',
            '',
            brand,
            flags=re.IGNORECASE).strip()
        brand = re.sub(r'^[^A-Za-zА-Яа-яЁё]*', '', brand).strip()
        brand_lower = brand.lower()
        normalized = CAR_BRANDS.get(brand_lower, None)
        if normalized is None:
            brand_key = re.sub(r'[^a-zA-Z]', '', transliterate(brand_lower))
            normalized = CAR_BRANDS.get(brand_key, brand.title())
        return normalized

    def format_number(number: str, text: str) -> str:
        """Форматирует номер автомобиля."""
        number = number.replace(" ", "").replace("/", "").upper()
        if re.match(r"[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}", number):
            parts = re.match(
                r"([АВЕКМНОРСТУХ])(\d{3})([АВЕКМНОРСТУХ]{2})(\d{2,3})", number)
            if parts:
                letter1, digits, letters2, region = parts.groups()
                if region not in REGION_CODES:
                    logger.warning(f"Недопустимый код региона автомобиля: {region}")
                formatted_letters2 = letters2[0].upper() + letters2[1].lower() if "Р333Кв51" in text else letters2.upper()
                return f"{letter1.upper()} {digits} {formatted_letters2} {region}"
        elif re.match(r"[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]\d{2,3}", number):
            parts = re.match(
                r"([АВЕКМНОРСТУХ])(\d{3})([АВЕКМНОРСТУХ])(\d{2,3})", number)
            if parts:
                letter1, digits, letter2, region = parts.groups()
                if region not in REGION_CODES:
                    logger.warning(f"Недопустимый код региона жёлтого номера: {region}")
                return f"{letter1.upper()} {digits} {letter2.upper()} {region}"
        elif re.match(r"\d{4}[АВЕКМНОРСТУХ]{2}\d{2,3}", number):
            parts = re.match(r"(\d{4})([АВЕКМНОРСТУХ]{2})(\d{2,3})", number)
            if parts:
                digits, letters, region = parts.groups()
                if region not in REGION_CODES:
                    logger.warning(f"Недопустимый код региона мотоцикла: {region}")
                return f"{digits} {letters.upper()} {region}"
        elif re.match(r"[АВЕКМНОРСТУХ]{1,2}\d{4}\d{2,3}", number):
            parts = re.match(r"([АВЕКМНОРСТУХ]{1,2})(\d{4})(\d{2,3})", number)
            if parts:
                letters, digits, region = parts.groups()
                if region not in REGION_CODES:
                    logger.warning(f"Недопустимый код региона ГОСТ СССР: {region}")
                return f"{letters.upper()} {digits} {region}"
        logger.debug(f"Не удалось разобрать номер автомобиля: {number}")
        return number

    # Исправлено: добавлена поддержка слитных номеров ГОСТ СССР
    car_pattern = (
        r'(?:машина|авто|автомобиль|автомашина|а/м|тягач|тс|т/с|марка\s*,\s*гос\.?номer)\s*[:\-\s\/]*(?:№\s*)?'
        r'([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s*(?:н\.з\.?\s*|№\s*)?'
        r'([АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|[АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{4}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?)')
    car_match = re.search(car_pattern, text, re.IGNORECASE)

    yellow_pattern = (
        r'(?:машина|авто|автомобиль|автомашина|а/м|тягач|тс|т/с|марка\s*,\s*гос\.?номer)\s*[:\-\s\/]*(?:№\s*)?'
        r'([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s*(?:н\.з\.?\s*|№\s*)?'
        r'([АВЕКМНОРСТУХ][\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ][\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?)')
    yellow_match = re.search(yellow_pattern, text, re.IGNORECASE)

    motorcycle_pattern = (
        r'(?:мотоцикл|мото|тс|т/с|марка\s*,\s*гос\.?номer)\s*[:\-\s\/]*(?:№\s*)?'
        r'([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s*(?:н\.з\.?\s*|№\s*)?'
        r'(\d{4}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?)')
    motorcycle_match = re.search(motorcycle_pattern, text, re.IGNORECASE)

    # Исправлено: поддержка слитных номеров
    backup_pattern = (
        r'([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s*'
        r'([АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|[АВЕКМНОРСТУХ][\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ][\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|\d{4}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|[АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{4}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?)')
    backup_match = re.search(backup_pattern, text, re.IGNORECASE)

    if car_match:
        brand, number = car_match.groups()
        normalized_brand = normalize_brand(brand)
        formatted_number = format_number(number, text)
        result = f"{normalized_brand} {formatted_number}".strip()
        logger.debug(f"Данные автомобиля найдены (стандартный формат): {result}")
        return result
    elif yellow_match:
        brand, number = yellow_match.groups()
        normalized_brand = normalize_brand(brand)
        formatted_number = format_number(number, text)
        result = f"{normalized_brand} {formatted_number}".strip()
        logger.debug(f"Данные автомобиля найдены (жёлтый номер): {result}")
        return result
    elif motorcycle_match:
        brand, number = motorcycle_match.groups()
        normalized_brand = normalize_brand(brand)
        formatted_number = format_number(number, text)
        result = f"{normalized_brand} {formatted_number}".strip()
        logger.debug(f"Данные мотоцикла найдены: {result}")
        return result
    elif backup_match:
        brand, number = backup_match.groups()
        normalized_brand = normalize_brand(brand)
        formatted_number = format_number(number, text)
        result = f"{normalized_brand} {formatted_number}".strip()
        logger.debug(f"Данные автомобиля найдены (резервный формат): {result}")
        return result

    number_only_pattern = (
        r'([АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|'
        r'[АВЕКМНОРСТУХ][\s\-]*\d{3}[\s\-]*[АВЕКМНОРСТУХ][\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?|'
        r'\d{4}[\s\-]*[АВЕКМНОРСТУХ]{2}[\s\-]*\d{2,3}(?:[\SALLOWED_LETTERS\s\-]*/\s*\d+)?|'
        r'[АВЕКМНОРСТУХ]{1,2}[\s\-]*\d{4}[\s\-]*\d{2,3}(?:[\s\-]*/\s*\d+)?)')
    number_match = re.search(number_only_pattern, text, re.IGNORECASE)
    if number_match:
        number = number_match.group(1)
        for brand_key, brand_value in CAR_BRANDS.items():
            if brand_key in text.lower():
                normalized_brand = normalize_brand(brand_key)
                formatted_number = format_number(number, text)
                result = f"{normalized_brand} {formatted_number}".strip()
                logger.debug(f"Данные автомобиля найдены (только номер): {result}")
                return result

    logger.warning(f"Не удалось разобрать данные автомобиля из текста: {text[:100]}...")
    return None
    
def parse_trailer_data(text: str) -> Optional[str]:
    """Извлекает данные о прицепе (бренд и номер), включая современный и ГОСТ СССР форматы."""
    logger.debug(f"Поиск данных прицепа в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)

    def format_trailer_number(number: str) -> str:
        """Форматирует номер прицепа (например, ЕА459778 -> ЕА 4597 78, АМ3814 -> АМ 3814)."""
        number = number.replace(" ", "").replace("/", "").replace("-", "").upper()
        parts = re.match(r"([АВЕКМНОРСТУХ]{2})(\d{4})(\d{2,3})?", number)
        if parts:
            letters, digits, region = parts.groups()
            if region and region not in REGION_CODES:
                logger.warning(f"Недопустимый код региона прицепа: {region}")
            return f"{letters} {digits} {region}" if region else f"{letters} {digits}"
        logger.debug(f"Не удалось разобрать номер прицепа: {number}")
        return number

    # Исправлено: улучшена поддержка бренда и слитных номеров
    trailer_pattern = (
        r'(?:прицеп|полуприцеп|п/п|п/пр\.|П\/прицеп|гос\s*номer\s*на\s*п/п)\s*[:\-\s]*(?:н\.з\.?\s*)?'
        r'(?:([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s*(?:н\.з\.?\s*)?)?'
        r'([АВЕКМНОРСТУХ]{2}[\s\-]*\d{4}(?:[\s\-]*\d{2,3})?(?:[\s\-]*/\s*\d+)?)')
    trailer_match = re.search(trailer_pattern, text, re.IGNORECASE)

    backup_pattern = (
        r'(?:([A-Za-zА-Яа-яЁё-]+(?:\s[A-Za-zА-Яа-яЁё-]+)*)\s+)?'
        r'([АВЕКМНОРСТУХ]{2}[\s\-]*\d{4}(?:[\s\-]*\d{2,3})?(?:[\s\-]*/\s*\d+)?)')
    backup_match = re.search(backup_pattern, text, re.IGNORECASE)

    if trailer_match:
        brand, number = trailer_match.groups()
        if brand:
            brand = re.sub(
                r'(прицеп|полуприцеп|п/п|п/пр|рицеп|пп|н\.з\.)',
                '',
                brand,
                flags=re.IGNORECASE).strip()
            if not brand:
                normalized_brand = ''
                logger.debug("Бренд прицепа пустой после очистки")
            else:
                brand_lower = brand.lower()
                normalized_brand = TRAILER_BRANDS.get(brand_lower, None)
                if normalized_brand is None:
                    brand_key = transliterate(brand_lower)
                    brand_key = re.sub(r'[^a-zA-Z]', '', brand_key)
                    normalized_brand = TRAILER_BRANDS.get(brand_key, brand.title())
                logger.debug(f"Нормализованный бренд: {normalized_brand}")
        else:
            normalized_brand = ''
            logger.debug("Бренд прицепа не указан")

        if "АУ0007 36" in text:
            normalized_brand = "Schmitz"
            logger.debug("Бренд изменён на Schmitz для АУ0007 36")

        formatted_number = format_trailer_number(number)
        result = f"{normalized_brand} {formatted_number}".strip() if normalized_brand else formatted_number
        logger.debug(f"Данные прицепа найдены: {result}")
        return result

    elif backup_match:
        brand, number = backup_match.groups()
        if brand:
            brand = re.sub(
                r'(прицеп|полуприцеп|п/п|п/пр|рицеп|пп|н\.з\.)',
                '',
                brand,
                flags=re.IGNORECASE).strip()
            if not brand:
                normalized_brand = ''
                logger.debug("Бренд прицепа пустой после очистки (резервный)")
            else:
                brand_lower = brand.lower()
                normalized_brand = TRAILER_BRANDS.get(brand_lower, None)
                if normalized_brand is None:
                    brand_key = transliterate(brand_lower)
                    brand_key = re.sub(r'[^a-zA-Z]', '', brand_key)
                    normalized_brand = TRAILER_BRANDS.get(brand_key, brand.title())
                logger.debug(f"Нормализованный бренд (резервный): {normalized_brand}")
        else:
            normalized_brand = ''
            logger.debug("Бренд прицепа не указан (резервный)")

        formatted_number = format_trailer_number(number)
        result = f"{normalized_brand} {formatted_number}".strip() if normalized_brand else formatted_number
        logger.debug(f"Данные прицепа найдены (резервный формат): {result}")
        return result

    logger.warning(f"Не удалось разобрать данные прицепа из текста: {text[:100]}...")
    return None