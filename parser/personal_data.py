# parser/personal_data.py
from typing import Dict, Optional
from datetime import datetime
from .imports_and_settings import re, logger, COMPOSITE_CITIES, CITY_NOMINATIVE, SMALL_WORDS

def validate_date(date_str: str) -> bool:
    """Проверяет, является ли строка валидной датой в формате ДД.ММ.ГГГГ."""
    try:
        datetime.strptime(date_str, '%d.%m.%Y')
        return True
    except ValueError:
        return False

def parse_driver_name(text: str) -> Optional[str]:
    """Извлекает ФИО водителя из текста."""
    logger.debug(f"Поиск ФИО водителя в тексте: {text[:100]}...")
    text = re.sub(r'\s+', ' ', text).strip()

    lines = text.split('\n')

    for line in lines:
        line = line.strip()
        logger.debug(f"Проверяем строку: {line}")
        name_match = re.match(
            r"(?:водитель|ф\.и\.о\. водителя|ф\.и\.о\.|фио\s*водителя|вод\.)\s*(?::|\s|-)*\s*([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,3}(?:\.)?)",
            line,
            re.IGNORECASE
        )
        if name_match:
            name = name_match.group(1).strip()
            name = re.sub(r'\.$', '', name).strip()
            name = re.sub(
                r'\s*(?:Паспорт|Серия|Данные|Телефон|тел\.?|Тел\.?|Мобильный|Д\/р|Дата\s*рождения)\b.*$',
                '',
                name,
                flags=re.IGNORECASE).strip()
            logger.debug(f"ФИО найдено (по строке): {name}")
            return name
        else:
            logger.debug(f"Совпадение по строке не найдено: {line}")

    name_match = re.search(
        r"(?:водитель|ф\.и\.о\. водителя|ф\.и\.о\.|фио\s*водителя|вод\.)\s*(?::|\s|-)*\s*([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,3}(?:\.)?)(?=\s|$|\n|д\.р\.|д\/р|г\.р\.|паспорт|серия|тел\.?|телефон|мобильный\s*водителя|в/у|ву|водительское\s*удостоверение|права|а/м|машина|автомобиль|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|прописан|данные|кем\s*выдан|вод\.|выдано|дата\s*рождения)",
        text,
        re.IGNORECASE
    )
    if name_match:
        name = name_match.group(1).strip()
        name = re.sub(r'\.$', '', name).strip()
        name = re.sub(
            r'\s*(?:Паспорт|Серия|Данные|Телефон|тел\.?|Тел\.?|Мобильный|Д\/р|Дата\s*рождения)\b.*$',
            '',
            name,
            flags=re.IGNORECASE).strip()
        logger.debug(f"ФИО найдено (по общему тексту): {name}")
        return name
    else:
        logger.debug("Совпадение по общему тексту не найдено")

    logger.debug("ФИО не найдено")
    return None

def parse_birth_data(text: str) -> Dict[str, Optional[str]]:
    """Извлекает дату и место рождения."""
    logger.debug(f"Поиск даты и места рождения в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    data = {}

    # Получаем дату выдачи паспорта и ВУ для исключения
    passport_date_pattern = re.compile(
        r"(?:паспорт|пасп|п/п|серия\s*и\s*номer|серия|данные\s*водителя|выдан).*?"
        r"(\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)
    passport_date_match = passport_date_pattern.search(text)
    passport_date = passport_date_match.group(1) if passport_date_match and validate_date(passport_date_match.group(1)) else None

    vu_date_pattern = re.compile(
        r"(?:в/у|ву|водительское\s*удостоверение|права|вод\.?\s*уд\.?|вод\.?\s*удост\.?).*?"
        r"(?:выдано|дата\s*выдачи|от)\s*(?::\s*)?[^0-9]*(\d{2}\.\d{2}\.\d{4})",
        re.IGNORECASE)
    vu_date_match = vu_date_pattern.search(text)
    vu_date = vu_date_match.group(1) if vu_date_match and validate_date(vu_date_match.group(1)) else None

    # Проверка на правдоподобность даты рождения
    def is_plausible_birth_date(date_str: str) -> bool:
        try:
            date = datetime.strptime(date_str, '%d.%m.%Y')
            current_date = datetime.now()
            age_years = (current_date - date).days // 365
            return 16 <= age_years <= 100  # Водитель должен быть от 16 до 100 лет
        except ValueError:
            return False

    # Сначала ищем дату рождения с явными ключевыми словами (приоритет)
    birth_date_priority_pattern = re.compile(
        r"(?:д\.?р\.?|дата\s*р\s*ождения|г\.?р\.?)\s*(?:[:\s-]*\s*)?(\d{2}[\.\-]\d{2}[\.\-]\d{4}(?:\s*г\.?)?)",
        re.IGNORECASE)
    birth_date_priority_match = birth_date_priority_pattern.search(text)
    if birth_date_priority_match:
        birth_date = birth_date_priority_match.group(1).replace("-", ".")
        birth_date = re.sub(r'\s*г\.?$', '', birth_date).strip()
        if validate_date(birth_date) and is_plausible_birth_date(birth_date):
            logger.debug(f"Дата рождения (приоритетная): {birth_date}")
            data["Дата_рождения"] = birth_date
        else:
            logger.debug(f"Дата рождения (приоритетная) не прошла валидацию: {birth_date}")

    # Если приоритетная дата не найдена, ищем другие даты
    if "Дата_рождения" not in data:
        patterns = [
            r"(?:[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)\s+(\d{2}[\.\-]\d{2}[\.\-]\d{4}(?:\s*г\.?)?)(?=\s|$|\n|паспорт|тел\.?|телефон|а/м|прицеп|перевозчик)",
            r"\b(\d{2}[\.\-]\d{2}[\.\-]\d{4}(?:\s*г\.?)?)\b"]
        birth_date_fallback_matches = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                match = match if isinstance(match, str) else match[0]
                match = match.replace("-", ".").strip()
                match = re.sub(r'\s*г\.?$', '', match).strip()
                if validate_date(match) and is_plausible_birth_date(match):
                    birth_date_fallback_matches.append(match)
        logger.debug(f"Найденные даты рождения: {birth_date_fallback_matches}")

        if birth_date_fallback_matches:
            for birth_date in birth_date_fallback_matches:
                if (not passport_date or birth_date != passport_date) and (not vu_date or birth_date != vu_date):
                    logger.debug(f"Дата рождения (без совпадений): {birth_date}")
                    data["Дата_рождения"] = birth_date
                    break
            else:
                logger.debug("Все найденные даты совпадают с датой выдачи паспорта или ВУ, пропускаем")
        else:
            logger.debug("Дата рождения не найдена")

    birth_place_pattern = re.compile(
        r"место\s*рождения\s*[:\-\s]*(.+?)(?=\s*(?:дата|код|в/у|ву|водительское\s*удостоверение|права|тел\.?|телефон|а/м|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|$))",
        re.IGNORECASE)
    birth_place_match = birth_place_pattern.search(text)
    birth_place = birth_place_match.group(1).strip() if birth_place_match else None
    if birth_place:
        logger.debug(f"Место рождения: {birth_place}")
        data["Место_рождения"] = birth_place

    return data

def parse_citizenship(text: str) -> Optional[str]:
    """Извлекает гражданство."""
    logger.debug(f"Поиск гражданства в тексте: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    citizenship_pattern = re.compile(
        r"гражданство\s*[:\-\s]*(.+?)(?=\s*(?:дата|код|в/у|ву|водительское\s*удостоверение|права|тел\.?|телефон|а/м|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|$))",
        re.IGNORECASE)
    citizenship_match = citizenship_pattern.search(text)
    citizenship = citizenship_match.group(1).strip() if citizenship_match else None
    if citizenship:
        logger.debug(f"Гражданство: {citizenship}")
    return citizenship

def parse_residence(text: str) -> Optional[str]:
    """Извлекает место жительства."""
    logger.debug(f"Поиск места жительства в тексте: {text}")
    text = re.sub(r'\n+', ' ', text)
    residence_pattern = re.compile(
        r"(?:проживает|проживает\s*по\s*адресу|адрес\s*проживания|прописан|прописка|адрес\s*регистрации|Зарегистрирован|Регистрация)\s*[:\-\s]*(.+?)(?=\s*(?:$|\n|паспорт|тел\.?|телефон|перевозчик|вод\.|в/у|ву|водительское\s*удостоверение|права|дата\s*рождения|код\s*подразделения|а/м|прицеп|гос\s*номер))",
        re.IGNORECASE
    )
    residence_match = residence_pattern.search(text)
    residence = residence_match.group(1).strip() if residence_match else None
    if residence:
        # Удаляем запятые
        residence = re.sub(r',', '', residence)
        # Исправляем "г.Коломна" на "г. Коломна"
        residence = re.sub(r'\bг\.([А-Яа-яЁё])', r'г. \1', residence)
        # Исправляем "пос.Затеречный" на "пос. Затеречный", но не добавляем лишние пробелы внутри слов
        residence = re.sub(r'\b(пос\.?|кв\.?|ул\.?|д\.?)\s*([А-Яа-яЁё0-9])', r'\1 \2', residence, flags=re.IGNORECASE)
        # Исправляем "квартира"
        residence = re.sub(r'\bкв\s+артира\b', 'квартира', residence, flags=re.IGNORECASE)
        # Исправляем "д.16" на "д. 16"
        residence = re.sub(r'\bд\.(\d+)', r'д. \1', residence, flags=re.IGNORECASE)
        # Удаляем код подразделения
        residence = re.sub(r'\s*Код\s*подразделения\s*\d{3}-\d{3}\b', '', residence, flags=re.IGNORECASE)
        # Нормализуем составные города
        for key, value in COMPOSITE_CITIES.items():
            residence = re.sub(rf'\b{key}\b', value, residence, flags=re.IGNORECASE)
        # Приводим города к именительному падежу
        for key, value in CITY_NOMINATIVE.items():
            residence = re.sub(rf'\b{key}\b', value, residence, flags=re.IGNORECASE)
        # Удаляем лишние пробелы
        residence = re.sub(r'\s+', ' ', residence).strip()
        # Удаляем точку в конце
        residence = re.sub(r'\.\s*$', '', residence)
        # Приводим "кв." к нижнему регистру
        residence = re.sub(r'\bкв\.(\s|$)', r'кв.\1', residence, flags=re.IGNORECASE)
        # Форматируем регистр слов
        words = residence.split()
        formatted_address = []
        for i, word in enumerate(words):
            if word.lower() in SMALL_WORDS and not word.startswith('кв'):
                formatted_address.append(word.lower())
            elif word in ['ул.', 'д.', 'пос.']:
                formatted_address.append(word.lower())
            elif word.lower() == 'м.' and i > 0 and words[i-1].lower() == 'ул.':
                formatted_address.append('М.')
            else:
                formatted_address.append(word)
        residence = ' '.join(formatted_address)
        # Исправляем "Дом." на "дом."
        residence = re.sub(r'\bДом\.(\s|$)', r'дом.\1', residence, flags=re.IGNORECASE)
        logger.debug(f"Место жительства: {residence}")
    else:
        logger.debug("Место жительства не найдено")
    return residence