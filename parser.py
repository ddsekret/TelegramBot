import re
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Определение констант
valid_letters = set("АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ")

TRAILER_BRANDS = {
    "шмитц": "ШМИТЦ",
    "шмиц": "ШМИТЦ",
    "крона": "Krone",
    "крон": "Krone",
    "кёгель": "Kögel",
    "кёгел": "Kögel",
    "kogel": "Kögel",
}

CAR_BRANDS = {
    "вольво": "Вольво",
    "волво": "volvo",
    "скания": "Скания",
    "ман": "MAN",
    "мерседес": "MERSEDES-BENZ",
    "мерседес-бенз": "MERSEDES-BENZ",
    "даф": "ДАФ",
    "фотон": "Фотон",
}

def validate_date(date_str):
    """Проверяет, является ли строка валидной датой в формате ДД.ММ.ГГГГ."""
    try:
        date_parts = date_str.split('.')
        if len(date_parts) != 3:
            return False
        day, month, year = map(int, date_parts)
        datetime(year, month, day)
        return True
    except (ValueError, TypeError):
        return False

def parse_passport_issuing_authority(text):
    """Извлекает место выдачи паспорта из текста."""
    logger.debug(f"Поиск места выдачи паспорта в тексте: {text[:100]}...")
    passport_place_pattern = re.compile(
        r"(?:выдан|выдано|кем\s*выдан)\s*[:\-\s]*(.+?)(?=\s*(?:д\.в\.?|дата\s*выдачи|код|в/у|ву|водительское\s*удостоверение|права|тел\.?|телефон|а/м|прицеп|полуприцеп|p/п|п/пр\.|перевозчик|$))",
        re.IGNORECASE
    )
    passport_place_match = passport_place_pattern.search(text)
    if passport_place_match:
        place = passport_place_match.group(1).strip()
        # Удаляем дату, код подразделения или другие ненужные части
        place = re.sub(r"\d{1,2}\.\d{1,2}\.\d{4}(?:г\.?)?|\d{3}-\d{3}", "", place).strip()
        place = re.sub(r"^(выдан|выдано|кем\s*выдан|_место_выдачи|серия\s*и\s*номer|серия|:\s*)", "", place, flags=re.IGNORECASE).strip()
        logger.debug(f"Место выдачи найдено: {place}")
        if len(place) < 5:
            logger.debug("Место выдачи слишком короткое, пропускаем")
            return None
        return place
    logger.debug("Место выдачи паспорта не найдено")
    return None

def parse_phone_numbers(text):
    """Извлекает и форматирует номера телефонов из текста."""
    logger.debug(f"Поиск телефона в тексте: {text}")
    text = re.sub(r'\s+', ' ', text).strip()
    phones = []

    vu_match = re.search(
        r"(?:в/у|ву|водительское\s*удостоверение|права|вод\.уд\.)\s*(?:№\s*)?"
        r"(\d{2}\s*\d{2}\s*\d{6}|\d{10}|\d{4}\s*\d{6})",
        text,
        re.IGNORECASE
    )
    vu_number = re.sub(r"\s+", "", vu_match.group(1)) if vu_match else None
    logger.debug(f"Найден номер ВУ для фильтрации: {vu_number}")

    passport_match = re.search(
        r"(?:паспорт|пасп|п/п|серия\s*и\s*номer|серия|данные\s*водителя)\s*(?:серия\s*)?"
        r"[:\-\s]*(?:№\s*|номер\s*)?(\d{2}\s*\d{2}|\d{4})\s*(?:№\s*|номер\s*)?(\d{6})",
        text,
        re.IGNORECASE
    )
    passport_number = f"{passport_match.group(1).replace(' ', '')}{passport_match.group(2)}" if passport_match else None
    logger.debug(f"Найден номер паспорта для фильтрации: {passport_number}")

    phone_pattern = re.compile(
        r"(?:тел\.?|телефон|\+7|8)[\s:-]*(\+?\d[\d\s\-\(\)]{9,14})|"
        r"(?<!\d)(\+?[\d\s\-\(\)]{10,14})(?!\d)",
        re.IGNORECASE
    )
    phone_matches = phone_pattern.finditer(text)
    for phone_match in phone_matches:
        phone = phone_match.group(1) or phone_match.group(2)
        logger.debug(f"Найден телефон (перед фильтрацией): {phone}")
        digits = re.sub(r"[^\d]", "", phone)

        if vu_number and digits == vu_number:
            logger.debug(f"Телефон {phone} совпадает с номером ВУ: {vu_number}")
            continue
        if passport_number and digits == passport_number:
            logger.debug(f"Телефон {phone} совпадает с номером паспорта: {passport_number}")
            continue

        if len(digits) in (10, 11):
            if digits[0] in "78":
                digits = digits[1:]  # Убираем 7 или 8
            formatted = f"+7 ({digits[0:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:10]}"
            phones.append(formatted)
        else:
            logger.debug(f"Некорректная длина номера телефона: {digits}")

    if phones:
        logger.debug(f"Найдены телефоны: {', '.join(phones)}")
        return ', '.join(phones)
    logger.debug("Телефоны не найдены")
    return None

def parse_trailer_data(text):
    """Извлекает данные о прицепе (бренд и номер)."""
    logger.debug(f"Поиск данных прицепа в тексте: {text[:100]}...")
    lines = text.strip().split('\n')
    for line in lines:
        line = line.strip()
        # Основной формат: АА 1234 12
        trailer_match = re.search(
            r'(?:прицеп|полуприцеп|п/п|п/пр\.)\s*[:\-\s]*(?:([A-Za-zА-Яа-яЁё-]+)\s+)?'
            r'([А-ЯЁ]{2}\s*\d{4}\s*\d{0,2})',
            line,
            re.IGNORECASE
        )
        if trailer_match:
            brand, number = trailer_match.groups()
            if brand:
                brand = re.sub(r'(прицеп|полуприцеп|п/п|п/пр|рицеп)', '', brand, flags=re.IGNORECASE).strip()
            brand = TRAILER_BRANDS.get(brand.lower(), brand if brand else '') if brand else ''
            number = number.replace(" ", "")
            # Форматируем номер: АА 1234 12
            number_parts = re.match(r"([А-ЯЁ]{2})(\d{4})(\d{0,2})", number)
            if number_parts:
                letter, digits, region = number_parts.groups()
                number = f"{letter} {digits} {region}"
            result = f"{brand} {number}".strip() if brand else number
            logger.debug(f"Данные прицепа найдены: {result}")
            return result
        else:
            # Гибкий формат: ЕТ 1913 50
            trailer_match = re.search(
                r'(?:прицеп|полуприцеп|п/п|п/пр\.)\s*[:\-\s]*(?:([A-Za-zА-Яа-яЁё-]+)\s+)?'
                r'([А-ЯЁ]{2}\s*\d{4}\s*\d{2})',
                line,
                re.IGNORECASE
            )
            if trailer_match:
                brand, number = trailer_match.groups()
                if brand:
                    brand = re.sub(r'(прицеп|полуприцеп|п/п|п/пр|рицеп)', '', brand, flags=re.IGNORECASE).strip()
                brand = TRAILER_BRANDS.get(brand.lower(), brand if brand else '') if brand else ''
                number = number.replace(" ", "")
                # Форматируем номер: ЕТ 1913 50
                number_parts = re.match(r"([А-ЯЁ]{2})(\d{4})(\d{2})", number)
                if number_parts:
                    letter, digits, region = number_parts.groups()
                    number = f"{letter} {digits} {region}"
                result = f"{brand} {number}".strip() if brand else number
                logger.debug(f"Данные прицепа найдены (гибкий формат): {result}")
                return result
    logger.debug("Данные прицепа не найдены")
    return None

def parse_car_data(text):
    """Извлекает данные об автомобиле (бренд и номер)."""
    logger.debug(f"Поиск данных автомобиля в тексте: {text[:100]}...")
    car_match = re.search(
        r'(?:машина|авто|автомобиль|а/м|тягач|тс|марка\s*,\s*гос\.?номer)\s*[:\-\s\/]*'
        r'(.+?)(?=\s*(?:прицеп|полуприцеп|п/п|п/пр\.|перевозчик|тел\.?|телефон|$))',
        text,
        re.IGNORECASE
    )
    if car_match:
        car_data = car_match.group(1).strip()
        car_data = re.sub(
            r'\b(автомобиль|машина|авто|а/м|мобиль|мобильмобиль|тягач|марка|гос\.?номer|№\s*|:)\b',
            '',
            car_data,
            flags=re.IGNORECASE
        ).strip()
        number_match = re.search(
            r"([А-ЯЁ])\s*(\d{3})\s*([А-ЯЁа-яё]{2})\s*(\d{2,3})$",
            car_data,
            re.IGNORECASE
        )
        if number_match:
            letter1, digits, letters2, region = number_match.groups()
            if all(l.upper() in valid_letters for l in (letter1 + letters2)):
                number = f"{letter1}{digits}{letters2}{region}"
                if "№" in car_data:
                    number = f"№ {letter1} {digits} {letters2} {region}"
                else:
                    number = f"{letter1} {digits} {letters2} {region}"
                brand = car_data[:number_match.start()].strip()
                brand_key = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', brand.lower())
                normalized_brand = CAR_BRANDS.get(brand_key, brand)
                result = f"{normalized_brand} {number}"
                logger.debug(f"Данные автомобиля найдены: {result}")
                return result
    else:
        # Попробуем извлечь автомобиль без явного ключа
        car_match = re.search(
            r'([A-Za-zА-Яа-яЁё-]+)\s+(?:№\s*)?([А-ЯЁ]\s*\d{3}\s*[А-ЯЁа-яё]{2}\s*\d{2,3})',
            text,
            re.IGNORECASE
        )
        if car_match:
            brand, number = car_match.groups()
            brand = re.sub(
                r'\b(автомобиль|машина|авто|а/м|мобиль|мобильмобиль|тягач|марка|гос\.?номer|№\s*|:)\b',
                '',
                brand,
                flags=re.IGNORECASE
            ).strip()
            brand_key = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', brand.lower())
            normalized_brand = CAR_BRANDS.get(brand_key, brand)
            number = number.replace(" ", "")
            number_parts = re.match(r"([А-ЯЁ])(\d{3})([А-ЯЁа-яё]{2})(\d{2,3})", number, re.IGNORECASE)
            if number_parts:
                letter1, digits, letters2, region = number_parts.groups()
                number = f"{letter1}{digits}{letters2}{region}"
                if "№" in text:
                    number = f"№ {letter1} {digits} {letters2} {region}"
                else:
                    number = f"{letter1} {digits} {letters2} {region}"
                result = f"{normalized_brand} {number}"
                logger.debug(f"Данные автомобиля найдены в строке без ключа: {result}")
                return result
    logger.debug("Данные автомобиля не найдены")
    return None

def parse_driver_data(text):
    """Парсит данные водителя из текста."""
    data = {}
    text = text.strip()

    normalized_text = text.replace("Ф.И.О. водителя", "Водитель") \
                         .replace("Ф.И.О.", "Водитель") \
                         .replace("водитель", "Водитель") \
                         .replace("паспорт", "Паспорт") \
                         .replace("Тел.", "Телефон") \
                         .replace("тел.", "Телефон") \
                         .replace("Контактный телефон", "Телефон") \
                         .replace("Телефон водителя", "Телефон") \
                         .replace("№ телефона водителя", "Телефон") \
                         .replace("Права", "Водительское удостоверение") \
                         .replace("Вод.уд.", "Водительское удостоверение") \
                         .replace("Вод. Уд.", "Водительское удостоверение") \
                         .replace("ВУ", "Водительское удостоверение") \
                         .replace("Авто", "Автомобиль") \
                         .replace("а/м", "Автомобиль") \
                         .replace("машина", "Автомобиль") \
                         .replace("А/М – тягач (марка, гос.номer)", "Автомобиль") \
                         .replace("Полуприцеп", "Прицеп") \
                         .replace("прицеп", "Прицеп") \
                         .replace("п/п", "Прицеп") \
                         .replace("П/прицеп", "Прицеп") \
                         .replace("Прописка", "Адрес регистрации") \
                         .replace("прописан", "Адрес регистрации") \
                         .replace("Зарегистрирован", "Адрес регистрации") \
                         .replace("Кем выдан", "Паспорт_место_выдачи")

    lines = normalized_text.split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("Водитель") and "Водительское удостоверение" not in line:
            match = re.match(r"Водитель\s*(?::|\s)*(.+)", line, re.IGNORECASE)
            if match:
                data["Водитель"] = match.group(1).strip()

        elif line.startswith(("Паспорт", "Серия", "Данные водителя")):
            # Извлечение серии и номера паспорта
            match = re.search(
                r"(?:Паспорт|Серия|Данные\s*водителя)\s*(?::|\s|-)*\s*(?:серия\s*)?(?:№\s*)?(\d{2}\s*\d{2}\s*\d{6}|\d{4}\s*\d{6}|\d{4}\s*\d{3}\s*\d{3})",
                line,
                re.IGNORECASE
            )
            if match:
                series_number = match.group(1).strip()
                series_number = re.sub(r'\s+', ' ', series_number)  # Нормализуем пробелы
                series_number = re.sub(r'№\s*', '', series_number)  # Убираем №
                data["Паспорт_серия_и_номер"] = series_number
            
            # Извлечение места выдачи
            place = parse_passport_issuing_authority(line)
            if place:
                data["Паспорт_место_выдачи"] = place
            
            # Извлечение даты выдачи
            date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", line)
            if date_match and validate_date(date_match.group(1)):
                data["Паспорт_дата_выдачи"] = date_match.group(1)
            
            # Извлечение кода подразделения
            code_match = re.search(r"код\s*подразделения\s*(?::|\s)*(\d{3}-\d{3})", line, re.IGNORECASE)
            if code_match:
                data["Паспорт_код_подразделения"] = code_match.group(1)

        elif line.startswith("Дата выдачи"):
            match = re.search(r"Дата\s*выдачи\s*(?::|\s)*(\d{2}\.\d{2}\.\d{4})[г.]?", line, re.IGNORECASE)
            if match and validate_date(match.group(1)):
                data["Паспорт_дата_выдачи"] = match.group(1)

        elif line.startswith("Паспорт_место_выдачи"):
            match = re.match(r"Паспорт_место_выдачи\s*(?::|\s)*(.+)", line, re.IGNORECASE)
            if match:
                data["Паспорт_место_выдачи"] = match.group(1).strip()

        elif line.startswith("код подразделения"):
            match = re.search(r"код\s*подразделения\s*(?::|\s)*(\d{3}-\d{3})", line, re.IGNORECASE)
            if match:
                data["Паспорт_код_подразделения"] = match.group(1)

        elif line.startswith("Водительское удостоверение"):
            match = re.search(
                r"Водительское удостоверение\s*(?::|\s)*(\d{2}\s*\d{2}\s*\d{6}|\d{10}|\d{4}\s*\d{6})",
                line,
                re.IGNORECASE
            )
            if match:
                data["ВУ_серия_и_номер"] = match.group(1).strip()
                date_match = re.search(r"дата\s*выдачи\s*(\d{2}\.\d{2}\.\d{4})", line, re.IGNORECASE)
                if date_match:
                    data["В/У_дата_срок"] = date_match.group(1)

        elif line.startswith("Адрес регистрации"):
            match = re.match(r"Адрес регистрации\s*(?::|\s)*(.+)", line, re.IGNORECASE)
            if match:
                data["Адрес регистрации"] = match.group(1).strip()

        elif line.startswith("Телефон"):
            phone = parse_phone_numbers(line)
            if phone:
                data["Телефон"] = phone

        elif line.startswith("Автомобиль"):
            car = parse_car_data(line)
            if car:
                data["Автомобиль"] = car

        elif line.startswith("Прицеп"):
            trailer = parse_trailer_data(line)
            if trailer:
                data["Прицеп"] = trailer

        elif line.startswith("перевозчик"):
            match = re.match(r"перевозчик\s*(?::|\s)*(.+)", line, re.IGNORECASE)
            if match:
                data["Перевозчик"] = match.group(1).strip()

        else:
            car_match = re.search(
                r'([A-Za-zА-Яа-яЁё-]+)\s+(?:№\s*)?([А-ЯЁ]\s*\d{3}\s*[А-ЯЁа-яё]{2}\s*\d{2,3})',
                line,
                re.IGNORECASE
            )
            if car_match:
                brand, number = car_match.groups()
                brand = re.sub(
                    r'\b(автомобиль|машина|авто|а/м|мобиль|мобильмобиль|тягач|марка|гос\.?номer|№\s*|:)\b',
                    '',
                    brand,
                    flags=re.IGNORECASE
                ).strip()
                brand_key = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', brand.lower())
                normalized_brand = CAR_BRANDS.get(brand_key, brand)
                number = number.replace(" ", "")
                number_parts = re.match(r"([А-ЯЁ])(\d{3})([А-ЯЁа-яё]{2})(\d{2,3})", number, re.IGNORECASE)
                if number_parts:
                    letter1, digits, letters2, region = number_parts.groups()
                    number = f"{letter1}{digits}{letters2}{region}"
                    if "№" in line:
                        number = f"№ {letter1} {digits} {letters2} {region}"
                    else:
                        number = f"{letter1} {digits} {letters2} {region}"
                data["Автомобиль"] = f"{normalized_brand} {number}"
                logger.debug(f"Данные автомобиля найдены в строке без ключа: {data['Автомобиль']}")

    return data

def parse_carrier_data(text):
    """Парсит данные перевозчика из текста."""
    data = {}
    text = text.strip().replace('\n', ' ')
    logger.debug(f"Парсинг данных перевозчика: {text[:100]}...")

    # Извлечение ФИО для ИП
    fio_match = re.search(
        r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)",
        text,
        re.IGNORECASE
    )
    if fio_match and "ИП" in text.upper():
        data["Имя перевозчика"] = fio_match.group(1).strip()

    # Извлечение типа организации и названия
    carrier_match = re.search(
        r"(ООО|ИП|ОАО|ЗАО)\s+(.+?)(?=\s*(?:ИНН|Телефон|[\+8]\d{10,11}|$))",
        text,
        re.IGNORECASE
    )
    if carrier_match:
        org_type, name = carrier_match.groups()
        name = name.strip()
        if org_type.upper() == "ИП" and fio_match:
            data["Перевозчик"] = f"ИП {fio_match.group(1).strip()}"
        else:
            name = re.sub(
                r"(?:\+?\d\s*\(?\d{3}\)?\s*\d{3}\-?\d{2}\-?\d{2}|\d{10,12})",
                "",
                name
            ).strip()
            data["Перевозчик"] = f"{org_type} {name}"

    # Извлечение телефона
    phone = parse_phone_numbers(text)
    if phone:
        data["Телефон"] = phone

    # Извлечение ИНН
    inn_match = re.search(r"ИНН\s*(\d{10,12})", text, re.IGNORECASE)
    if inn_match:
        data["ИНН"] = inn_match.group(1).strip()

    logger.debug(f"Результат парсинга перевозчика: {data}")
    return data