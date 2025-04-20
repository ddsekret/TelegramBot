import os

# Путь к файлу parser.py
parser_path = r"C:\Users\Dsekr\TelegramBot\parser.py"

# Новый код для parser.py
new_code = """import re
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Определение недостающих констант
valid_letters = set("АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ")

TRAILER_BRANDS = {
    "шмитц": "Шмитц",
    "шмиц": "Шмитц",
    "крона": "Krone",
    "крон": "Krone",
    "кёгель": "Kögel",
    "кёгел": "Kögel",
    "kogel": "Kögel",
}

CAR_BRANDS = {
    "вольво": "ВОЛЬВО",
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
        r"(?:паспорт|пасп|п/п|серия\s*и\s*номer|серия|кем\s*выдан|паспорт_место_выдачи)\s*[:\-\s]*(?:\d{2}\s*\d{2}\s*(?:№\s*)?\d{6}|\d{4}\s*\d{6}|\d{4}\s*\d{3}\s*\d{3})?\s*(?:выдан|выдано|отделом|:\s*)?"
        r"(?:\d{1,2}\.\d{1,2}\.\d{4}(?:г\.?)?\s*)?(.+?)(?=\s*(?:д\.в\.?|дата\s*выдачи|"
        r"код|в/у|ву|водительское\s*удостоверение|права|тел\.?|телефон|а/м|прицеп|полуприцеп|"
        r"п/п|п/пр\.|перевозчик|$))",
        re.IGNORECASE
    )
    passport_place_match = passport_place_pattern.search(text)
    if passport_place_match:
        place = passport_place_match.group(1).strip()
        date_match = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4}(?:г\.?)?)", place)
        if date_match:
            place = place[:date_match.start()].strip()
        code_match = re.search(r"(\d{3}-\d{3})", place)
        if code_match:
            place = place.replace(code_match.group(1), "").strip()
        place = re.sub(
            r"^(выдан|выдано|кем\s*выдан|_место_выдачи|:)\s*",
            "",
            place,
            flags=re.IGNORECASE
        ).strip()
        logger.debug(f"Место выдачи найдено: {place}")
        if len(place) < 5 or place in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
            logger.debug("Место выдачи слишком короткое или некорректное, пропускаем")
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
        r"(?<!\d)(\+?\d[\d\s\-\(\)]{9,14})(?!\d)",
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

        if len(digits) == 11 and digits[0] in "78":
            formatted = f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
            phones.append(formatted)
        elif len(digits) == 10:
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
        trailer_match = re.search(
            r'(?:прицеп|полуприцеп|п/п|п/пр\.)\s*[:\-\s]*(?:([A-Za-zА-Яа-яЁё-]+)\s+)?'
            r'([А-ЯЁ]{2}\s*\d{4,6}\s*\d{0,2})',
            line,
            re.IGNORECASE
        )
        if trailer_match:
            brand, number = trailer_match.groups()
            if brand:
                brand = re.sub(r'(прицеп|полуприцеп|п/п|п/пр|рицеп)', '', brand, flags=re.IGNORECASE).strip()
            brand = TRAILER_BRANDS.get(brand.lower(), brand if brand else '') if brand else ''
            number = number.strip()
            result = f"{brand} {number}".strip() if brand else number
            logger.debug(f"Данные прицепа найдены: {result}")
            return result
        else:
            # Попробуем более гибкий формат для прицепов, например, "ЕТ 1913 50"
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
                number = number.strip()
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
            r'\b(автомобиль|машина|авто|а/м|мобиль|мобильмобиль|тягач|марка|гос\.?номer|:)\b',
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
            if all(l.upper() in valid_letters for l in (letter1 + letters2.upper())):
                number = f"{letter1.upper()}{digits}{letters2} {region}"
                brand = car_data[:number_match.start()].strip()
                brand_key = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', brand.lower())
                normalized_brand = CAR_BRANDS.get(brand_key, brand)
                result = f"{normalized_brand} {number}"
                logger.debug(f"Данные автомобиля найдены: {result}")
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
            match = re.search(
                r"(?:Паспорт|Серия|Данные водителя)\s*(?::|\s)*(?:серия\s*)?(?:номер\s*)?(\d{2}\s*\d{2}\s*(?:№\s*)?\d{6}|\d{4}\s*\d{6}|\d{4}\s*\d{3}\s*\d{3})",
                line,
                re.IGNORECASE
            )
            if match:
                series_number = match.group(1)
                series_number = re.sub(r'№\s*', '', series_number).strip()
                data["Паспорт_серия_и_номер"] = series_number
            
            place = parse_passport_issuing_authority(line)
            if place:
                data["Паспорт_место_выдачи"] = place
            
            date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", line)
            if date_match and validate_date(date_match.group(1)):
                data["Паспорт_дата_выдачи"] = date_match.group(1)
            
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
                r'([A-Za-zА-Яа-яЁё-]+)\s+([А-ЯЁ]\s*\d{3}\s*[А-ЯЁа-яё]{2}\s*\d{2,3})',
                line,
                re.IGNORECASE
            )
            if car_match:
                brand, number = car_match.groups()
                brand = re.sub(
                    r'\b(автомобиль|машина|авто|а/м|мобиль|мобильмобиль|тягач|марка|гос\.?номer|:)\b',
                    '',
                    brand,
                    flags=re.IGNORECASE
                ).strip()
                brand_key = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', brand.lower())
                normalized_brand = CAR_BRANDS.get(brand_key, brand)
                data["Автомобиль"] = f"{normalized_brand} {number}"
                logger.debug(f"Данные автомобиля найдены в строке без ключа: {data['Автомобиль']}")

    return data

def parse_carrier_data(text):
    """Парсит данные перевозчика из текста."""
    data = {}
    text = text.strip().replace('\n', ' ')
    
    patterns = {
        "Перевозчик": r"Перевозчик\s*:\s*(.+?)(?=\s*(?:Имя|Телефон|ИНN|$))",
        "Имя": r"Имя\s*:\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)",
        "Телефон": r"(?:Телефон\s*:\s*|Телефон\s+)?(\+?\d\s*\(?\d{3}\)?\s*\d{3}\-?\d{2}\-?\d{2}|\d\s*\d{3}\s*\d{3}\d{2}\d{2})",
        "ИНN": r"ИНN\s*(\d+)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if key == "Телефон":
                data[key] = parse_phone_numbers(match.group(1).strip())
            elif key == "Имя":
                data["Имя перевозчика"] = match.group(1).strip()
            else:
                data[key] = match.group(1).strip()

    if "Перевозчик" not in data:
        carrier_match = re.search(
            r"^(ООО|ИП|ОАО|ЗАО)\s+(.+?)(?=\s*(?:Имя|Телефон|ИНN|$))",
            text,
            re.IGNORECASE
        )
        if carrier_match:
            data["Перевозчик"] = f"{carrier_match.group(1)} {carrier_match.group(2).strip()}"

    return data
"""

# Обновление файла
with open(parser_path, 'w', encoding='utf-8') as f:
    f.write(new_code)

# Запуск тестов
os.system("cd C:\\Users\\Dsekr\\TelegramBot && python -m unittest tests\\test_driver_parsing.py -v > test_output.txt")
print("Тесты завершены. Результаты сохранены в test_output.txt")