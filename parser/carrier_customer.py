# parser/carrier_customer.py
from typing import Dict, Optional
from .imports_and_settings import re, logger
from .phone import parse_phone_numbers

def parse_carrier_data(text: str) -> Dict[str, Optional[str]]:
    """Parses carrier data from text."""
    data = {}
    text = text.strip().replace('\n', ' ')
    logger.debug(f"Входной текст для парсинга перевозчика: {text[:100]}...")

    phone = parse_phone_numbers(text)
    if phone:
        data["Контакт"] = phone
        logger.debug(f"Добавлен телефон в данные перевозчика: {phone}")

    inn_match = re.search(r"ИНН\s*[:\-\s]*(\d{10,12})", text, re.IGNORECASE)
    if inn_match:
        data["ИНН"] = inn_match.group(1).strip()
        logger.debug(f"Добавлен ИНН: {data['ИНН']}")

    carrier_pattern = r"(?:перевозчик|превозчик)\s*[:\-\s]*(.+?)(?=\s*(?:$|\n|водитель|паспорт|тел\.?|телефон|а/м|машина|автомобиль|прицеп|полуприцеп|p/п|п/пр\.|ИНН))"
    carrier_match = re.search(carrier_pattern, text, re.IGNORECASE)

    if carrier_match:
        carrier_text = carrier_match.group(1).strip()
        org_type_match = re.search(
            r"^(ООО|ИП|ОАО|ЗАО)",
            carrier_text,
            re.IGNORECASE)
        org_type = org_type_match.group(1).upper() if org_type_match else None
        name = carrier_text[len(org_type):].strip() if org_type else carrier_text

        if name:
            name = re.sub(r'[\'\"«»]', '', name).strip()
            name = re.sub(r"\b(перевозчик|имя|телефон)\b", "", name, flags=re.IGNORECASE).strip()

            if org_type == "ИП":
                name_parts = name.split()
                if len(name_parts) >= 2:
                    full_name = ' '.join(word.capitalize() for word in name_parts)
                    data["Перевозчик"] = f"ИП {full_name}"
                    data["Короткое название"] = name_parts[0].capitalize()
                    if phone and len(name_parts) > 1:
                        data["Контакт"] = f"{full_name} {phone}"
                else:
                    data["Перевозчик"] = f"ИП {name.capitalize()}"
                    data["Короткое название"] = name.capitalize()
            else:
                formatted_name = ' '.join(word.capitalize() for word in name.split())
                data["Перевозчик"] = f"{org_type} {formatted_name}" if org_type else formatted_name
                data["Короткое название"] = formatted_name
        else:
            data["Перевозчик"] = org_type if org_type else "Не указано"
            data["Короткое название"] = "Не указано"
        logger.debug(f"Добавлено название перевозчика: {data['Перевозчик']}, короткое: {data['Короткое название']}")
    else:
        data["Перевозчик"] = "Не указано"
        data["Короткое название"] = "Не указано"
        logger.debug("Название перевозчика не найдено")

    logger.debug(f"Результат парсинга перевозчика: {data}")
    return data

def parse_customer_data(text: str) -> Dict[str, Optional[str]]:
    """Парсит данные фирмы-заказчика из текста."""
    logger.debug(f"Парсинг данных фирмы-заказчика: {text[:100]}...")
    text = re.sub(r'\n+', ' ', text)
    data = {}

    name_match = re.search(
        r"Название\s*:\s*(.+?)(?=\s*(?:ИНН|Короткое\s+название|$))",
        text,
        re.IGNORECASE)
    inn_match = re.search(r"ИНН\s*:\s*(\d{10,12})", text, re.IGNORECASE)
    short_name_match = re.search(
        r"Короткое\s+название\s*:\s*(.+?)(?=\s*(?:$|\.))",
        text,
        re.IGNORECASE)

    if name_match and inn_match:
        full_name = name_match.group(1).strip().rstrip('.').rstrip(',')
        quoted_name_match = re.search(r'[\'\"«](.+?)[\'\"»]', full_name)
        if quoted_name_match:
            name = quoted_name_match.group(1).strip()
            org_type_match = re.search(r"^(ООО|ИП|ОАО|ЗАО)", full_name, re.IGNORECASE)
            org_type = org_type_match.group(1).upper() if org_type_match else ""
            full_name = f"{org_type} {name}" if org_type else name
        data["Название"] = ' '.join(word.capitalize() for word in full_name.split())
        data["ИНН"] = inn_match.group(1).strip()

        if short_name_match:
            short_name = short_name_match.group(1).strip().rstrip('.')
            short_name = re.sub(r"\(без\s+ИП\)\.\s*", "", short_name, flags=re.IGNORECASE).strip()
            short_name = re.sub(r"без\s+инициалов", "", short_name, flags=re.IGNORECASE).strip()
            data["Короткое название"] = short_name.capitalize()
        else:
            org_type_match = re.search(r"^(ООО|ИП|ОАО|ЗАО)", full_name, re.IGNORECASE)
            org_type = org_type_match.group(1).upper() if org_type_match else ""
            if org_type:
                name_without_type = full_name[len(org_type):].strip()
                if org_type == "ИП":
                    name_parts = name_without_type.split()
                    short_name = name_parts[0] if name_parts else name_without_type
                else:
                    short_name = name_without_type
                data["Короткое название"] = short_name.capitalize()
            else:
                data["Короткое название"] = data["Название"].capitalize()
        logger.debug(f"Добавлено название заказчика: {data['Название']}, ИНН: {data['ИНН']}, короткое: {data['Короткое название']}")
    else:
        org_type_match = re.search(r"^(ООО|ИП|ОАО|ЗАО)", text, re.IGNORECASE)
        org_type = org_type_match.group(1).upper() if org_type_match else ""

        name_match = re.search(r"(.+?)(?=\s*(?:ИНН|[\+8]\d{10,11}|$))", text, re.IGNORECASE)
        if name_match:
            full_name = name_match.group(1).strip()
            quoted_name_match = re.search(r'[\'\"«](.+?)[\'\"»]', full_name)
            if quoted_name_match:
                name = quoted_name_match.group(1).strip()
                full_name = f"{org_type} {name}" if org_type else name
            data["Название"] = ' '.join(word.capitalize() for word in full_name.split()) if full_name else ""

            if org_type:
                name_without_type = full_name[len(org_type):].strip()
                if org_type == "ИП":
                    name_parts = name_without_type.split()
                    short_name = name_parts[0] if name_parts else name_without_type
                else:
                    short_name = name_without_type
                data["Короткое название"] = short_name.capitalize()
            else:
                data["Короткое название"] = data.get("Название", "").capitalize()

        inn_match = re.search(r"ИНН\s*(\d{10,12})", text, re.IGNORECASE)
        if inn_match:
            data["ИНН"] = inn_match.group(1).strip()
        logger.debug(f"Добавлено название заказчика (альтернативный формат): {data.get('Название', 'Не указано')}, ИНН: {data.get('ИНН', 'Не указано')}")

    logger.debug(f"Результат парсинга фирмы-заказчика: {data}")
    return data