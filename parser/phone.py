# parser/phone.py
from typing import Optional
from .imports_and_settings import re, logger

def parse_phone_numbers(text: str) -> Optional[str]:
    """Извлекает и форматирует номера телефонов из текста."""
    logger.debug(f"Поиск телефона в тексте: {text}")
    text = re.sub(r'\s+', ' ', text).strip()

    # Обновлённый шаблон для извлечения номеров
    phone_pattern = r"(?:(?:\+7|8)\s*(?:[\(\-]\s*)?\d{3}(?:[\)\-]\s*)?(?:\d{3}(?:[\-]\s*)?\d{2}(?:[\-]\s*)?\d{2}|\d{2}(?:[\-]\s*)?\d{2}(?:[\-]\s*)?\d{3}|\d{7})|\+7\s*\d{10}|\d{10,11}|(?:\+7|8)\s*\d{3}\s*\d{3}\s*\d{2}\s*\d{2}|(?:\+7|8)\s*\(\d{3,5}\)\s*\d{3}(?:[\-]\s*)?\d{2}(?:[\-]\s*)?\d{2})"
    phones = re.findall(phone_pattern, text)

    formatted_phones = []
    for phone in phones:
        phone_clean = re.sub(r'\D', '', phone)
        # Проверяем длину номера и формат
        if (phone_clean.startswith('9') and len(phone_clean) == 10) or \
           (phone_clean.startswith('8') and len(phone_clean) == 11):
            phone_clean = '7' + phone_clean[1:] if phone_clean.startswith('8') else '7' + phone_clean
        elif len(phone_clean) == 10 and phone_clean[0] in '345':
            phone_clean = '7' + phone_clean
        if len(phone_clean) == 11 and phone_clean.startswith('7'):
            formatted = f"+7 ({phone_clean[1:4]}) {phone_clean[4:7]}-{phone_clean[7:9]}-{phone_clean[9:11]}"
            formatted_phones.append(formatted)

    if formatted_phones:
        logger.debug(f"Найдены телефоны: {', '.join(formatted_phones)}")
        # Возвращаем первый подходящий номер
        return formatted_phones[0]
    logger.debug("Телефоны не найдены")
    return None