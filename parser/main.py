# parser/main.py
from typing import Dict, Optional, Tuple
from .personal_data import parse_driver_name, parse_birth_data, parse_citizenship, parse_residence
from .passport import parse_passport_data
from .driver_license import parse_driver_license_data
from .phone import parse_phone_numbers
from .vehicle import parse_vehicle_data
from .normalization import normalize_data

def parse_by_keywords(text: str, is_driver_data: bool = False) -> Tuple[str, Dict[str, Optional[str]]]:
    """Парсит текст по ключевым словам."""
    text = text.strip()
    data: Dict[str, Optional[str]] = {}

    driver_name = parse_driver_name(text)
    if driver_name:
        data["Водитель"] = driver_name

    birth_data = parse_birth_data(text)
    data.update(birth_data)

    citizenship = parse_citizenship(text)
    if citizenship:
        data["Гражданство"] = citizenship

    residence = parse_residence(text)
    if residence:
        data["Адрес_регистрации"] = residence

    passport_data = parse_passport_data(text)
    data.update(passport_data)

    driver_license_data = parse_driver_license_data(text)
    data.update(driver_license_data)

    phone = parse_phone_numbers(text)
    if phone:
        data["Телефон"] = phone

    if is_driver_data:
        vehicle, trailer = parse_vehicle_data(text)
        if vehicle:
            data["Автомобиль"] = vehicle
        if trailer:
            data["Прицеп"] = trailer

    data = normalize_data(data, text)
    return text, data