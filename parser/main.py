# parser/main.py
from typing import Dict, Optional, Tuple
from functools import lru_cache
from .imports_and_settings import logger
from .passport import parse_passport_data
from .driver_license import parse_driver_license_data
from .phone import parse_phone_numbers
from .personal_data import parse_driver_name, parse_birth_data, parse_citizenship, parse_residence
from .vehicle import parse_car_data, parse_trailer_data
from .carrier_customer import parse_carrier_data
from .normalization import normalize_data

@lru_cache(maxsize=1000)
def parse_by_keywords(text: str, is_driver_data: bool = True) -> Tuple[Dict[str, Optional[str]], Dict[str, Optional[str]]]:
    """Извлекает данные по ключевым словам."""
    logger.debug(f"Полный текст для парсинга: {text}")
    data = {}

    try:
        if is_driver_data:
            # Парсинг ФИО водителя
            try:
                driver_name = parse_driver_name(text)
                if driver_name:
                    data["Водитель"] = driver_name
                    logger.debug(f"ФИО найдено: {driver_name}")
                else:
                    logger.debug("ФИО водителя не найдено")
            except Exception as e:
                logger.error(f"Ошибка при парсинге ФИО: {e}")
                data["Водитель"] = None

            # Парсинг данных паспорта
            try:
                passport_data = parse_passport_data(text)
                data.update(passport_data)
                logger.debug(f"Данные паспорта: {passport_data}")
            except Exception as e:
                logger.error(f"Ошибка при парсинге паспорта: {e}")

            # Парсинг данных водительского удостоверения
            try:
                vu_data = parse_driver_license_data(text)
                data.update(vu_data)
                logger.debug(f"Данные ВУ: {vu_data}")
            except Exception as e:
                logger.error(f"Ошибка при парсинге ВУ: {e}")

            # Парсинг даты и места рождения
            try:
                birth_data = parse_birth_data(text)
                data.update(birth_data)
                logger.debug(f"Данные о рождении: {birth_data}")
            except Exception as e:
                logger.error(f"Ошибка при парсинге даты и места рождения: {e}")

            # Парсинг гражданства
            try:
                citizenship = parse_citizenship(text)
                if citizenship:
                    data["Гражданство"] = citizenship
                    logger.debug(f"Гражданство: {citizenship}")
                else:
                    logger.debug("Гражданство не найдено")
            except Exception as e:
                logger.error(f"Ошибка при парсинге гражданства: {e}")
                data["Гражданство"] = None

            # Парсинг данных автомобиля
            try:
                car = parse_car_data(text)
                if car:
                    data["Автомобиль"] = car
                    logger.debug(f"Car data: {car}")
                else:
                    logger.debug("Данные автомобиля не найдены")
            except Exception as e:
                logger.error(f"Ошибка при парсинге автомобиля: {e}")
                data["Автомобиль"] = None

            # Парсинг данных прицепа
            try:
                trailer = parse_trailer_data(text)
                if trailer:
                    data["Прицеп"] = trailer
                    logger.debug(f"Прицепы: {trailer}")
                else:
                    logger.debug("Данные прицепа не найдены")
            except Exception as e:
                logger.error(f"Ошибка при парсинге прицепа: {e}")
                data["Прицеп"] = None

            # Парсинг места жительства
            try:
                residence = parse_residence(text)
                if residence:
                    data["Адрес регистрации"] = residence
                    data["Адрес_регистрации"] = residence
                    logger.debug(f"Место жительства: {residence}")
                else:
                    logger.debug("Место жительства не найдено")
            except Exception as e:
                logger.error(f"Ошибка при парсинге места жительства: {e}")
                data["Адрес_регистрации"] = None

        # Парсинг телефона
        try:
            phone = parse_phone_numbers(text)
            if phone:
                data["Телефон"] = phone
                logger.debug(f"Найдены телефоны: {phone}")
            else:
                data["Телефон"] = None
                logger.debug("Телефон не найден")
        except Exception as e:
            logger.error(f"Ошибка при парсинге телефона: {e}")
            data["Телефон"] = None

        # Парсинг перевозчика
        try:
            carrier = parse_carrier_data(text)
            data.update(carrier)
            logger.debug(f"Данные перевозчика: {carrier}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге перевозчика: {e}")

        # Копируем сырые данные
        raw_data = data.copy()
        # Нормализуем данные
        normalized_data = normalize_data(data, text)
        logger.debug(f"Сырые данные: {raw_data}")
        logger.debug(f"Нормализованные данные: {normalized_data}")
        return raw_data, normalized_data

    except Exception as e:
        logger.error(f"Критическая ошибка в parse_by_keywords: {e}")
        return {}, {}