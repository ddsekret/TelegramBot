import os
import pandas as pd
import yadisk

# Константы
YANDEX_DISK_TOKEN = "y0__wgBELawpyAYkbY0IKiM5oQSstnwahr424ZzdNX_Y9dCWfPK-ac"
LOCAL_FILE_PATH = "Transportation_Main.xlsx"
REMOTE_FILE_PATH = "disk:/BotDocs/Transportation_Main.xlsx"

# Примерные данные
data = {
    "Номер": [1, 2, 3],
    "Дата": ["2025-01-17", "2025-01-18", "2025-01-19"],
    "Фирма": ["ООО Пример", "ИП Тест", "ЗАО Образец"],
    "Направление": ["Москва - СПб", "Казань - Уфа", "Новосибирск - Омск"],
    "Водитель/Перевозчик": ["Иванов Иван", "Петров Петр", "Сидоров Алексей"],
    "Стоимость": [50000, 30000, 40000],
    "Оплата": [30000, 30000, 35000],
    "Разница": [20000, 0, 5000],
}

# Создание таблицы
df = pd.DataFrame(data)
df.to_excel(LOCAL_FILE_PATH, index=False, engine="openpyxl")

# Инициализация Yandex Disk
yadisk_client = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)

# Проверка существования директории на Яндекс.Диске
def ensure_remote_directory_exists():
    remote_dir = os.path.dirname(REMOTE_FILE_PATH)
    if not yadisk_client.exists(remote_dir):
        yadisk_client.mkdir(remote_dir)

# Загрузка файла на Яндекс.Диск
def upload_to_yandex_disk():
    try:
        ensure_remote_directory_exists()
        yadisk_client.upload(LOCAL_FILE_PATH, REMOTE_FILE_PATH, overwrite=True)
        print(f"Файл успешно загружен на Яндекс.Диск: {REMOTE_FILE_PATH}")
    except Exception as e:
        print(f"Ошибка загрузки файла на Яндекс.Диск: {e}")

# Основной блок
if __name__ == "__main__":
    upload_to_yandex_disk()