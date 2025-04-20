import pandas as pd
import yadisk
import os

# Токен Яндекс.Диска
YANDEX_DISK_TOKEN = "y0__wgBELawpyAYkbY0IKiM5oQSstnwahr424ZzdNX_Y9dCWfPK-ac"

# Пути
LOCAL_FILE_PATH = "Transportation_Main.xlsx"
REMOTE_FILE_PATH = "disk:/BotDocs/Transportation_Main.xlsx"

# Инициализация Yandex Disk
yadisk_client = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)

# Создание таблицы с обновлённой структурой
def create_updated_table():
    try:
        df = pd.DataFrame(columns=[
            "Дата", "Фирма", "Маршрут",
            "Водитель ФИО", "Марка машины", "Номер машины",
            "Номер полуприцепа", "Телефон", "Перевозчик",
            "Цена", "Оплата", "Разница"
        ])
        df.to_excel(LOCAL_FILE_PATH, index=False, engine="openpyxl")
        print(f"Файл {LOCAL_FILE_PATH} успешно создан с обновлённой структурой.")

        # Проверка и создание папки на Яндекс.Диске
        if not yadisk_client.exists("disk:/BotDocs"):
            yadisk_client.mkdir("disk:/BotDocs")
            print("Папка disk:/BotDocs создана.")

        # Загрузка таблицы на Яндекс.Диск
        yadisk_client.upload(LOCAL_FILE_PATH, REMOTE_FILE_PATH, overwrite=True)
        print(f"Файл успешно загружен на Яндекс.Диск: {REMOTE_FILE_PATH}")
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    create_updated_table()