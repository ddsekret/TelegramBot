import os

def apply_fixes():
    # Читаем текущий parser.py как список строк
    with open('parser.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Читаем исправленную версию parse_carrier_data из файла
    with open('fixed_parse_carrier_data.txt', 'r', encoding='utf-8') as f:
        new_parse_carrier_data = f.read().splitlines()

    # Ищем начало и конец функции parse_carrier_data
    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("def parse_carrier_data(text):"):
            start_idx = i
            continue
        if start_idx is not None and line.strip() == "return data":
            end_idx = i
            break

    if start_idx is not None and end_idx is not None:
        # Заменяем старую функцию на новую
        lines[start_idx:end_idx + 1] = [line + '\n' for line in new_parse_carrier_data]

        # Сохраняем изменения
        with open('parser.py', 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print("Исправления успешно применены к parser.py")
    else:
        print("Не удалось найти функцию parse_carrier_data в parser.py")

if __name__ == "__main__":
    apply_fixes()