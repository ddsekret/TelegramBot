# test_import.py
import sys
sys.path.append("C:\\Users\\Dsekr\\TelegramBot")
print("sys.path:", sys.path)

try:
    from parser.personal_data import parse_driver_name
    print("Successfully imported parse_driver_name from parser.personal_data")
except ImportError as e:
    print(f"Failed to import parse_driver_name: {e}")