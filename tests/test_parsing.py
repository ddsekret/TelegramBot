import unittest
from parser import parse_carrier_data

class TestCarrierParsing(unittest.TestCase):
    def test_ip_with_fio(self):
        text = "ИП Атакишиев Маил Алиаббас Оглы Телефон +7 (123) 456-78-90 ИНН 123456789012"
        data = parse_carrier_data(text)
        self.assertEqual(data["Перевозчик"], "ИП Атакишиев Маил Алиаббас Оглы")
        self.assertEqual(data["Имя перевозчика"], "Атакишиев Маил Алиаббас Оглы")
        self.assertEqual(data["Телефон"], "+7 (123) 456-78-90")
        self.assertEqual(data["ИНN"], "123456789012")

    def test_ip_with_fio_after_inn(self):
        text = "ИП Телефон +7 (123) 456-78-90 ИНН 123456789012 Атакишиев Маил Алиаббас Оглы"
        data = parse_carrier_data(text)
        self.assertEqual(data["Перевозчик"], "ИП Атакишиев Маил Алиаббас Оглы")
        self.assertEqual(data["Имя перевозчика"], "Атакишиев Маил Алиаббас Оглы")
        self.assertEqual(data["Телефон"], "+7 (123) 456-78-90")
        self.assertEqual(data["ИНN"], "123456789012")

    def test_ooo(self):
        text = "ООО Транспортная Компания Телефон +7 (987) 654-32-10 ИНН 1234567890"
        data = parse_carrier_data(text)
        self.assertEqual(data["Перевозчик"], "ООО Транспортная Компания")
        self.assertNotIn("Имя перевозчика", data)
        self.assertEqual(data["Телефон"], "+7 (987) 654-32-10")
        self.assertEqual(data["ИНN"], "1234567890")

if __name__ == '__main__':
    unittest.main(verbosity=2)