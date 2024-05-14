from functools import reduce

from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

texts = [
    "Подтвердить цены",
    "Поменять цены сейчас",
    "Отобразить конфигурацию",
    "Изменить конфигурацию",
]

keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=t) for t in texts
        ]
    ],
    one_time_keyboard=True
)

parameters = []

w1 = 'Ебитда'
w2 = 'Оборачиваемость'
for day in (7, 14, 21, 28):
    parameters.append([])
    for category in ('A', 'B'):
        parameters[-1].append(f'{w1} {day} {category}')

parameters.append([])

for day in (14, 21):
    parameters[-1].append(f'{w2} {day}')

parameters.append(['Назад'])

all_config_words = reduce(lambda x, y: x + y, parameters[:-1])

config_keybord = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text=t) for t in params
        ]
        for params in parameters
    ],
    one_time_keyboard=True
)
