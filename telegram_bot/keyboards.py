from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="Подтвердить цены"),
            KeyboardButton(text="Поменять цены сейчас")
        ]
    ],
    one_time_keyboard=True
)