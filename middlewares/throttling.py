from aiogram import types
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.handler import CancelHandler, current_handler
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils.exceptions import Throttled

class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit=2):
        self.rate_limit = limit
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        handler = current_handler.get()
        dp = Dispatcher.get_current()
        if handler:
            limit = getattr(handler, 'throttling_rate_limit', self.rate_limit)
            key = getattr(handler, 'throttling_key', f"{handler.__name__}_default")
        else:
            limit = self.rate_limit
            key = "default"
        try:
            await dp.throttle(key, rate=limit)
        except Throttled as throttled:
            await self.message_throttled(message, throttled)
            raise CancelHandler()

    async def message_throttled(self, message: types.Message, throttled: Throttled):
        if throttled.exceeded_count <= 2:
            await message.reply("Juda ko'p xabar yozmang!")
