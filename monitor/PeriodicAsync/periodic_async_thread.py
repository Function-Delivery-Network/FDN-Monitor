import time
import asyncio

class PeriodicAsyncThread:
    def __init__(self, period):
        self.period = period

    def set_period(self, period):
        self.period = period
    async def invoke_forever(self, corofn):
        while True:
            then = time.time()
            await corofn()
            elapsed = time.time() - then
            await asyncio.sleep(self.period - elapsed)