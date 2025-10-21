import asyncio
from time import time


class RateLimiter:
    def __init__(self, rate, per):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time()

    async def acquire(self):
        current = time()
        elapsed = current - self.last_check
        self.last_check = current
        self.allowance += elapsed * (self.rate / self.per)

        if self.allowance > self.rate:
            self.allowance = self.rate

        if self.allowance < 1:
            sleep_time = (1 - self.allowance) * (self.per / self.rate)
            await asyncio.sleep(sleep_time)
            self.allowance = 0
        else:
            self.allowance -= 1
