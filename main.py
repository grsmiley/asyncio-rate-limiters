import asyncio, time
from datetime import timedelta

class RateLimitedQueue(asyncio.Queue):
    ''' A rate limited queue. Set the interval as seconds (int or float) or a timedelta
        to control how frequently queue items are retured.
    '''
    def __init__(self, interval:int|float|timedelta, maxsize=0, *, loop=asyncio.mixins._marker):
        super().__init__(maxsize=maxsize, loop=loop)
        self._lock = RateLimitedLock(interval, concurrency=1)

    async def get(self):
        async with self._lock:
            return await super().get()

class RateLimitedLock(asyncio.Semaphore):
    ''' A rate limited semaphore. Supports concurrency. '''
    def __init__(self, interval:int|float|timedelta, concurrency=1):
        super().__init__(value=concurrency)

        self.interval = interval
        if isinstance(self.interval, timedelta):
            self.interval = self.interval.total_seconds()
        assert isinstance(self.interval, (int, float))
        self._last_acquired = time.monotonic()
        self._sync_lock = asyncio.Lock()

    async def acquire(self):
        result = await super().acquire()
        async with self._sync_lock:
            time_to_sleep = self.interval - (time.monotonic()-self._last_acquired)
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep)
        self._last_acquired = time.monotonic()
        return result