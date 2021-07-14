import datetime
import time


class Timer:
    def __init__(self):
        self.__start_time = time.perf_counter()

    @property
    def elapsed(self):
        return datetime.timedelta(seconds=time.perf_counter() - self.__start_time)
