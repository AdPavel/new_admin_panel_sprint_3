import time
from functools import wraps
import logging.config
from config.config import settings

logging.config.fileConfig('config/log_config')
log = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=settings.border_time):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            retries = 0
            t = 0
            while t < border_sleep_time:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as msg:
                    t = start_sleep_time * factor ** retries
                    log.error(f'Ошибка {msg}, попытка востановления № {retries} пауза на: {t} секунд', exc_info=False)
                    time.sleep(t)
                    retries += 1
        return inner
    return func_wrapper
