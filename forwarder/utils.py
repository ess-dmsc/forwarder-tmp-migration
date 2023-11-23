from threading import Lock


class Counter:
    """Thread safe Counter class"""

    def __init__(self):
        self._value = 0
        self._lock = Lock()

    def increment(self, amount=1):
        with self._lock:
            self._value += amount

    @property
    def value(self):
        with self._lock:
            return self._value
