class MetrikaAPIError(Exception):
    """Базовая ошибка API"""
    def __init__(self, message, status_code=None):
        self.status_code = status_code
        super().__init__(f"[{status_code}] {message}" if status_code else message)

class MetrikaAuthError(MetrikaAPIError):
    """Ошибки авторизации"""

class MetrikaCounterNotFound(MetrikaAPIError):
    """Счётчик не найден или нет доступа"""