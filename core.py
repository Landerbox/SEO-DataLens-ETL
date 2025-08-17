import requests

from exceptions import MetrikaAPIError, MetrikaAuthError
from urllib.parse import urlparse, urlunparse

def get_yandex_webmaster_user_id(oauth_token: str) -> str:
    """
    Получает user_id для API Яндекс.Вебмастера
    
    Args:
        oauth_token: OAuth-токен Яндекса (полученный через oauth.yandex.ru)
    
    Returns:
        user_id (str) - идентификатор пользователя
    
    Raises:
        Exception: Если запрос не удался
    """
    url = "https://api.webmaster.yandex.net/v4/user"
    
    headers = {
        "Authorization": f"OAuth {oauth_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Проверка ошибок HTTP
        return response.json()['user_id']
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка при запросе user_id: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"\nСтатус код: {e.response.status_code}\nОтвет: {e.response.text}"
        raise Exception(error_msg) from e


class YandexWebmaster:
    def __init__(self, token: str, host: str, user_id: str, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'OAuth {token}',
            'Content-Type': 'application/json'
        })
        self.timeout = timeout
        self.host = host
        self.user_id = user_id

    def _request(self, method: str, url: str, **kwargs) -> dict:
        """Базовый метод запроса"""
        try:
            response = self.session.request(
                method,
                f"https://api.webmaster.yandex.net/v4/user/{self.user_id}/hosts/{self.host}{url}",
                timeout=self.timeout,
                **kwargs
            )
            
            if response.status_code == 403:
                raise MetrikaAuthError("Access denied. Check token permissions")
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise MetrikaAPIError(f"Request failed: {str(e)}")
        
    def get_summary(self):
        return self._request('GET', '/summary')
    
    def get_top_search_requests(self, date_from: str, date_to: str): # ёбанный яндекс не может принять параметры бля списком архитектуру мне похерили
        try:
            response = self.session.request(
                'GET',
                f"https://api.webmaster.yandex.net/v4/user/{self.user_id}/hosts/{self.host}/search-queries/popular?order_by=TOTAL_CLICKS&query_indicator=TOTAL_SHOWS&date_from={date_from}&date_to={date_to}&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION",
            )
            
            if response.status_code == 403:
                raise MetrikaAuthError("Access denied. Check token permissions")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise MetrikaAPIError(f"Request failed: {str(e)}")
        
        

class YandexMetrika:
    def __init__(self, token: str, counter_id: str, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json"
        })
        self.timeout = timeout
        self.counter_id = counter_id
        self.base_metrika_url = '/stat/v1/data'

    def _request(self, method: str, url: str, **kwargs) -> dict:
        """Базовый метод запроса"""
        try:
            response = self.session.request(
                method,
                f"https://api-metrika.yandex.net{url}",
                timeout=self.timeout,
                **kwargs
            )
            
            if response.status_code == 403:
                raise MetrikaAuthError("Access denied. Check token permissions")
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise MetrikaAPIError(f"Request failed: {str(e)}")

    def get_counters(self) -> list:
        """Получить список доступных счётчиков"""
        return self._request("GET", "/management/v1/counters")["counters"]

    def get_visits(self, date_from: str, date_to: str = None) -> int:
        """Получить количество визитов за период"""
        date_to = date_to or date_from
        data = self._request(
            "GET",
            "/stat/v1/data",
            params={
                "ids": self.counter_id,
                "metrics": "ym:s:visits",
                "date1": date_from,
                "date2": date_to
            }
        )
        return data["data"][0]["metrics"][0] if data["data"] else 0

    def get_sources(self, date: str, limit: int = 10) -> list:
        """Топ источников трафика за день"""
        return self._request(
            "GET",
            self.base_metrika_url,
            params={
                "ids": self.counter_id,
                "metrics": "ym:s:visits",
                "dimensions": "ym:s:lastTrafficSource",
                "date1": date,
                "date2": date,
                "limit": limit
            }
        )
    

    def get_all_traffic_by_url(self, date_from: str, date_to: str, url: str) -> dict:
        """
        Получает визиты по всем типам трафика за период по урлу.
        
        :param date_from: Начальная дата (YYYY-MM-DD)
        :param date_to: Конечная дата (YYYY-MM-DD)
        :return: Словарь {тип_трафика: количество_визитов}
        """
        
        # Основные типы трафика для анализа
        traffic_types = {
            'organic': 'organic',          # Поисковые системы
            'direct': 'direct',            # Прямые заходы
            'social': 'social',            # Соцсети
            'referral': 'referral',        # Рефералы
            'ad': 'ad',                    # Реклама
            'internal': 'internal',        # Внутренние переходы
            'email': 'email'               # Email-рассылки
        }
        
        result = {}
        
        for name, source in traffic_types.items():
            data = self._request(
                "GET",
                self.base_metrika_url,
                params={
                    "ids": self.counter_id,
                    "metrics": "ym:s:visits",
                    "date1": date_from,
                    "date2": date_to,
                    "filters": f"ym:s:trafficSource=='{source}' AND ym:s:startURLPathLevel2=='{url}'"
                }
            )
            result[name] = data["data"][0]["metrics"][0] if data["data"] else 0
        
        return result
    

    
    def get_traffic_by_urls(self, date_from: str, date_to: str, url: str, organic=True, limit=1000):
        '''
        Возвращает органический трафик для страницы. 

        :param url: Полный URL для анализа (например 'https://zaruku.ru/pitanie/')
        :param organic: True по умолчанию
        '''
        organic_query = ''
        if organic:
            organic_query = "ym:s:trafficSource=='organic' AND "

        url_filter = f"{organic_query}ym:s:startURLPathLevel2=='{url}'"

        data = self._request(
            "GET",
            self.base_metrika_url,
            params={
                "ids": self.counter_id,
                "metrics": "ym:s:visits",
                "date1": date_from,
                "date2": date_to,
                "filters": url_filter,
                "limit": limit,
                "accuracy": "full"
            }
        )

        return data["data"][0]["metrics"][0] if data["data"] else 0
    

    def get_search_engines_traffic(self, date_from: str, date_to: str, url:str = False) -> dict:
        """
        Получает визиты из поисковых систем (Яндекс и Google) за период

        :param date_from: Начальная дата (YYYY-MM-DD)
        :param date_to: Конечная дата (YYYY-MM-DD)
        :param url: URL второго уровня. По-умолчанию вернёт весь поисковый трафик
        :return: Словарь {'yandex': X, 'google': Y, 'other_search': Z}
        """
        
        # Фильтр для органического трафика из поисковиков
        search_filter = f"ym:s:trafficSource=='organic' AND ym:s:searchEngine!='(none)'"
        if url:
            search_filter += f" AND ym:pv:URLPathLevel2=='{url}'"
        
        # Запрос данных с разбивкой по поисковым системам
        data = self._request(
            "GET",
            self.base_metrika_url,
            params={
                "ids": self.counter_id,
                "metrics": "ym:s:visits",
                "dimensions": "ym:s:searchEngine",
                "date1": date_from,
                "date2": date_to,
                "filters": search_filter,
                "limit": 10
            }
        )
        
        # Обработка результатов
        result = {'yandex': 0, 'google': 0, 'other_search': 0}
        
        for row in data.get("data", []):
            engine = row["dimensions"][0]["name"].lower()
            visits = row["metrics"][0]
            
            if 'yandex' in engine:
                result['yandex'] += visits
            elif 'google' in engine:
                result['google'] += visits
            else:
                result['other_search'] += visits
        
        return result

    def get_organic_pages_from_url(self, date_from: str, date_to: str, base_url: str, limit: int = 100) -> list:
        """
        Получает страницы входа из органики для указанного URL реферера

        :param base_url: Базовый URL второго уровня (например 'https://zaruku.ru/rak-lyogkogo/')
        :param date_from: Начальная дата (YYYY-MM-DD)
        :param date_to: Конечная дата (YYYY-MM-DD)
        :param limit: Максимальное количество возвращаемых URL
        :return: Список словарей {'url': str, 'visits': int, 'bounce_rate': float}
        """
        
        # Нормализуем базовый URL (убираем параметры и якоря)
        parsed = urlparse(base_url)
        clean_base_url = urlunparse(parsed._replace(query='', fragment=''))
        
        # Формируем фильтр для URL третьего уровня и ниже
        url_filter = (
            f"ym:s:trafficSource=='organic' AND "
            f"ym:s:startURL=~'^{clean_base_url}[^/]+/.*'"
        )
        
        data = self._request(
            "GET",
            "/stat/v1/data",
            params={
                "ids": self.counter_id,
                "metrics": "ym:s:visits,ym:s:bounceRate",
                "dimensions": "ym:s:startURL",
                "date1": date_from,
                "date2": date_to,
                "filters": url_filter,
                "limit": limit,
                "sort": "-ym:s:visits",  # Сортировка по визитам
                "accuracy": "full"
            }
        )
        
        result = []
        total_visits = sum(row['metrics'][0] for row in data.get('data', []))
        
        for row in data.get('data', []):
            url = row['dimensions'][0]['name']
            visits = row['metrics'][0]
            bounce_rate = row['metrics'][1]
            
            result.append({
                'page_url': url,
                'visits': visits,
                'bounce_rate': round(bounce_rate, 1),
                'traffic_share': round((visits / total_visits * 100), 1) if total_visits > 0 else 0
            })
        
        return result

    def get_behavior_metrics(self, date_from: str, date_to: str, base_url: str = None) -> dict:
        """
        :param base_url: URL второго уровня (например 'https://zaruku.ru/rak-lyogkogo/').
        Если None - возвращает данные для всех
        :param date_from: Начальная дата (YYYY-MM-DD)
        :param date_to: Конечная дата (YYYY-MM-DD)
        :return: {
            'bounce_rate': float,  # Процент отказов
            'page_depth': float,   # Глубина просмотра (страниц/визит)
            'avg_visit': float,    # Средняя продолжительность визита (секунды)
            'visits': int          # Количество визитов
        }
        """
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds",
            "date1": date_from,
            "date2": date_to
        }

        if base_url:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(base_url)
            clean_base_url = urlunparse(parsed._replace(query='', fragment=''))
            params["filters"] = f"ym:s:startURL=~'^{clean_base_url}[^/]*/?$'"

        data = self._request("GET", "/stat/v1/data", params=params)

        if not data.get('data'):
            return {
                'bounce_rate': 0,
                'page_depth': 0,
                'avg_visit': 0,
                'visits': 0
            }

        metrics = data['data'][0]['metrics']
        
        return {
            'bounce_rate': round(metrics[1], 1),       # Процент отказов
            'page_depth': round(metrics[2], 2),        # Глубина просмотра
            'avg_visit': int(metrics[3]),              # Продолжительность (сек)
            'visits': int(metrics[0])                  # Количество визитов
        }
    
    def get_referral_traffic(
        self,
        date_from: str,
        date_to: str,
        entry_url: str = None
        ) -> dict:
        """
        Получить переходы с других сайтов (реферальный трафик) за период.
        
        :param date_from: Начальная дата периода в формате YYYY-MM-DD
        :param date_to: Конечная дата периода в формате YYYY-MM-DD
        :param entry_url: (опционально) URL точки входа для фильтрации (например 'https://zaruku.ru/rak-molochnoj-zhelezy/')
        :return: Словарь вида {'реферальный_домен': количество_визитов}
        """
        params = {
            "ids": self.counter_id,
            "metrics": "ym:s:visits",
            "dimensions": "ym:s:externalReferer",
            "date1": date_from,
            "date2": date_to
        }
        
        if entry_url:
            params["filters"] = f"ym:s:startURL=='{entry_url}'"
        
        data = self._request("GET", self.base_metrika_url, params=params)
        
        referral_traffic = {}
        for row in data.get("data", []):
            domain = row["dimensions"][0]["name"]
            visits = row["metrics"][0]
            referral_traffic[domain] = visits
        
        return referral_traffic

    