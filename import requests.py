import requests


TOKEN = 'y0__xDzrKqtAxjYpiogwqjv8xOscTs_r_2wDTSEF-WEiXOf4ASGeA'
METRICA_API_URL = "https://api-metrika.yandex.net/stat/v1/data"



response = requests.get(
    "https://api-metrika.yandex.net/management/v1/counters",
    headers={"Authorization": f"OAuth {TOKEN}"}
)

if response.status_code == 200:
    counters = response.json()["counters"]
    for counter in counters:
        print(f"ID: {counter['id']}, Название: {counter['name']}, Сайт: {counter['site']}")
else:
    print("Ошибка:", response.json())


def get_metrica_data(token, counter_id):
    """
    Проверяет токен и возвращает базовые метрики из счётчика
    
    :param token: Ваш OAuth-токен
    :param counter_id: ID счётчика (например, '12345678')
    :return: Словарь с данными или ошибкой
    """
    try:
        # 1. Проверяем доступ к API
        response = requests.get(
            "https://api-metrika.yandex.net/stat/v1/data",
            params={
                "ids": counter_id,
                "metrics": "ym:s:visits,ym:s:pageviews",
                "date1": "7daysAgo",
                "date2": "today",
                "oauth_token": token
            }
        )
        
        # 2. Если ошибка доступа
        if response.status_code == 403:
            return {"error": "Доступ запрещен. Проверьте токен и права доступа"}
        
        # 3. Если счётчик не найден
        if response.status_code == 400:
            return {"error": "Счётчик не найден. Проверьте ID"}
        
        # 4. Возвращаем данные
        data = response.json()
        return {
            "status": "success",
            "visits": data["data"][0]["metrics"][0],
            "pageviews": data["data"][0]["metrics"][1]
        }
        
    except Exception as e:
        return {"error": f"Ошибка: {str(e)}"}
    
print(get_metrica_data(TOKEN, '99370966'))

