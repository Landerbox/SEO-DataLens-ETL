import db
import os

from dotenv import load_dotenv
from core import YandexMetrika, YandexWebmaster, get_yandex_webmaster_user_id
from utils import (
    format_date,
    generate_monthly_periods,
    format_date,
    get_current_month_period
    )


load_dotenv()
COUNTER_ID = os.getenv('COUNTER_ID')
OAUTH_TOKEN = os.getenv('OAUTH_TOKEN')
WEBMASTER_HOST = os.getenv('WEBMASTER_HOST')
WEBMASTER_USER_ID = get_yandex_webmaster_user_id(OAUTH_TOKEN) # лол яндекс контора криворуких пидорасов апи их полое говно


def get_metrika_data(token, counter_id, date_from: str, date_to: str):
    '''
    Получить все данные от указанного периода до сегодняшнего дня
    
    :param token: Oauth-токен яндекса
    :param couner_id: ID счётчика
    :param date_from: Начальная дата (YYYY-MM-DD)
    :param date_to: Конечная дата (YYYY-MM-DD)
    '''
    metrika = YandexMetrika(token, counter_id)

    urls = ['https://zaruku.ru/rak-lyogkogo/',
            'https://zaruku.ru/rak-molochnoj-zhelezy/',
            'https://zaruku.ru/obshie-temy/',
            'https://zaruku.ru/rak-mochevogo-puzyrya/',
            'https://zaruku.ru/melanoma/',
            'https://zaruku.ru/limfoma/',
            'https://zaruku.ru/rak-pecheni/',
            'https://zaruku.ru/pitanie/'
            ]

    for url in urls:
        traffic_data = {
            'url': None, 'date_from': None, 'date_to': None, 'organic': None, 
            'direct': None, 'social': None, 'referral': None, 'ad': None, 
            'internal': None, 'email': None, 'google_traffic': None, 
            'yandex_traffic': None, 'bounce_rate': None, 'page_depth': None, 
            'avg_visit': None, 'visits': None, 'month_year': None
        }

        for date_start, date_end in generate_monthly_periods(date_from, date_to):
            
            traffic_data['url'] = url 
            traffic_data['date_from'] = date_start
            traffic_data['date_to'] = date_end

            traffic_data.update(metrika.get_all_traffic_by_url(date_start, date_end, url))
            traffic_data.update(metrika.get_behavior_metrics(date_start, date_end, url))

            search_engines = metrika.get_search_engines_traffic(date_start, date_end, url)
            traffic_data['yandex_traffic'] = search_engines.get('yandex')
            traffic_data['google_traffic'] = search_engines.get('google')

            traffic_data['month_year'] = format_date(str(traffic_data['date_from']))

            db.upsert_traffic_data(traffic_data)
            print(f'Записано в БД {url}, {date_start} - {date_end}')

            for page_data in (metrika.get_organic_pages_from_url(date_start, date_end, url)):
                organic_page_data = {
                    'base_url': None,
                    'page_url': None, 'date_from': None, 'date_to': None, 
                    'page_url': None, 'bounce_rate': None, 
                    'visits': None, 'traffic_share': None, 'month_year': None
                }
                
                organic_page_data.update(page_data)
                organic_page_data['base_url'] = url
                organic_page_data['date_from'] = date_start
                organic_page_data['date_to'] = date_end
                organic_page_data['month_year'] = format_date(str(traffic_data['date_from']))

                db.upsert_organic_pages_data(organic_page_data)
                print(f'Записано в БД {organic_page_data.get('page_url')}, {date_start} - {date_end}')


def get_webmaster_data(token, host, user_id, date_start, date_end):
    webmaster = YandexWebmaster(token, host, user_id)
    for date_from, date_to in generate_monthly_periods(date_start, date_end):
        for query in webmaster.get_top_search_requests(date_from, date_to).get('queries'):
            indicators = query['indicators']
            data = {
                'query_text': query['query_text'],
                'shows': indicators['TOTAL_SHOWS'],
                'clicks': indicators['TOTAL_CLICKS'],
                'avg_show_position': indicators['AVG_SHOW_POSITION'],
                'date_from': date_from,
                'date_to': date_to,
                'month_year': format_date(str(date_from))
                    }
            db.upsert_search_queries_webmaster_data(data)
            print(f'Записано в БД {data.get('query_text')}, {date_from} - {date_to}')

def check_services(token, counter_id, webmaster_host, yandex_user_id):
    metrika = YandexMetrika(token, counter_id)
    webmaster = YandexWebmaster(token, webmaster_host, yandex_user_id)
    if webmaster.get_summary() !=[] and metrika.get_counters() != []:
        return True
    else:
        return False


if __name__ == '__main__':
    dates = (get_current_month_period())
    date_from = dates[0]
    date_to = dates[1]
    get_metrika_data(OAUTH_TOKEN, COUNTER_ID, date_from, date_to)
    get_webmaster_data(OAUTH_TOKEN, WEBMASTER_HOST, WEBMASTER_USER_ID, date_from, date_to)
    print('Успешный успех')

    

    
    




    
    


