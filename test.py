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
WEBMASTER_USER_ID = get_yandex_webmaster_user_id(OAUTH_TOKEN)


if __name__ == '__main__':
    dates = (generate_monthly_periods('2024-01-01', '2025-08-08'))
    metrika = YandexMetrika(OAUTH_TOKEN, COUNTER_ID)
    import main
    for date in dates:
        x = main.get_metrika_referral_urls(metrika, date[0], date[1])
        for url_data in x:
            print(url_data)
            db.upsert_referral_urls_data(url_data)
        print('Успешный успех')