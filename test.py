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
    dates = (get_current_month_period())
    metrika = YandexMetrika(OAUTH_TOKEN, COUNTER_ID)
    data = metrika.get_referral_traffic(dates[0], dates[1])
    import main
    print(main.get_metrika_referral_urls(metrika, dates[0], dates[1]))