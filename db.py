import psycopg2
import os
import logging

from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional

def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Ротация логов (10 МБ, максимум 5 файлов)
    file_handler = RotatingFileHandler(
        'bd.log', 
        maxBytes=10*1024*1024, 
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Вывод в консоль
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

load_dotenv()

DB_CONFIG ={
    "host": os.getenv('IP'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DATABASE'),
    "port": os.getenv('PORT')
}

SQL_COMMANDS = [
    """
    CREATE TABLE IF NOT EXISTS public.all_traffic_by_url (
        id BIGSERIAL PRIMARY KEY,
        url VARCHAR(512) NOT NULL,
        date_from DATE NOT NULL,
        date_to DATE NOT NULL,
        organic INTEGER NOT NULL,
        direct INTEGER NOT NULL,
        social INTEGER NOT NULL,
        referral INTEGER NOT NULL,
        ad INTEGER NOT NULL,
        internal INTEGER NOT NULL,
        email INTEGER NOT NULL,
        google_traffic INTEGER NOT NULL,
        yandex_traffic INTEGER NOT NULL,
        bounce_rate NUMERIC(5,2) NOT NULL,  -- Проценты с 2 знаками после запятой
        page_depth NUMERIC(5,2) NOT NULL,   -- Среднее значение с 2 знаками
        avg_visit NUMERIC(10,2) NOT NULL,   -- Время с 2 знаками
        visits INTEGER NOT NULL,
        month_year VARCHAR(512) NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_date_range_url UNIQUE (date_from, date_to, url)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.organic_pages_by_url (
        id BIGSERIAL PRIMARY KEY,
        base_url VARCHAR(512),
        page_url VARCHAR(512),
        date_from DATE NOT NULL,
        date_to DATE NOT NULL,
        bounce_rate NUMERIC(5,2) NOT NULL,      -- Проценты
        visits INTEGER NOT NULL,
        traffic_share NUMERIC(5,2) NOT NULL,    -- Проценты
        month_year VARCHAR(512) NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_date_range_page_url UNIQUE (date_from, date_to, page_url)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.search_queries_webmaster (
        id BIGSERIAL PRIMARY KEY,
        query_text VARCHAR(512),
        shows INTEGER NOT NULL,
        clicks INTEGER NOT NULL,
        avg_show_position NUMERIC(5,2) NOT NULL,  -- Позиция с 2 знаками
        date_from DATE NOT NULL,
        date_to DATE NOT NULL,
        month_year VARCHAR(512) NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_query_text_date UNIQUE (date_from, date_to, query_text)
    )
    """
]

def create_tables():
    """Создаёт таблицы и индексы в БД"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for command in SQL_COMMANDS:
            cursor.execute(command)
            logger.info("Выполнена команда: %s", command.split()[0:4] + ["..."])
        
        conn.commit()
        logger.info("Все таблицы и индексы успешно созданы")
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка при создании таблиц: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


def check_database():
    """Проверяет подключение к БД и список таблиц"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Проверяем текущую БД
        cursor.execute("SELECT current_database()")
        db_name = cursor.fetchone()[0]
        logger.info(f"Подключены к БД: {db_name}")
        
        # Проверяем существование таблиц
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        logger.info("Существующие таблицы: %s", [t[0] for t in tables])
        
        # Проверяем структуру таблиц (если они есть)
        for table in ['all_traffic_by_url', 'organic_pages_by_url']:
            if table in [t[0] for t in tables]:
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                """)
                logger.info(f"Структура таблицы {table}: {cursor.fetchall()}")
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка проверки: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def upsert_traffic_data(data: dict) -> Optional[int]:
    """
    Вставляет или обновляет данные трафика по периоду дат
    
    Args:
        data: Словарь с данными

    
    Returns:
        int: ID обновленной/созданной записи
        None: В случае ошибки
    """

    query = """
    INSERT INTO public.all_traffic_by_url (
        url, date_from, date_to, organic, direct, social, 
        referral, ad, internal, email, google_traffic, 
        yandex_traffic, bounce_rate, page_depth, avg_visit, visits, month_year
    ) VALUES (
        %(url)s, %(date_from)s, %(date_to)s, %(organic)s, %(direct)s,
        %(social)s, %(referral)s, %(ad)s, %(internal)s, %(email)s,
        %(google_traffic)s, %(yandex_traffic)s, %(bounce_rate)s,
        %(page_depth)s, %(avg_visit)s, %(visits)s, %(month_year)s
    )
    ON CONFLICT (date_from, date_to, url)
    DO UPDATE SET
        organic = EXCLUDED.organic,
        direct = EXCLUDED.direct,
        social = EXCLUDED.social,
        referral = EXCLUDED.referral,
        ad = EXCLUDED.ad,
        internal = EXCLUDED.internal,
        email = EXCLUDED.email,
        google_traffic = EXCLUDED.google_traffic,
        yandex_traffic = EXCLUDED.yandex_traffic,
        bounce_rate = EXCLUDED.bounce_rate,
        page_depth = EXCLUDED.page_depth,
        avg_visit = EXCLUDED.avg_visit,
        visits = EXCLUDED.visits,
        month_year = EXCLUDED.month_year,
        updated_at = NOW()
    RETURNING id
    """
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                record_id = cursor.fetchone()[0]
                conn.commit()
                logging.info(f"Данные за период {data['date_from']}-{data['date_to']} обновлены. ID: {record_id}")
                return record_id
                
    except psycopg2.Error as e:
        logging.error(f"Ошибка при обновлении данных: {e}")
        return None


def upsert_organic_pages_data(data: dict) -> Optional[int]:
    """
    Вставляет или обновляет данные органического трафика по страницам
    
    Args:
        data: Словарь с данными
            
    
    Returns:
        int: ID обновленной/созданной записи
        None: В случае ошибки
    """


    # 3. UPSERT запрос
    query = """
    INSERT INTO public.organic_pages_by_url (
        base_url, page_url, date_from, date_to, 
        bounce_rate, visits, traffic_share, month_year
    ) VALUES (
        %(base_url)s, %(page_url)s, %(date_from)s, %(date_to)s,
        %(bounce_rate)s, %(visits)s, %(traffic_share)s, %(month_year)s
    )
    ON CONFLICT (date_from, date_to, page_url)
    DO UPDATE SET
        base_url = EXCLUDED.base_url,
        page_url = EXCLUDED.page_url,
        bounce_rate = EXCLUDED.bounce_rate,
        visits = EXCLUDED.visits,
        traffic_share = EXCLUDED.traffic_share,
        month_year = EXCLUDED.month_year,
        updated_at = NOW()
    RETURNING id
    """
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                record_id = cursor.fetchone()[0]
                conn.commit()
                logging.info(f"Данные за {data['date_from']}-{data['date_to']} обновлены. ID: {record_id}")
                return record_id
                
    except psycopg2.Error as e:
        logging.error(f"Ошибка базы данных: {e}")
        return None

def upsert_search_queries_webmaster_data(data: dict) -> Optional[int]:
    """
    Вставляет или обновляет данные запросов с вебмастера
    
    Args:
        data: Словарь с данными
    
    Returns:
        int: ID обновленной/созданной записи
        None: В случае ошибки
    """
    # 1. Проверка обязательных полей



    # 3. UPSERT запрос
    query = """
    INSERT INTO public.search_queries_webmaster (
        query_text, shows, clicks, avg_show_position, 
        date_from, date_to, month_year
    ) VALUES (
        %(query_text)s, %(shows)s, %(clicks)s, %(avg_show_position)s,
        %(date_from)s, %(date_to)s, %(month_year)s
    )
    ON CONFLICT (date_from, date_to, query_text)
    DO UPDATE SET
        query_text = EXCLUDED.query_text,
        shows = EXCLUDED.shows,
        clicks = EXCLUDED.clicks,
        avg_show_position = EXCLUDED.avg_show_position,
        month_year = EXCLUDED.month_year,
        updated_at = NOW()
    RETURNING id
    """
    
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, data)
                record_id = cursor.fetchone()[0]
                conn.commit()
                logging.info(f"Данные за {data['date_from']}-{data['date_to']} обновлены. ID: {record_id}")
                return record_id
                
    except psycopg2.Error as e:
        logging.error(f"Ошибка базы данных: {e}")
        return None

def execute_sql_query(query, params=None):
    """
    Выполняет SQL-запрос к PostgreSQL и возвращает результат
    
    :param db_config: словарь с параметрами подключения (host, dbname, user, password)
    :param query: SQL-запрос (строка или объект sql.SQL)
    :param params: параметры для запроса (кортеж или словарь)
    :return: результат fetchall() или None для запросов без возврата данных
    """
    conn = None
    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # Выполняем запрос
            if isinstance(query, str):
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Для SELECT возвращаем результаты
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            
            # Фиксируем изменения для DML-запросов
            conn.commit()
            logger.info(f"Успешно выполнено: {query}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    execute_sql_query('''
    UPDATE search_queries_webmaster
    SET month_year = REGEXP_REPLACE(month_year, '\s+', ' ')''')

