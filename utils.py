from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple

def format_date(dt: datetime) -> str:
    """Форматирование даты для API"""
    return dt.strftime("%Y-%m-%d")

def get_last_week_dates() -> tuple:
    """Получить даты за последние 7 дней"""
    today = datetime.now()
    week_ago = today - timedelta(days=7)
    return format_date(week_ago), format_date(today)

def generate_monthly_periods(from_date: str, to_date: str) -> List[Tuple[str, str]]:
    """
    Генерирует список периодов (начало и конец месяца) между указанными датами
    
    :param from_date: Начальная дата в формате 'YYYY-MM-DD'
    :param to_date: Конечная дата в формате 'YYYY-MM-DD'
    :return: Список кортежей (начало_месяца, конец_месяца)
    """
    periods = []
    current_date = datetime.strptime(from_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(to_date, "%Y-%m-%d").date()
    
    while current_date <= end_date:
        # Начало месяца
        month_start = current_date.replace(day=1)
        
        # Конец месяца
        next_month = month_start + relativedelta(months=1)
        month_end = next_month - timedelta(days=1)
        
        # Если конец месяца выходит за пределы конечной даты, используем конечную дату
        if month_end > end_date:
            month_end = end_date
        
        periods.append((
            month_start.strftime("%Y-%m-%d"),
            month_end.strftime("%Y-%m-%d")
        ))
        
        # Переход к следующему месяцу
        current_date = next_month
    
    return periods

def format_date(date: str):
    '''2025-07-20 в July 2025
    
    return: Строка даты
    '''
    date_obj = datetime.strptime(date, "%Y-%m-%d")  # преобразуем строку в datetime
    return date_obj.strftime("%B %Y") 

def get_current_month_period():
    today = datetime.now()
    
    # Первый день текущего месяца
    first_day = today.replace(day=1)
    
    # Последний день текущего месяца (через переход к следующему месяцу и вычитание 1 дня)
    next_month = today.replace(day=28) + timedelta(days=4)  # гарантированно переходим в след. месяц
    last_day = next_month - timedelta(days=next_month.day)
    
    return (
        first_day.strftime("%Y-%m-%d"),  # Начало месяца
        last_day.strftime("%Y-%m-%d")    # Конец месяца
    )
