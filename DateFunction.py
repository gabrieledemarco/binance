from datetime import datetime
from dateutil.relativedelta import *


def datetime_to_date(input_data):
    return input_data.date()


def datetime_to_milliseconds_int(input_data: datetime):
    return int(input_data.timestamp()) * 1000


def milliseconds_to_datetime(input_data: int):
    return datetime.fromtimestamp(input_data / 1000)


def string_to_datetime(input_data, format_date: str):
    return datetime.strptime(input_data, format_date)


def now_date():
    return datetime.now()


def limit(start_date: datetime, end_date: datetime):
    return (datetime_to_date(input_data=end_date) - datetime_to_date(input_data=start_date)).days


def get_past_date(data_end: datetime, day: int = 0, month: int = 0, year: int = 0):
    return data_end + relativedelta(day=+day) + relativedelta(months=+month) + relativedelta(year=+year)
