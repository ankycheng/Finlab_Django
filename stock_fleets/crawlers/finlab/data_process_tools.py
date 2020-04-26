import datetime
import requests
import pandas as pd
from io import StringIO

"""""
民國年日期轉換
"""""


def year_transfer(t):
    try:
        date = str(int(t[:t.index("/")]) + 1911) + t[t.index("/"):].replace('/', '-')
    except AttributeError:
        t = str(t)
        date = str(int(t[:-4]) + 1911) + '-' + t[-4:-2] + '-' + t[-2:]
    except ValueError:
        date = None
    return date


"""""
月週期爬蟲,轉換成上月
"""""


def last_month(date):
    if date.month == 12:
        url_date = datetime.date(date.year, 11, 1)
    elif date.month > 1:
        url_date = datetime.date(date.year, ((date.month % 12) - 1), 1)
    else:
        url_date = datetime.date(date.year - 1, 12, 1)

    return url_date


