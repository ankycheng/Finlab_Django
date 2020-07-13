import datetime


def year_transfer(t):
    """""
    民國年日期轉換
    """""
    try:
        date = str(int(t[:t.index("/")]) + 1911) + t[t.index("/"):].replace('/', '-')
    except AttributeError:
        t = str(t)
        date = str(int(t[:-4]) + 1911) + '-' + t[-4:-2] + '-' + t[-2:]
    except ValueError:
        date = None
    return date


def last_month(date):
    """""
    月週期爬蟲,轉換成上月
    """""
    if date.month == 12:
        url_date = datetime.date(date.year, 11, 1)
    elif date.month > 1:
        url_date = datetime.date(date.year, ((date.month % 12) - 1), 1)
    else:
        url_date = datetime.date(date.year - 1, 12, 1)

    return url_date


def char_filter(target, *trash_key):
    """""
    垃圾字切片
    """""
    for trash_word in trash_key:
        if trash_word in target:
            target = target[:target.index(trash_word)]
    return target


def symbols_change(word, target=None):
    """""
    字元取代循環
    """""
    if target is None:
        target = {}
    if len(word) > 0:
        for sym, value in target.items():
            if sym in word:
                word = word.replace(sym, '_' + value)
        if word[0] is '_':
            word = word[1:]
    return word


def url_month(month):
    """""
    月份補0
    """""
    if month < 10:
        m = '0' + str(month)
    else:
        m = str(month)
    return m
