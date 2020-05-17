import datetime


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


"""""
垃圾字切片
"""""


def char_filter(target, *trash_key):
    for trash_word in trash_key:
        if trash_word in target:
            target = target[:target.index(trash_word)]
    return target


"""""
字元取代循環
"""""


def symbols_change(word, target=None):
    if target is None:
        target = {}
    if len(word) > 0:
        for sym, value in target.items():
            if sym in word:
                word = word.replace(sym, '_' + value)
        if word[0] is '_':
            word = word[1:]
    return word
