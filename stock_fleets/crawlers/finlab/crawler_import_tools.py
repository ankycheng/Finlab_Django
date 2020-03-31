import datetime
import time
import os
import pandas as pd
from tqdm import tnrange, tqdm_notebook
from datetime import date
from dateutil.rrule import rrule, DAILY, MONTHLY
from django.core.exceptions import ObjectDoesNotExist
from sqlalchemy import create_engine
import json5

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('/crawlers', '')
with open(os.path.join(BASE_DIR, "config.json"), 'r', encoding='utf8') as file:
    CONFIG_DATA = json5.load(file)
connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(CONFIG_DATA['DBACCOUNT'], CONFIG_DATA['DBPASSWORD'],
                                                                    CONFIG_DATA['DBHOST'], CONFIG_DATA['DBPORT'],
                                                                    CONFIG_DATA['DBNAME'])
engine = create_engine(connect_info)


def date_range(start_date, end_date):
    return [dt.date() for dt in rrule(DAILY, dtstart=start_date, until=end_date)]


def month_range(start_date, end_date):
    return [dt.date() for dt in rrule(MONTHLY, dtstart=start_date, until=end_date)]


def season_range(start_date, end_date):
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.date()

    if isinstance(end_date, datetime.datetime):
        end_date = end_date.date()

    ret = []
    for year in range(start_date.year - 1, end_date.year + 1):
        ret += [datetime.date(year, 5, 15),
                datetime.date(year, 8, 14),
                datetime.date(year, 11, 14),
                datetime.date(year + 1, 3, 31)]
    ret = [r for r in ret if start_date < r < end_date]

    return ret


def table_exist(conn, table):
    return list(conn.execute(
        "select count(*) from information_schema.tables where TABLE_NAME=" + "'" + table + "'"))[0][0] == 1


def table_latest_date(conn, table):
    cursor = list(conn.execute('SELECT date FROM ' + table + ' ORDER BY date DESC LIMIT 1;'))
    return cursor[0][0]


def table_earliest_date(conn, table):
    cursor = list(conn.execute('SELECT date FROM ' + table + ' ORDER BY date ASC LIMIT 1;'))
    return cursor[0][0]


def in_date_list(conn, model_name, check_date):
    table = model_name._meta.db_table
    cursor = list(conn.execute("SELECT date FROM " + table + " where date ='" + check_date + "'"))
    if len(cursor) > 0:
        return True
    else:
        return False


def add_to_sql(model_name, df):
    bulk_update_data = []
    bulk_create_data = []

    # if data_date isn't in table,process bulk_create
    data_date = df['date'].iloc[0].strftime('%Y-%m-%d')
    check_date = in_date_list(engine, model_name, data_date)

    if check_date == False:
        # Change CSV to iterrow
        for index, item in df.iterrows():
            # Use bulk_update to update obj,PS:must include primekey column
            try:
                obj_create_data = dict((field.name, item[field.name]) for field in model_name._meta.fields if
                                       field.name != 'id')
                obj_create = model_name(**obj_create_data)
                bulk_create_data.append(obj_create)
                print(f"create{' '}{model_name}{' '}Stock_id:{item['stock_id']}{' '}Date:{item['date']}")

            except Exception as e:
                print(f"error{' '}{e}{' '}Stock_id:{item['stock_id']}{' '}Date:{item['date']}")
                pass

    else:
        # Change CSV to iterrow
        for index, item in df.iterrows():

            # Use bulk_update to update obj,PS:must include primekey column
            try:
                obj_check = model_name.objects.get(stock_id=item['stock_id'], date=item['date'])
                obj_update_data = list(
                    (field.name, item[field.name]) if field.name != 'id' else (field.name, obj_check.id) for field in
                    model_name._meta.fields)

                for attributes, update_value in obj_update_data:
                    obj_check.attribute = update_value

                bulk_update_data.append(obj_check)
                print(f"update{' '}{model_name}{' '}Stock_id:{item['stock_id']}{' '}Date:{item['date']}")

            # Use dict to bulk_create obj when get nothing ,process incomplete data
            except ObjectDoesNotExist:
                obj_create_data = dict((field.name, item[field.name]) for field in model_name._meta.fields if
                                       field.name != 'id')
                obj_create = model_name(**obj_create_data)
                bulk_create_data.append(obj_create)
                print(f"create{' '}{model_name}{' '}Stock_id:{item['stock_id']}{' '}Date:{item['date']}")

            except Exception as e:
                print(f"error{' '}{e}{' '}Stock_id:{item['stock_id']}{' '}Date:{item['date']}")
                pass

    # Process bulk
    model_name.objects.bulk_create(bulk_create_data, batch_size=1000)
    print(f"Finish{' '}{model_name}{'bulk_create'}{':'}{len(bulk_create_data)}")
    update_fields_area = [field.name for field in model_name._meta.fields if field.name != 'id']
    model_name.objects.bulk_update(bulk_update_data, update_fields_area, batch_size=1000)
    print(f"Finish{' '}{model_name}{'bulk_update'}{':'}{len(bulk_update_data)}")


class CrawlerProcess:

    def __init__(self, func, model_name):
        self.crawler_func_name = func
        self.model_name = model_name
        self.table_latest_date = table_latest_date(engine, self.model_name._meta.db_table)

    def __repr__(self):
        return str(self.model_name._meta.db_table) + ' ' + "table_latest_date:" + str(self.table_latest_date)

    def crawl_process(self, date_list: list):
        for d in date_list:
            df = self.crawler_func_name(d)
            try:
                ret = df.drop_duplicates(['stock_id', 'date'], keep='last')
                add_to_sql(self.model_name, ret)
                print(f'Finish {d} Data')

            # holiday is blank
            except AttributeError:
                print(f'fail, check if {d} is a holiday')
            time.sleep(12)

    # 指定區間，主要為測試用
    def specified_date_crawl(self, start_date: str, end_date: str, range_date=date_range):

        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        try:
            if (start_date - end_date).days <= 0:
                date_list = range_date(start_date, end_date)
                self.crawl_process(date_list)
            else:
                print(f"The start_date > your end_date,please modify your start_date <={end_date} .")
                return None
        except ValueError:
            print('Error:last_day form is %Y-%m-%d,please modify. ')
            return None

            # 自動爬取結尾後日期的資料

    def working_process(self):
        recent = datetime.datetime.now()
        day_num = (recent - self.table_latest_date).days
        if day_num > 0:
            return 0
        elif day_num == 0:
            return 1
        else:
            return 2

    def auto_update_crawl(self, last_day='Now', range_date=date_range):

        try:
            if last_day == 'Now':
                end_date = datetime.datetime.now()
            else:
                end_date = datetime.datetime.strptime(last_day, "%Y-%m-%d")

            if self.working_process() == 0:
                start_date = self.table_latest_date
                date_list = range_date(start_date, end_date)
                self.crawl_process(date_list)

            elif self.working_process() == 1:
                print(f"Finish Update Work")
                return None
            else:
                print(f"The table_latest_date > your setting date,please modify your setting date >{last_day} .")
                return None
        except ValueError:
            print('Error:last_day form is %Y-%m-%d,please modify. ')
            return None
