import datetime
import time
import pandas as pd
from dateutil.rrule import rrule, DAILY, MONTHLY
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.conf import settings
from sqlalchemy import create_engine
from django.db import models

"""""
DB connection
"""""

dbName = (
    settings.CONFIG_DATA.get("DBNAME")
    if settings.CONFIG_DATA.get("PRODUCTION")
    else settings.CONFIG_DATA.get("DBNAME_DEV")
)
connect_info = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4".format(
    settings.CONFIG_DATA.get("DBACCOUNT"),
    settings.CONFIG_DATA.get("DBPASSWORD"),
    settings.CONFIG_DATA.get("DBHOST"),
    settings.CONFIG_DATA.get("DBPORT"),
    dbName,
)
engine = create_engine(connect_info)

"""""
date generator
"""""


def date_range(start_date, end_date):
    return [dt for dt in rrule(DAILY, dtstart=start_date, until=end_date)]


def month_range(start_date, end_date):
    return [dt for dt in rrule(MONTHLY, dtstart=start_date, until=end_date)]


def season_range(start_date, end_date):
    if isinstance(start_date, datetime.datetime):
        start_date = start_date
    if isinstance(end_date, datetime.datetime):
        end_date = end_date
    ret = []
    for year in range(start_date.year - 1, end_date.year + 1):
        ret += [datetime.date(year, 5, 15),
                datetime.date(year, 8, 14),
                datetime.date(year, 11, 14),
                datetime.date(year + 1, 3, 31)]
    ret = [r for r in ret if start_date < r < end_date]
    return ret


"""""
Table check tools
"""""


def table_exist(conn, table):
    return list(conn.execute(
        "select count(*) from information_schema.tables where TABLE_NAME=" + "'" + table + "'"))[0][0] == 1


def table_latest_date(conn, table):
    try:
        cursor = list(conn.execute('SELECT date FROM ' + table + ' ORDER BY date DESC LIMIT 1;'))
        return cursor[0][0]
    except IndexError:
        return print("No Data")


def table_earliest_date(conn, table):
    try:
        cursor = list(conn.execute('SELECT date FROM ' + table + ' ORDER BY date ASC LIMIT 1;'))
        return cursor[0][0]
    except IndexError:
        return print("No Data")


def in_date_list(conn, model_name, check_date):
    table = model_name._meta.db_table
    cursor = list(conn.execute("SELECT date FROM " + table + " where date ='" + check_date + "'"))
    try:
        if len(cursor) > 0:
            return True
        else:
            return False
    except IndexError:
        print("No Data in table,start to init import table. ")
        return False


"""""
Dataframe匯入DB,適用時間序列資料,fk_columns-外鍵對應過濾用欄位名
"""""


class AddToSQL:
    @classmethod
    def get_fk_field_names(cls, model_name):
        fk_field_names = [field.name for field in model_name._meta.fields
                          if isinstance(field, models.ForeignKey)]
        return fk_field_names

    @classmethod
    def corr_obj(cls, model_name, attributes):
        try:
            df = model_name.objects.get(**attributes)
            return df
        except ObjectDoesNotExist:
            pass

    @classmethod
    def fk_import(cls, model_name, df, fk_columns):
        fk_field_names = cls.get_fk_field_names(model_name)
        fk_remote_model = [model_name._meta.get_field(i).remote_field.model for i in fk_field_names]
        filter_dict = [{a: df[b] for a, b in sub_dict.items()} for sub_dict in fk_columns]
        fk_obj = [cls.corr_obj(m, n) for m, n in zip(fk_remote_model, filter_dict)]
        data = {'fk_field_names': fk_field_names, 'fk_obj': fk_obj}
        return data

    @classmethod
    def fk_create(cls, model_name, df, fk_columns):
        data = cls.fk_import(model_name, df, fk_columns)
        fk_create_data = dict((m, n) for m, n in zip(data['fk_field_names'], data['fk_obj']))
        return fk_create_data

    @classmethod
    def fk_update(cls, model_name, df, fk_columns):
        data = cls.fk_import(model_name, df, fk_columns)
        return zip(data['fk_field_names'], data['fk_obj'])

    @classmethod
    def pk_select(cls, df, pk_columns=None):
        if pk_columns is None:
            pk_columns = ['stock_id', 'date']
        get_pk_dict = {pk: df[pk] for pk in pk_columns}
        get_pk_contain_dict = {pk + '__contains': df[pk] for pk in pk_columns}
        return [get_pk_dict, get_pk_contain_dict]

    @classmethod
    def add_to_sql(cls, model_name, df, pk_columns=None, fk_columns=None, jump_create=False, jump_update=False):
        df = df.where(pd.notnull(df), None)
        columns_list = list(df.columns.values)
        bulk_update_data = []
        bulk_create_data = []

        def bulk_create_func(bc_model_name, bc_columns_list):
            obj_create_data = dict((field, item[field]) for field in bc_columns_list)
            # 處理ForeignKey
            if fk_columns is not None:
                obj_create_data.update(cls.fk_create(model_name, item, fk_columns))
            obj_create = bc_model_name(**obj_create_data)
            bulk_create_data.append(obj_create)

        # if data_date isn't in table,process bulk_create
        if 'date' in columns_list:
            data_date = df['date'].iloc[0].strftime('%Y-%m-%d')
            check_date = in_date_list(engine, model_name, data_date)
        else:
            check_date = True
        if check_date is False:
            # Change CSV to iterrow
            for index, item in df.iterrows():
                # Use bulk_update to update obj,PS:must include prime_key column
                try:
                    bulk_create_func(model_name, columns_list)
                except Exception as e:
                    print(f"error{' '}{e}{' '}Stock_id:{item['stock_id']}")
                    pass
        else:
            if jump_create is True:
                return None
            else:
                for index, item in df.iterrows():
                    # Use bulk_update to update obj,PS:must include primary key column
                    if jump_update is False:
                        try:
                            pk_filter = cls.pk_select(item, pk_columns)
                            try:
                                obj_check = model_name.objects.get(**pk_filter[0])
                            # 無法區分字母大小寫,回傳2個值
                            except MultipleObjectsReturned:
                                obj_check = model_name.objects.get(**pk_filter[1])

                            attribute_data = columns_list
                            update_data = [item[field] for field in columns_list]
                            for attribute, update_value in zip(attribute_data, update_data):
                                setattr(obj_check, attribute, update_value)
                            # ForeignKey update
                            if fk_columns is not None:
                                for attribute, update_value in cls.fk_update(model_name, item, fk_columns):
                                    setattr(obj_check, attribute, update_value)
                            bulk_update_data.append(obj_check)
                        # Use dict to bulk_create obj when get nothing ,process incomplete data
                        except ObjectDoesNotExist:
                            bulk_create_func(model_name, columns_list)
                        except Exception as e:
                            print(f"error{' '}{e}{' '}Stock_id:{item['stock_id']}")
                            pass
                    else:
                        bulk_create_func(model_name, columns_list)
        # Process bulk
        model_name.objects.bulk_create(bulk_create_data, batch_size=1000)
        if 'date' in columns_list:
            print_date = df['date'].values[0]
        else:
            print_date = datetime.datetime.now()
        print(f"Finish{' '}{model_name}{'date'}{':'}{print_date}{' '}{'bulk_create'}{':'}{len(bulk_create_data)}")
        update_fields_area = [field.name for field in model_name._meta.fields if field.name != 'id']
        model_name.objects.bulk_update(bulk_update_data, update_fields_area, batch_size=1000)
        print(f"Finish{' '}{model_name}{'date'}{':'}{print_date}{' '}{'bulk_update'}{':'}{len(bulk_update_data)}")


"""""
爬蟲執行物件,適用時間序列資料
Attributes:
crawl_class-爬蟲類別、crawl_method-類別執行方法、model_name-存取資料庫模型、range_date-爬蟲日期產生器週期選擇
time_sleep-爬蟲間隔時間
nest-巢狀爬蟲，一日內執行多次，分次輸入，不用整併每日後再輸入，避免中斷白工
pk_columns-update檢測用主鍵群
fk_columns-外鍵對應過濾用欄位名
"""""


class CrawlerProcess:
    def __init__(self, crawl_class, crawl_method, model_name, range_date, nest=False, time_sleep=13, pk_columns=None,
                 fk_columns=None, jump_create=False, jump_update=False):
        self.crawl_class = crawl_class
        self.crawl_method = crawl_method
        self.model_name = model_name
        self.table_earliest_date = table_earliest_date(engine, self.model_name._meta.db_table)
        self.table_latest_date = table_latest_date(engine, self.model_name._meta.db_table)
        self.range_date = range_date
        self.nest = nest
        self.time_sleep = time_sleep
        self.pk_columns = pk_columns
        self.fk_columns = fk_columns
        self.jump_create = jump_create
        self.jump_update = jump_update

    def __repr__(self):
        return str(self.model_name._meta.db_table) + ' ' + "\ntable_earliest_date:" + str(
            self.table_earliest_date) + "\ntable_latest_date:" + str(self.table_latest_date)

    def crawl_process(self, date_list: list):
        for d in date_list:
            if self.nest is False:
                df = getattr(self.crawl_class(d), self.crawl_method)()
                try:
                    AddToSQL.add_to_sql(self.model_name, df, self.pk_columns, self.fk_columns, self.jump_create,
                                        self.jump_update)
                    print(f'Finish {d} Data')
                # holiday is blank
                except AttributeError:
                    print(f'fail, check if {d} is a holiday')
            else:
                try:
                    df = getattr(self.crawl_class(d), self.crawl_method)()
                    if df is False:
                        print(f'fail, check if {d} is a holiday')
                # holiday is blank
                except AttributeError:
                    print(f'fail, check if {d} is a holiday')
            time.sleep(self.time_sleep)

    @staticmethod
    def monthly_import(start_date, end_date, deadline):
        if end_date.day > deadline:
            print(f"day > {deadline},Finish Update Work")
            return None
        # add 15 day to append month range,because deadline is 10,if the day is 1-9,it need update
        end_date = end_date + datetime.timedelta(days=deadline)
        date_list = month_range(start_date, end_date)
        # jump next month, quick_method
        if end_date.month is not start_date.month:
            date_list = date_list[1:]
        return date_list

    # 指定區間，主要為初始化table和測試用
    def specified_date_crawl(self, start_date: str, end_date: str, deadline=15):
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        try:
            if (start_date - end_date).days <= 0:
                if self.range_date == 'date_range':
                    date_list = date_range(start_date, end_date)[1:]
                elif self.range_date == 'month_range':
                    date_list = self.monthly_import(start_date, end_date, deadline)
                else:
                    print(f"Finish Update Work")
                    return None
                self.crawl_process(date_list)
            else:
                print(f"The start_date > your end_date,please modify your start_date <={end_date} .")
        except Exception as e:
            print(e)
            return None

    # 進度判斷
    def working_process(self, start_date, end_date):
        day_num = (end_date - start_date).days
        if self.range_date == 'date_range':
            if day_num > 0:
                return 0
            elif day_num == 0:
                return 1
            else:
                return 2
        else:
            return 0

    # 自動爬取結尾後日期的資料,jump－跳過最近日更新
    def auto_update_crawl(self, last_day='Now', deadline=15, jump=True):
        try:
            if last_day == 'Now':
                end_date = datetime.datetime.now().date()
            else:
                end_date = datetime.datetime.strptime(last_day, "%Y-%m-%d").date()
            start_date = self.table_latest_date
            working_process = self.working_process(start_date, end_date)
            if working_process == 0:
                if self.range_date == 'date_range':
                    # [1:] avoid same index,let program be faster
                    if jump is True:
                        date_list = date_range(start_date, end_date)[1:]
                    else:
                        date_list = date_range(start_date, end_date)
                elif self.range_date == 'month_range':
                    date_list = self.monthly_import(start_date, end_date, deadline)
                else:
                    date_list = season_range(start_date, end_date)
                self.crawl_process(date_list)
            elif working_process == 1:
                print(f"Finish Update Work")
                return None
            else:
                print(f"The table_latest_date > your setting date,please modify your setting date >{last_day} .")
                return None
        except Exception as e:
            print(e)
            return None
