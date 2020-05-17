import pandas as pd
from django.db.models import Q
import datetime
from crawlers.finlab.import_tools import engine


class GetModelDateRangeBySlice:
    def __init__(self, model, offset=0, limit=100000, recent=True):
        self.model = model
        self.offset = offset
        self.limit = limit
        self.recent = recent

    def get_date_list(self, conn):
        table = self.model._meta.db_table
        if self.recent:
            cursor = sorted(list(conn.execute("SELECT DISTINCT date FROM " + table)))[-self.offset:self.limit]
        else:
            cursor = sorted(list(conn.execute("SELECT DISTINCT date FROM " + table)))[
                     self.offset:self.offset + self.limit]
        cursor = [cursor[i][0] for i in range(len(cursor))]
        return cursor


class OrmBasicFilter(GetModelDateRangeBySlice):
    def __init__(self, model, stock_id, start_date=None, end_date=None, offset=0, limit=100000, recent=True,
                 fields=None):
        super().__init__(model, offset, limit, recent)
        self.stock_id = stock_id
        self.start_date = start_date
        self.end_date = end_date
        self.fields = fields

    def basic_filter_set(self):
        stock_id = {}
        start_date = {}
        end_date = {}
        if self.stock_id is not None:
            stock_id['stock_id'] = self.stock_id
        if self.start_date is not None:
            start = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
            start_date['date__gte'] = start
        if self.end_date is not None:
            end = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
            end_date['date__lte'] = end
        if (self.offset is not 0) and (self.limit is not 100000):
            date_range = self.get_date_list(engine)
            start_date['date__gte'] = date_range[0]
            end_date['date__lte'] = date_range[-1]
        filter_set = {'stock_id': stock_id, 'start_date': start_date, 'end_date': end_date}
        return filter_set

    def get_orm_data(self):
        fs = self.basic_filter_set()
        if self.fields is not None:
            fields = self.fields.split('-')
            orm_data = self.model.objects.filter(Q(**fs['stock_id']), Q(**fs['start_date']),
                                                 Q(**fs['end_date'])).order_by('date').values(*fields)
        else:
            orm_data = self.model.objects.filter(Q(**fs['stock_id']), Q(**fs['start_date']),
                                                 Q(**fs['end_date'])).order_by('date').values()
        return orm_data

    def get_dataframe(self):
        df = pd.DataFrame(self.get_orm_data())
        return df
