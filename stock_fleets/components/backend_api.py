import pandas as pd
from crawlers.finlab.import_tools import engine


class GetModelDateRangeBySlice:
    def __init__(self, model, offset=0, limit=100000, recent=1):
        self.model = model
        self.offset = int(offset)
        self.limit = int(limit)
        self.recent = int(recent)

    def get_date_list(self, conn):
        table = self.model._meta.db_table
        if self.recent == 1:
            cursor = sorted(list(conn.execute("SELECT DISTINCT date FROM " + table)))[
                     -self.offset:-self.offset + self.limit]
        else:
            cursor = sorted(list(conn.execute("SELECT DISTINCT date FROM " + table)))[
                     self.offset:self.offset + self.limit]
        cursor = [cursor[i][0] for i in range(len(cursor))]
        return cursor


class DataFilter(GetModelDateRangeBySlice):
    def __init__(self, model, fields=None, offset=0, limit=100000, stock_id=None, start_date=None, end_date=None,
                 recent=True):
        super().__init__(model, offset, limit, recent)
        self.stock_id = stock_id
        self.start_date = start_date
        self.end_date = end_date
        self.fields = fields

    def basic_filter_set(self):
        query = {}
        if self.stock_id is not None:
            query['stock_id'] = self.stock_id
        if self.start_date is not None:
            query['date__gte'] = self.start_date
        if self.end_date is not None:
            query['date__lte'] = self.end_date
        if (self.offset is not 0) and (self.limit is not 100000):
            date_range = self.get_date_list(engine)
            query['date__gte'] = date_range[0]
            query['date__lte'] = date_range[-1]
        return query

    def get_orm_data(self):
        fs = self.basic_filter_set()
        if self.fields is not None:
            orm_data = self.model.objects.filter(**fs).order_by('date').values(*self.fields)
        else:
            orm_data = self.model.objects.filter(**fs).order_by('date').values()
        return orm_data

    def get_dataframe(self):
        df = pd.DataFrame(self.get_orm_data())
        return df

    # stock_id,date must in fields head
    def get_pivot(self):
        df = self.get_dataframe()
        col = [i for i in self.fields if i not in ['stock_id', 'date']]
        if len(col) > 2:
            pivot_set = {}
            for f in col:
                table = pd.pivot_table(df, index=['date'], columns=['stock_id'], values=f)
                pivot_set[f] = table
        else:
            pivot_set = pd.pivot_table(df, index=['date'], columns=['stock_id'], values=col[0])
        return pivot_set


