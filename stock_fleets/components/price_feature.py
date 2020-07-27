from components.backend_api import DataFilter
import numpy as np
from crawlers.models import StockPriceTW, StockDivideRatioTW
import pandas as pd


class AdjustedPrice(DataFilter):

    @staticmethod
    def adj_holiday(item, df):
        all_index = (df.index | item.index).sort_values()
        all_index = all_index[all_index >= item.index[0]]
        df = df.reindex(all_index)
        group = all_index.isin(item.index).cumsum()
        df = df.groupby(group).mean()
        df.index = item.index
        return df

    def get(self):
        columns = ['stock_id', 'date', self.fields]
        query_set = {'model': self.model, 'fields': columns, 'offset': self.offset, 'limit': self.limit,
                     'start_date': self.start_date, 'end_date': self.end_date, 'market': self.market}
        price = DataFilter(**query_set)
        item = price.get_pivot()
        divide_ratio = DataFilter(StockDivideRatioTW, ['stock_id', 'date', 'divide_ratio'])
        ratio = self.adj_holiday(item, divide_ratio.get_pivot())
        divide_ratio = (ratio.reindex_like(item).fillna(1).cumprod())
        divide_ratio[np.isinf(divide_ratio)] = 1
        result = (item * divide_ratio).fillna(method='ffill', limit=1)
        return result


class GetAdj:
    def __init__(self, start_date: str, interval=None, model=StockPriceTW, price_choice='close_price',
                 market=None):
        if market is None:
            market = ['sii', 'otc']
        self.start_date = start_date
        self.interval = interval
        self.model = model
        self.price_choice = price_choice
        self.market = market

    def price_adj(self):
        price_adj = AdjustedPrice(model=self.model, fields=self.price_choice, start_date=self.start_date,
                                  market=self.market).get()
        return price_adj

    def trade_date_mode(self, df=None):
        if df is None:
            df = self.price_adj()
        date_range = list(df.index[::self.interval])
        last_date = df.index[-1]
        if last_date not in date_range:
            date_range.append(last_date)
        result = df.loc[date_range]
        return result

    def normal_date_mode(self):
        df = self.price_adj()
        last_date = df.index[-1]
        date_range = pd.date_range(start=self.start_date, end=last_date)
        df = df.reindex(date_range).fillna(method='ffill').dropna(how='all')
        result = self.trade_date_mode(df)
        return result
