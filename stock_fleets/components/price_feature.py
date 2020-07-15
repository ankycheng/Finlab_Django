from components.backend_api import DataFilter
import numpy as np
from crawlers.models import *


class GetAdj:
    def __init__(self, col_name, offset=0, limit=100000):
        self.col_name = col_name
        self.offset = int(offset)
        self.limit = int(limit)

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
        columns = ['stock_id', 'date', self.col_name]
        price = DataFilter(StockPriceTW, columns)
        item = price.get_pivot()
        divide_ratio = DataFilter(StockDivideRatioTW, ['stock_id', 'date', 'divide_ratio'])
        ratio = self.adj_holiday(item, divide_ratio.get_pivot())
        divide_ratio = (ratio.reindex_like(item).fillna(1).cumprod())
        divide_ratio[np.isinf(divide_ratio)] = 1
        result = (item * divide_ratio)[-self.offset:-self.offset + self.limit].dropna(axis=1)
        return result
