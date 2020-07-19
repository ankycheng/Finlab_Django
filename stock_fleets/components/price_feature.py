from components.backend_api import DataFilter
import numpy as np
from crawlers.models import StockDivideRatioTW


class GetAdj(DataFilter):

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
                     'start_date': self.start_date, 'end_date': self.end_date}
        price = DataFilter(**query_set)
        item = price.get_pivot()
        divide_ratio = DataFilter(StockDivideRatioTW, ['stock_id', 'date', 'divide_ratio'])
        ratio = self.adj_holiday(item, divide_ratio.get_pivot())
        divide_ratio = (ratio.reindex_like(item).fillna(1).cumprod())
        divide_ratio[np.isinf(divide_ratio)] = 1
        result = (item * divide_ratio)
        return result
