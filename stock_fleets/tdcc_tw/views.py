from components.backend_api import OrmBasicFilter
from crawlers.models import StockPriceTW, StockTdccTW
from fastapi import FastAPI

app = FastAPI()


class ReloadTdccTW(OrmBasicFilter):
    group_list = ['little', 'medium', 'big', 'super', 'total']

    def dataframe_process(self):
        group_list = self.group_list
        df = self.get_dataframe()
        df['hold_class_group'] = [
            group_list[0] if i < 10 else group_list[1] if i < 12 else group_list[2] if i < 15 else group_list[
                3] if i < 16 else group_list[4] for i in df['hold_class']]
        df = df.groupby(['stock_id', 'date', 'hold_class_group'])[['people', 'hold_num', 'hold_pt']].sum()
        df['hold_num'] = round(df['hold_num'] / 1000)
        df['hold_pt'] = round(df['hold_pt'], 2)
        df = df.reset_index()
        return df

    def group_data(self):
        context = {}
        df = self.dataframe_process()
        context["stock_id"] = self.stock_id
        context["date"] = sorted(list(set(df['date'].values)))
        df = df.drop(columns=['stock_id', 'date'])
        for group in self.group_list:
            context[group] = list(
                df[df['hold_class_group'] == group].drop(columns='hold_class_group').T.to_dict().values())
        context['price'] = list(StockPriceTW.objects.filter(stock_id=self.stock_id, date__in=context["date"]).values())
        return context


async def read_tdcc(stock_id: str, start_date: str = None, end_date: str = None, offset: int = 0, limit: int = 100000,
                    recent: int = 1, fields: list = None):
    query_params = {'model': StockTdccTW, 'stock_id': stock_id, 'start_date': start_date,
                    'end_date': end_date, 'offset': offset, 'limit': limit, 'recent': recent, 'fields': fields}
    context = ReloadTdccTW(**query_params).group_data()
    return context
