from components.backend_api import DataFilter
from crawlers.models import StockPriceTW
import pandas as pd


class ReloadTdccTW(DataFilter):
    # little:100-down,medium:100-400,big:400-1000,super:1000-up
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


class TdccStrategy(ReloadTdccTW):
    def get_tdcc_table(self):
        df = self.dataframe_process()
        df = df.set_index(['date', 'stock_id'])
        return df

    @staticmethod
    def tdcc_select(dataframe, column: str, hold_level: str, rank_max: float, rank_min: float):
        df = dataframe[dataframe['hold_class_group'] == hold_level]
        table = pd.pivot_table(df, index=['date'], columns=['stock_id'], values=column)

        start = table.iloc[0]
        finish = table.iloc[-1]

        growth_ratio = round(((finish - start) / start).dropna() * 100, 2).sort_values(ascending=False)
        rank_series = growth_ratio.rank(pct=True)
        rank_series_select = (rank_series >= rank_min) & (rank_series <= rank_max)

        return rank_series_select

    def select_list(self, shp_rank_max=1, shp_rank_min=0, bhp_rank_max=1, bhp_rank_min=0, lhp_rank_max=1,
                    lhp_rank_min=0, lp_rank_max=1, lp_rank_min=0, issued_num_max=100000000, issued_num_min=0,
                    ps_rank_max=1, ps_rank_min=0):
        df = self.get_tdcc_table()
        cond1 = self.tdcc_select(df, 'hold_pt', 'super', float(shp_rank_max), float(shp_rank_min))
        cond2 = self.tdcc_select(df, 'hold_pt', 'big', float(bhp_rank_max), float(bhp_rank_min))
        cond3 = self.tdcc_select(df, 'hold_pt', 'little', float(lhp_rank_max), float(lhp_rank_min))
        cond4 = self.tdcc_select(df, 'people', 'little', float(lp_rank_max), float(lp_rank_min))

        # 股本篩選
        df2 = df[df['hold_class_group'] == 'total']
        df2 = df2.loc[df2.index.get_level_values(0).max()]
        cond5 = (df2['hold_num'] >= int(issued_num_min)) & (df2['hold_num'] <= int(issued_num_max))

        # 小股東賣壓篩選
        df2 = df[df['hold_class_group'] == 'little']
        df2 = df2.loc[df2.index.get_level_values(0).max()]
        little_hold = df2['hold_pt'].rank(pct=True)
        cond6 = (little_hold >= float(ps_rank_min)) & (little_hold <= float(ps_rank_max))

        cond_all = cond1 & cond2 & cond3 & cond4 & cond5 & cond6
        select_list = list(cond_all[cond_all].index)
        return select_list
