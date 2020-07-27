from crawlers.models import *
import pandas as pd
from components.price_feature import GetAdj
import numpy as np


class Backtest(GetAdj):
    def __init__(self, start_date: str, strategy, model=StockPriceTW, price_choice='close_price',
                 market=None, fee_ratio=4 / 10000, tax_ratio=3 / 1000):
        super().__init__(start_date, model, price_choice, market)
        self.strategy = strategy
        self.fee_ratio = fee_ratio
        self.tax_ratio = tax_ratio

    def get_backtest_data(self):
        backtest_data = GetAdj(self.start_date).normal_date_mode()
        backtest_data = backtest_data.reindex(self.strategy.index)
        return backtest_data

    def period_return(self, num, backtest_data, strategy):
        backtest_shift = backtest_data.shift(1)
        return_table = (backtest_data * (1 - self.fee_ratio) - (
                backtest_shift * (self.fee_ratio + self.tax_ratio))) / backtest_shift
        return_table = return_table.dropna(how='all')

        current_select = strategy.iloc[num].dropna()
        current_select = current_select[current_select]
        current_idx = current_select.index
        result = return_table[current_idx].iloc[num].mean()
        return result

    @staticmethod
    def max_drawdown(arr):
        i = np.argmax((np.maximum.accumulate(arr) - arr) / np.maximum.accumulate(arr))  # end of the period
        j = np.argmax(arr[:i])  # start of period
        data = {'max_drawdown': (1 - arr[i] / arr[j]), 'start': j, 'end': i}
        return data

    def report(self):
        backtest_data = self.get_backtest_data()
        report = {}
        benchmark = backtest_data['0050']
        benchmark_shift = benchmark.shift(1)
        report['benchmark'] = ((benchmark * (1 - self.fee_ratio) - (
                benchmark_shift * (self.fee_ratio + self.tax_ratio))) / benchmark_shift).fillna(1)
        report['date'] = backtest_data.index
        report['result'] = np.array(
            [1] + [self.period_return(i, backtest_data, self.strategy) for i in range(len(self.strategy) - 1)])
        report['cumprod_result'] = report['result'].cumprod()
        max_drawdown = self.max_drawdown(report['cumprod_result'])
        report['max_drawdown'] = max_drawdown['max_drawdown']
        max_drawdown_period = [max_drawdown['start'], max_drawdown['end']]
        report['max_drawdown_period'] = [report['date'][i] for i in max_drawdown_period]

        report['benchmark'].cumprod().plot()
        pd.Series(report['cumprod_result'], index=report['date']).plot()
        return report
