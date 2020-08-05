from crawlers.models import StockPriceTW
import pandas as pd
from components.price_feature import GetAdj
import numpy as np
import math


class Backtest(GetAdj):
    def __init__(self, start_date: str, strategy, model=StockPriceTW, price_choice='close_price',
                 market=None, fee_ratio=4 / 10000, tax_ratio=3 / 1000, benchmark='0050'):
        super().__init__(start_date, model, price_choice, market)
        self.strategy = strategy
        self.fee_ratio = fee_ratio
        self.tax_ratio = tax_ratio
        self.benchmark = benchmark

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
        data = {'result': result, 'nstock': len(current_idx)}
        return data

    @staticmethod
    def max_drawdown(arr):
        i = np.argmax((np.maximum.accumulate(arr) - arr) / np.maximum.accumulate(arr))  # end of the period
        j = np.argmax(arr[:i])  # start of period
        data = {'max_drawdown': (1 - arr[i] / arr[j]), 'start': j, 'end': i}
        return data

    def report(self):
        backtest_data = self.get_backtest_data()
        report = {}
        benchmark = backtest_data[self.benchmark]
        benchmark_shift = benchmark.shift(1)
        report['benchmark'] = (benchmark / benchmark_shift).fillna(1)
        report['date'] = backtest_data.index
        backtest_info = [self.period_return(i, backtest_data, self.strategy) for i in range(len(self.strategy) - 1)]
        report['result'] = np.array([1] + [i['result'] for i in backtest_info])
        report['nstock'] = np.array([0] + [i['nstock'] for i in backtest_info])
        for m, i in enumerate(report['result']):
            if np.isnan(i):
                report['result'][m] = report['result'][m - 1]
        report['cumprod_result'] = report['result'].cumprod()
        max_drawdown = self.max_drawdown(report['cumprod_result'])
        report['max_drawdown'] = max_drawdown['max_drawdown']
        max_drawdown_period = [max_drawdown['start'], max_drawdown['end']]
        report['max_drawdown_period'] = [report['date'][i] for i in max_drawdown_period]
        report['annual_return'] = math.pow(report['cumprod_result'][-1], 12 / (len(report['date'])))
        return report

    def plot(self):
        df = self.report()
        df['benchmark'].cumprod().plot()
        pd.Series(df['cumprod_result'], index=df['date']).plot()
