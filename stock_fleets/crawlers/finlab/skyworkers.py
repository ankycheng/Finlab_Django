import pandas as pd
from io import StringIO
import requests


class CrawlFuturePriceTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime('%Y/%m/%d')
        self.target_name = "台股期貨價格資訊"

    def crawl_main(self):
        para = '&queryStartDate=' + self.date_str + '&queryEndDate=' + self.date_str
        url = 'https://www.taifex.com.tw/cht/3/futDataDown?down_type=1&commodity_id=all&commodity_id2=' + para
        r = requests.post(url)
        lines = r.text.replace('\r', '').split('\n')
        lines = list(filter(lambda l: len(l.split(',')) > 10, lines))
        if len(lines) < 10:
            return None
        content = "\n".join(lines)
        df = pd.read_csv(StringIO(content))
        df = df.drop(columns='價差對單式委託成交量')
        df = df.reset_index()
        df.columns = lines[0].split(',')
        df = df.astype(str)
        df = df.drop(columns=['交易日期', '漲跌價', '歷史最高價', '歷史最低價'])
        df = df.rename(columns={k: v for k, v in zip(df.columns, ['stock_id', 'contract_date', 'open', 'high',
                                                                  'low', 'close', 'quote_change', 'turnover_vol',
                                                                  'settlement_price', 'open_interest', 'best_bid',
                                                                  'best_ask',
                                                                  'trading_halt', 'trading_session',
                                                                  'cross_contract_vol'])})
        to_numeric_col = list(df.columns)[2:]
        to_numeric_col.remove('trading_session')
        df[to_numeric_col] = df[to_numeric_col].apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df['date'] = self.date
        return df


class CrawlCommodityTaifex:
    @classmethod
    def stock_relate(cls):
        r = requests.post('https://www.taifex.com.tw/cht/2/stockLists')
        r.encoding = 'utf8'
        lines = r.text.replace('\r', '').split('\n')
        content = "\n".join(lines)
        df = pd.read_html(StringIO(content))
        df = pd.DataFrame(df[0])
        df = df.astype(str)
        df['證券代號'] = df['證券代號'].apply(lambda s: s[:-2])
        df.iloc[:, 4:9] = df.iloc[:, 4:9].replace('●', 1).replace('◎', 1).replace('nan', 0)
        df['標準型證券股數'] = df['標準型證券股數'].apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df = df.drop(columns=['標的證券'])
        df['標的證券簡稱'] = df['標的證券簡稱'].apply(lambda s: s + '期')
        df = df.rename(columns={k: v for k, v in zip(df.columns, ['stock_id', 'spot_id', 'stock_name', 'check_fc',
                                                                  'check_opt', 'check_sii', 'check_otc', 'check_etf',
                                                                  'spot_unit'])})
        df['spot_id'] = df['spot_id'].apply(lambda s: '00' + s if len(s) < 4 else s)
        return df.iloc[:-1]

    @staticmethod
    def modify_df(df):
        if '中文簡稱.1' in df.columns:
            df['中文簡稱'] = [a if a == b else a + b for a, b in zip(df['中文簡稱'].values, df['中文簡稱.1'].values)]
            df = df.drop(columns=['中文簡稱.1'])
        return df

    @classmethod
    def normal(cls):
        r = requests.post('https://www.taifex.com.tw/cht/4/contractName')
        r.encoding = 'utf8'
        lines = r.text.replace('\r', '').split('\n')
        content = "\n".join(lines)
        df = pd.read_html(StringIO(content))
        df_all = pd.concat([cls.modify_df(pd.DataFrame(df[i])) for i in range(len(df))])
        df_all = df_all.dropna(thresh=8, axis=1).dropna(how='any', axis=0)
        df_all = df_all.rename(columns={k: v for k, v in zip(df_all.columns, ['stock_name', 'stock_id'])})
        return df_all

    @classmethod
    def crawl_main(cls):
        try:
            df = pd.concat([cls.stock_relate(), cls.normal()])
            df.iloc[:, 3:8] = df.iloc[:, 3:8].fillna(0)
        except ValueError:
            return None
        return df
