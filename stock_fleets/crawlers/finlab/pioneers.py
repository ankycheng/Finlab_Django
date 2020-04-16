import pandas as pd
from io import StringIO
import requests
import datetime
from crawlers.finlab.data_process_tools import year_transfer, last_month
import time

"""""
爬蟲-台股上市櫃股價
"""""


class CrawlStockPriceTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime("%Y%m%d")
        self.target_name = "台股每日交易資訊"
        self.sub_market = ["sii", "otc", "rotc"]

    def crawl_sii(self):

        r = requests.post(
            "http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=" + self.date_str + "&type=ALLBUT0999")

        content = r.text.replace("=", "")
        lines = content.split("\n")
        lines = list(filter(lambda l: len(l.split('",')) > 10, lines))
        content = "\n".join(lines)
        if content == "":
            return None
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(",", ""))
        df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        df["date"] = pd.to_datetime(self.date)
        df = df.loc[:, ["證券代號", "date", "證券名稱", "成交股數", "成交金額", "開盤價", "收盤價", "最高價", "最低價"]]
        df = df.rename(columns={"證券代號": "stock_id", "證券名稱": "stock_name",
                                "成交股數": "turnover_vol", "成交金額": "turnover_price",
                                "開盤價": "open_price", "收盤價": "close_price",
                                "最高價": "high_price", "最低價": "low_price"})
        df = df.where(pd.notnull(df), None)

        return df

    @staticmethod
    def select_otc_id(code):
        if len(code) > 5:
            if code[-1] == "P":
                return False
            else:
                try:
                    code = int(code)
                    if code > 10000:
                        return False
                    else:
                        return True
                except ValueError:
                    return True
        else:
            return True

    def crawl_otc(self):

        y = str(int(self.date.strftime("%Y")) - 1911)
        date_str = y + "/" + self.date.strftime("%m") + "/" + self.date.strftime("%d")

        link = "http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d=" \
               + date_str + "&s=0,asc,0"
        r = requests.get(link)

        lines = r.text.replace("\r", "").split("\n")
        try:
            df = pd.read_csv(StringIO("\n".join(lines[3:])), header=None)
        except pd.errors.ParserError:
            return None
        df.columns = list(map(lambda s: s.replace(" ", ""), lines[2].split(",")))
        df = df.apply(lambda s: s.str.replace(",", ""))
        df["stock_id"] = df["代號"]
        df["代號"] = df["代號"].apply(lambda s: self.select_otc_id(s))
        df = df[df["代號"]]
        df["date"] = pd.to_datetime(self.date)
        df = df.loc[:, ["stock_id", "date", "名稱", "成交股數", "成交金額(元)", "開盤", "收盤", "最高", "最低"]]
        df = df.rename(columns={"名稱": "stock_name",
                                "成交股數": "turnover_vol", "成交金額(元)": "turnover_price",
                                "開盤": "open_price", "收盤": "close_price",
                                "最高": "high_price", "最低": "low_price"})
        df.iloc[:, 3:] = df.iloc[:, 3:].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        df = df[df["turnover_vol"] >= 0]
        df = df.where(pd.notnull(df), None)

        return df

    def crawl_rotc(self):

        link = "http://www.tpex.org.tw/web/emergingstock/historical/daily/EMDaily_dl.php?l=zh-tw&f=EMdes010." + \
               self.date_str + "-C.csv"

        r = requests.get(link)
        lines = r.text.replace("\r", "").split("\n")
        try:
            columns_line = lines[3]
        except IndexError:
            return None
        lines = list(filter(lambda l: len(l.split('",')) > 10, lines))
        try:
            df = pd.read_csv(StringIO("\n".join(lines)), header=None)
        except pd.errors.EmptyDataError:
            return None
        df.columns = list(map(lambda l: l.replace(" ", ""), columns_line.split(",")))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(",", ""))
        df.iloc[:, 3:] = df.iloc[:, 3:].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        df["date"] = pd.to_datetime(self.date)
        if "證券名稱" in df.columns:
            df = df.loc[:, ["證券代號", "date", "證券名稱", "成交量", "成交金額", "前日均價", "最後", "最高", "最低"]]

        # old format("名稱")
        else:
            df = df.loc[:, ["證券代號", "date", "名稱", "成交量", "成交金額", "前日均價", "最後", "最高", "最低"]]
            df = df.rename(columns={"名稱": "證券名稱"})

        df = df.rename(columns={"證券代號": "stock_id", "證券名稱": "stock_name",
                                "成交量": "turnover_vol", "成交金額": "turnover_price",
                                "前日均價": "open_price", "最後": "close_price",
                                "最高": "high_price", "最低": "low_price"})

        # solve " "
        df["stock_id"] = df["stock_id"].apply(lambda s: s[:4])
        df = df[df["stock_id"] != "合計"]
        df = df.where(pd.notnull(df), None)
        return df

    def crawl_main(self):
        try:
            df = pd.concat([self.crawl_sii(), self.crawl_otc(), self.crawl_rotc()])
        except ValueError:
            return None
        return df


"""""
爬蟲-台股月營收
"""""


class CrawlMonthlyRevnueTW:
    def __init__(self, date):
        self.date = date
        self.target_name = "台股月營收資訊"
        self.sub_market = ["sii", "otc", "rotc"]

    def crawl_main(self):
        url_date = last_month(self.date)
        data = []
        for i in self.sub_market:

            url = 'https://mops.twse.com.tw/nas/t21/' + i + '/t21sc03_' + str(url_date.year - 1911) + '_' + str(
                url_date.month) + '.html'

            # 偽瀏覽器
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko)'
                              ' Chrome/39.0.2171.95 Safari/537.36'}

            # 下載該年月的網站，並用pandas轉換成 dataframe
            try:
                r = requests.get(url, headers=headers)
                r.encoding = 'big5'
                html_df = pd.read_html(StringIO(r.text))
                # 處理一下資料
                if html_df[0].shape[0] > 500:
                    df = html_df[0].copy()
                else:
                    df = pd.concat([df for df in html_df if (df.shape[1] <= 11) and (df.shape[1] > 5)])

                if 'levels' in dir(df.columns):
                    df.columns = df.columns.get_level_values(1)
                else:
                    df = df[list(range(0, 10))]
                    column_index = df.index[(df[0] == '公司代號')][0]
                    df.columns = df.iloc[column_index]

                df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
                df = df[~df['當月營收'].isnull()]
                df = df[df['公司代號'] != '合計']

                df['date'] = datetime.date(self.date.year, self.date.month, 10)

                df = df.rename(columns={'公司代號': 'stock_id'})
                df = df.set_index(['stock_id', 'date'])

                data.append(df)
            except Exception as e:
                print(e)
                print('**WARRN: Pandas cannot find any table in the HTML file')
                return None
        df = pd.concat(data)
        if '備註' not in df.columns:
            df['備註'] = None
        df.iloc[:, 1:-1] = df.iloc[:, 1:-1].apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df = df[df['公司名稱'] != '總計']
        df = df.where(pd.notnull(df), None)
        df = df.rename(columns={'公司名稱': "stock_name", "當月營收": "this_month_rev",
                                '上月營收': "last_month_rev", "去年當月營收": "last_year_rev",
                                '上月比較增減(%)': "cp_last_month_rev", "去年同月增減(%)": "cp_last_year_rev",
                                '當月累計營收': "cm_this_month_rev", "去年累計營收": "cm_last_month_rev",
                                '前期比較增減(%)': "cp_cm_rev", "備註": "note",
                                })
        df = df.reset_index()

        return df


"""""
爬蟲-台股上市櫃、興櫃企業基本資料
"""""


class CrawlCompanyBasicInfoTW:
    def __init__(self):
        self.target_name = "台股企業基本資訊"
        self.sub_market = ["sii", "otc", "rotc"]

    def crawl_main(self):
        data = []
        market_category = self.sub_market
        for market in market_category:
            url = "https://mops.twse.com.tw/mops/web/ajax_t51sb01"
            form_data = {
                "encodeURIComponent": "1",
                "step": "1",
                "firstin": "1",
                "TYPEK": market
            }

            res = requests.post(url, data=form_data)
            res.encoding = "utf-8"
            df = pd.read_html(res.text)
            df = pd.DataFrame(df[0])
            data.append(df)

        df2 = pd.concat(data)
        df2 = df2.astype(str)
        df2 = df2.apply(lambda s: s.str.replace(",", ""))
        df3 = df2.loc[:, ["公司代號", "update_time", "公司名稱", "公司簡稱", "產業類別", "外國企業註冊地國", "住址",
                          "董事長", "總經理", "發言人", "發言人職稱", "總機電話",
                          "成立日期", "上市日期", "上櫃日期", "興櫃日期", "實收資本額(元)", "已發行普通股數或TDR原發行股數",
                          "私募普通股(股)", "特別股(股)", "普通股盈餘分派或虧損撥補頻率", "股票過戶機構", "簽證會計師事務所",
                          "公司網址", "投資人關係聯絡電話", "投資人關係聯絡電子郵件", "英文簡稱"]]

        df3 = df3.rename(columns={
            "公司代號": "stock_id", "公司名稱": "name",
            "公司簡稱": "short_name", "產業類別": "category",
            "外國企業註冊地國": "registered_country", "住址": "address",
            "董事長": "chairman", "總經理": "ceo",
            "發言人": "spokesman", "發言人職稱": "spokesman_title",
            "總機電話": "phone", "成立日期": "establishment_date",
            "上市日期": "sii_date", "上櫃日期": "otc_date",
            "興櫃日期": "rotc_date", "已發行普通股數或TDR原發行股數": "shares_issued",
            "私募普通股(股)": "private_shares", "特別股(股)": "special_shares",
            "普通股盈餘分派或虧損撥補頻率": "dividend_frequency", "股票過戶機構": "stock_transfer_institution",
            "簽證會計師事務所": "visa_accounting_firm", "公司網址": "website",
            "投資人關係聯絡電話": "investor_relations_contact", "投資人關係聯絡電子郵件": "investor_relations_email",
            "英文簡稱": "english_abbreviation", "實收資本額(元)": "capital"

        })
        # Data format Process
        df3 = df3[df3["stock_id"] != "公司代號"]
        df3["registered_country"] = df3["registered_country"].apply(lambda s: s.replace("－", "台灣"))

        for share_column in ["capital", "shares_issued", "private_shares", "special_shares"]:
            df3[share_column] = df3[share_column].apply(lambda s: pd.to_numeric(s, errors="coerce"))

        for date_column in ["establishment_date", "sii_date", "otc_date", "rotc_date"]:
            df3[date_column] = df3[date_column].apply(lambda t: year_transfer(t))

        df3["update_time"] = datetime.datetime.now()
        df3 = df3.fillna('')
        return df3
