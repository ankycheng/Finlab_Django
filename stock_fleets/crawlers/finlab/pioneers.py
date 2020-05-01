import pandas as pd
from io import StringIO
import requests
import datetime
from crawlers.finlab.data_process_tools import year_transfer, last_month, char_filter
from crawlers.finlab.import_tools import AddToSQL
from crawlers.models import CompanyBasicInfoTW, BrokerInfoTW, BrokerTradeTW
from django.core.exceptions import ObjectDoesNotExist


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
        df['market'] = '上市'
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
        df['market'] = '上櫃'
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
        df['stock_id'] = df['stock_id'].apply(lambda s: s[:s.index(' ')] if '" "' in s else s)
        df['stock_name'] = df['stock_name'].apply(lambda s: s[:s.index(' ')] if '" "' in s else s)
        df = df[df["stock_id"] != "合計"]
        df['market'] = '興櫃'
        df = df.where(pd.notnull(df), None)
        return df

    def crawl_main(self):
        try:
            df = pd.concat([self.crawl_sii(), self.crawl_otc(), self.crawl_rotc()])
        except ValueError:
            return None
        return df


class CrawlMonthlyRevnueTW:
    def __init__(self, date):
        self.date = date
        self.target_name = "台股月營收資訊"
        self.sub_market = ["sii", "otc", "rotc"]
        self.format = "time_series"

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
        df = pd.concat(data, sort=False)
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
            df['market'] = '上市' if market == 'sii' else '上櫃' if market == 'otc' else '興櫃'
            data.append(df)
        df2 = pd.concat(data, sort=False)
        df2 = df2.astype(str)
        df2 = df2.apply(lambda s: s.str.replace(",", ""))
        df3 = df2.loc[:, ["公司代號", "update_time", "公司名稱", "公司簡稱", "產業類別", "外國企業註冊地國", "住址",
                          "董事長", "總經理", "發言人", "發言人職稱", "總機電話",
                          "成立日期", "上市日期", "上櫃日期", "興櫃日期", "實收資本額(元)", "已發行普通股數或TDR原發行股數",
                          "私募普通股(股)", "特別股(股)", "普通股盈餘分派或虧損撥補頻率", "股票過戶機構", "簽證會計師事務所",
                          "公司網址", "投資人關係聯絡電話", "投資人關係聯絡電子郵件", "英文簡稱", "market"]]
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

    @staticmethod
    def update_xy():
        GetNTLSxy.update_xy_data(CompanyBasicInfoTW)


class CrawlStockIndexPriceTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime("%Y%m%d")
        self.target_name = "台股指數資訊"
        self.sub_market = ["sii", "otc"]
        self.format = "time_series"

    def sii_index(self):
        r = requests.post(
            'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + self.date_str + '&type=IND')
        content = r.text.replace('=', '')
        lines = content.split('\n')
        lines = list(filter(lambda l: len(l.split('",')) > 5, lines))
        content = "\n".join(lines)
        if content == '':
            return None
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        df = df.rename(columns={'指數': 'stock_id', '收盤指數': 'index_price',
                                '漲跌百分比(%)': 'quote_change'})
        df['date'] = pd.to_datetime(self.date)
        df['stock_id'] = df['stock_id'].apply(lambda s: '上市' + s)
        df.loc[:, ['index_price', 'quote_change']] = df.loc[:, ['index_price', 'quote_change']].apply(
            lambda s: pd.to_numeric(s, errors='coerce'))

        df_all = df.loc[:, ['stock_id', 'date', 'index_price', 'quote_change']]
        df_all = df_all.dropna()
        return df_all

    def otc_index(self):

        y = str(int(self.date.strftime("%Y")) - 1911)
        date_str = y + "/" + self.date.strftime("%m") + "/" + self.date.strftime("%d")
        link = 'http://www.tpex.org.tw/web/stock/aftertrading/index_summary/summary_download.php?l=zh-tw&d=' + \
               date_str + '&s=0,asc,0'
        r = requests.get(link)
        lines = r.text.replace("\r", "").split("\n")
        try:
            df = pd.read_csv(StringIO("\n".join(lines[3:])), header=None)
        except pd.errors.ParserError:
            return None
        df.columns = list(map(lambda s: s.replace(" ", ""), lines[2].split(",")))
        df = df.apply(lambda s: s.str.replace(",", ""))
        df['stock_id'] = '上櫃' + (df['指數'].apply(lambda s: s.replace('指數', ''))) + '指數'
        # 第二個櫃買指數以下的才是報酬指數，找出第二個，各年指數項目不同使用find來定位
        rem_loc = df['指數'].str.find('櫃買指數')
        rem_loc = (rem_loc[rem_loc > -1].index.tolist())[-1]
        # 一般指數
        df_normal = df.iloc[:rem_loc]
        # 報酬指數
        df_rem = df.iloc[rem_loc:]
        # 合併
        df_all = pd.concat([df_normal, df_rem])
        df_all = df_all.rename(columns={'收市指數': 'index_price', '漲跌幅度': 'quote_change'})
        df_all.loc[:, ['index_price', 'quote_change']] = df_all.loc[:, ['index_price', 'quote_change']].apply(
            lambda s: pd.to_numeric(s, errors='coerce'))
        df_all['date'] = pd.to_datetime(self.date)
        df_all = df_all.loc[:, ['stock_id', 'date', 'index_price', 'quote_change']]
        df_all = df_all.dropna()
        return df_all

    def crawl_main(self):
        try:
            df = pd.concat([self.sii_index(), self.otc_index()], sort=False)
        except ValueError:
            return None
        return df


class CrawlStockIndexVolTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime("%Y%m%d")
        self.target_name = "台股指數成交量資訊"
        self.sub_market = ["sii", "otc"]
        self.format = "time_series"

    def sii_vol(self):
        r = requests.post('http://www.twse.com.tw/exchangeReport/BFIAMU?response=csv&date=' + self.date_str)
        content = r.text.replace('=', '')
        lines = content.split('\n')
        lines = list(filter(lambda l: len(l.split('",')) > 4, lines))
        content = "\n".join(lines)
        if content == '':
            return None
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        df = df.rename(
            columns={'分類指數名稱': 'stock_id', '成交股數': 'turnover_vol', '成交金額': 'turnover_price', '成交筆數': 'turnover_num'})
        df['date'] = pd.to_datetime(self.date)
        df.loc[:, ['turnover_vol', 'turnover_price', 'turnover_num']] = df.loc[:, ['turnover_vol', 'turnover_price',
                                                                                   'turnover_num']].apply(
            lambda s: pd.to_numeric(s, errors='coerce'))
        df = df.drop(columns=['漲跌指數', 'Unnamed: 5'])
        df['stock_id'] = df['stock_id'].apply(lambda s: '上市' + s)
        return df

    def sii_statistic(self):
        r = requests.post(
            'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + self.date_str + '&type=MS')
        content = r.text.replace('=', '')
        lines = content.split('\n')
        lines = list(filter(lambda l: len(l.split('",')) < 6, lines))
        lines = lines[1:]
        content = "\n".join(lines)
        if content == '':
            return None
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        df = df.rename(columns={'成交統計': 'stock_id', '成交金額(元)': 'turnover_price',
                                '成交股數(股)': 'turnover_vol', '成交筆數': 'turnover_num'})
        df = df.drop(columns={'Unnamed: 4'})
        df['date'] = pd.to_datetime(self.date)
        df.iloc[:, 1:4] = df.iloc[:, 1:4].apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df = df.dropna()
        df['stock_id'] = df['stock_id'].apply(lambda s: '上市' + s[s.index(".") + 1:] if "." in s else '上市' + s)
        return df

    def otc_statistic(self):
        y = str(int(self.date.strftime("%Y")) - 1911)
        date_str = y + "/" + self.date.strftime("%m") + "/" + self.date.strftime("%d")
        link = 'https://www.tpex.org.tw/web/stock/aftertrading/market_statistics/statistics_result.php?l=zh-tw&t=D&o=' \
               'htm&d=' + date_str
        r = requests.get(link)
        lines = r.text.replace("\r", "").split("\n")
        if len(lines) < 35:
            return None
        df = pd.read_html(StringIO("\n".join(lines[3:])), header=None)[0]
        df = pd.DataFrame(df)
        df.columns = df.columns.get_level_values(1)
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        df = df.rename(columns={'成交統計': 'stock_id', '成交金額(元)': 'turnover_price',
                                '成交股數(股)': 'turnover_vol', '成交筆數': 'turnover_num'})
        df = df.loc[:, ['stock_id', 'turnover_vol', 'turnover_price', 'turnover_num']]
        df['date'] = pd.to_datetime(self.date)
        df.iloc[:, 1:4] = df.iloc[:, 1:4].apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df = df.dropna()
        df['stock_id'] = df['stock_id'].apply(lambda s: '上櫃' + s[s.index(".") + 1:] if "." in s else '上櫃' + s)
        return df

    def crawl_main(self):
        try:
            df = pd.concat([self.sii_vol(), self.sii_statistic(), self.otc_statistic()], sort=False)
        except ValueError:
            return None
        return df


class CrawlBrokerInfoTW:
    def __init__(self):
        self.target_name = "台股券商資訊"
        self.format = "non_time_series"

    @staticmethod
    def headquarter_info():
        r = requests.get('https://www.twse.com.tw/zh/brokerService/brokerServiceAudit')
        html_df = pd.read_html(StringIO(r.text))
        df = pd.DataFrame(html_df[0])
        df['department'] = '總公司'
        df = df.drop(columns='分公司')
        return df

    @staticmethod
    def branch_info(broker_hq_id):
        url = 'https://www.twse.com.tw/brokerService/brokerServiceAudit?showType=list&stkNo=' + broker_hq_id + \
              '&focus=6'
        r = requests.get(url)
        html_df = pd.read_html(StringIO(r.text))
        df = pd.DataFrame(html_df[3])
        return df

    def crawl_main(self):
        broker_hq = self.headquarter_info()
        branch_data = pd.concat([self.branch_info(i) for i in broker_hq['證券商代號'].values])
        branch_data['department'] = '分公司'
        df_all = pd.concat([broker_hq, branch_data])
        df_all = df_all.rename(columns={'證券商代號': 'stock_id', '證券商名稱': 'broker_name',
                                        '開業日': 'date_of_establishment', '地址': 'address',
                                        '電話': 'phone'
                                        })
        df_all = df_all[df_all['stock_id'] != '查無資料']
        df_all['date_of_establishment'] = df_all['date_of_establishment'].apply(lambda t: year_transfer(t))
        df_all['broker_name'] = df_all['broker_name'].apply(lambda s: s.replace(' ', '').replace('證券', ''))
        return df_all

    @staticmethod
    def update_xy():
        GetNTLSxy.update_xy_data(BrokerInfoTW)


class GetNTLSxy:
    @classmethod
    def get_xy(cls, address):
        for i in ['新竹科學工業園區', '新竹科學園區', '大發工業區',
                  '南部科學工業園區', '平鎮工業區', '高雄加工出口區'
                                       '南崗工業區']:
            address = address.replace(i, '')
        address = char_filter(address, '及', '部分', '、', ',', '（')
        # 解決郵遞區號問題
        filter_num = filter(str.isalpha, address[:6])
        address = ''.join(list(filter_num)) + address[6:]
        url = 'https://moisagis.moi.gov.tw/moiap/gis2010/content/user/matchservice/singleMatch.cfm'
        form = {
            'address': address,
            'matchRange': '0',
            'fuzzyNum': '0',
            'roadEQstreet': 'false',
            'subnumEQnum': 'false',
            'isLockTown': 'false',
            'isLockVillage': 'false',
            'ex_coor': 'EPSG:4326',
            'U02DataYear': '2015',
            'output_xml': '1'
        }
        try:
            r = requests.post(url, data=form)
            html_df = pd.read_html(StringIO(r.text))
        except ValueError:
            return None
        df = pd.DataFrame(html_df[0])
        df = df.where(pd.notnull(df), None)
        return df

    # 地址有些漏區的
    @classmethod
    def main_process(cls, address):
        df = cls.get_xy(address)
        if df is None:
            return None
        elif df['X'].values[0] is None:
            address = address[:3] + '信義區' + address[3:]
            df = cls.get_xy(address)
            return df
        else:
            return df

    # 更新table中經緯度資料,start、end控制更新範圍,only_null控制是否只爬空值
    @classmethod
    def update_xy_data(cls, model_name, start=None, end=None, only_null=True):
        bulk_update_data = []
        if only_null is True:
            obj_list = model_name.objects.filter(longitude__isnull=True)[start:end]
        else:
            obj_list = model_name.objects.all()
        for obj_check in obj_list:
            location = obj_check.address
            print(location, obj_check.id)
            df = cls.main_process(location)
            if df is None:
                print('pass')
                continue
            obj_check.city = df['縣市'].values[0]
            obj_check.district = df['鄉鎮'].values[0]
            obj_check.longitude = df['X'].values[0]
            obj_check.latitude = df['Y'].values[0]
            bulk_update_data.append(obj_check)
        update_fields_area = ['city', 'district', 'latitude', 'longitude']
        model_name.objects.bulk_update(bulk_update_data, update_fields_area, batch_size=1000)


class CrawlBrokerTradeTW:
    def __init__(self, start_date):
        self.start_date = start_date
        self.start_date_str = start_date.strftime("%Y-%m-%d")
        self.target_name = "台股分點進出資訊"
        self.format = "time_series"

    def check_trade_day(self):
        stock_range = CrawlStockPriceTW(self.start_date)
        try:
            df = pd.concat([stock_range.crawl_sii(), stock_range.crawl_otc()])['stock_id'].values
            return df
        except ValueError:
            return None

    def broker_trade(self, stock_id):
        print(stock_id)
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a=' + stock_id + '&e=' \
              + self.start_date_str + \
              '&f=' + self.start_date_str
        r = requests.post(url)
        html_df = pd.read_html(StringIO(r.text))
        df = pd.DataFrame(html_df[2])
        # holiday
        if len(df) < 9:
            return None
        df.columns = df.iloc[5]
        buy_net_avg_cost = float(df[df['買超券商'] == '平均買超成本']['買超'].values[0])
        sell_net_avg_cost = float(df[df['賣超券商'] == '平均賣超成本']['賣超'].values[0])
        df = df.iloc[6:-3]
        buy_side = df.iloc[:, :5]
        buy_side = buy_side.rename(columns={'買超券商': 'broker_name', '買進': 'buy_num',
                                            '賣出': 'sell_num', '買超': 'net_bs',
                                            '佔成交比重': 'transactions_pt'})
        sell_side = df.iloc[:, 5:]
        sell_side = sell_side.rename(columns={'賣超券商': 'broker_name', '買進': 'buy_num',
                                              '賣出': 'sell_num', '賣超': 'net_bs',
                                              '佔成交比重': 'transactions_pt'})
        df_all = pd.concat([buy_side, sell_side], sort=False).dropna()
        df_all.iloc[:, 1:] = df_all.iloc[:, 1:].apply(lambda s: pd.to_numeric(s.str.replace('%', ''), errors="coerce"))
        df_all['net_bs'] = df_all['buy_num'] - df_all['sell_num']
        df_all['net_bs_cost'] = [i * buy_net_avg_cost if i > 0 else i * sell_net_avg_cost for i in df_all['net_bs']]
        df_all['date'] = pd.to_datetime(self.start_date)
        df_all['broker_name'] = df_all['broker_name'].apply(lambda s: s.replace('證券', '')).apply(
            lambda s: s.replace('(牛牛牛)', '犇'))
        df_all['broker_name'] = df_all['broker_name'].apply(lambda s: s if '停' not in s else s[:s.index('-')])
        df_all['stock_id'] = df_all['broker_name'].apply(lambda s: stock_id + '-' + s)
        AddToSQL.add_to_sql(BrokerTradeTW, df_all, 'broker_name')
        return df_all

    def crawl_main(self):
        crawl_list = self.check_trade_day()
        if crawl_list is not None:
            for stock_id in crawl_list[:5]:
                self.broker_trade(stock_id)
        else:
            pass


class CrawlStockTiiTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime("%Y%m%d")
        self.target_name = "台股三大法人個股買賣超資訊"
        self.sub_market = ["sii", "otc", "rotc"]

    def crawl_sii(self):
        r = requests.get('http://www.tse.com.tw/fund/T86?response=csv&date=' + self.date_str + '&selectType=ALLBUT0999')
        try:
            df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
        except pd.errors.EmptyDataError:
            return None
        df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
        df['證券代號'] = df['證券代號'].str.replace('=', '').str.replace('"', '')
        df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)
        df = df.rename(columns={'證券代號': 'stock_id', '證券名稱': 'stock_name',
                                '外陸資買進股數(不含外資自營商)': 'fm_buy', '外陸資賣出股數(不含外資自營商)': 'fm_sell',
                                '外陸資買賣超股數(不含外資自營商)': 'fm_net', '外資自營商買進股數': 'fd_buy',
                                '外資自營商賣出股數': 'fd_sell', '外資自營商買賣超股數': 'fd_net',
                                '投信買進股數': 'itc_buy', '投信賣出股數': 'itc_sell',
                                '投信買賣超股數': 'itc_net', '自營商買賣超股數': 'dealer_net',
                                '自營商買進股數(自行買賣)': 'dealer_ppt_buy', '自營商賣出股數(自行買賣)': 'dealer_ppt_sell',
                                '自營商買賣超股數(自行買賣)': 'dealer_ppt_net', '自營商買進股數(避險)': 'dealer_hedge_buy',
                                '自營商賣出股數(避險)': 'dealer_hedge_sell', '自營商買賣超股數(避險)': 'dealer_hedge_net',
                                '三大法人買賣超股數': 'tii_net'
                                })
        df['ft_net'] = df['fm_net'] + df['fd_net']
        df["date"] = pd.to_datetime(self.date)
        return df

    def crawl_otc(self):
        y = str(int(self.date.strftime("%Y")) - 1911)
        date_str = y + "/" + self.date.strftime("%m") + "/" + self.date.strftime("%d")
        r = requests.get(
            'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=' +
            date_str + '&s=0,asc')
        try:
            df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
        except pd.errors.ParserError:
            return None
        df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
        df['代號'] = df['代號'].str.replace('=', '').str.replace('"', '')
        df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)
        df = df.rename(columns={'代號': 'stock_id', '名稱': 'stock_name',
                                '外資及陸資(不含外資自營商)-買進股數': 'fm_buy', '外資及陸資(不含外資自營商)-賣出股數': 'fm_sell',
                                '外資及陸資(不含外資自營商)-買賣超股數': 'fm_net', '外資自營商-買進股數': 'fd_buy',
                                '外資自營商-賣出股數': 'fd_sell', '外資自營商-買賣超股數': 'fd_net',
                                '投信-買進股數': 'itc_buy', '投信-賣出股數': 'itc_sell',
                                '投信-買賣超股數': 'itc_net', '自營商-買賣超股數': 'dealer_net',
                                '自營商(自行買賣)-買進股數': 'dealer_ppt_buy', '自營商(自行買賣)-賣出股數': 'dealer_ppt_sell',
                                '自營商(自行買賣)-買賣超股數': 'dealer_ppt_net', '自營商(避險)-買進股數': 'dealer_hedge_buy',
                                '自營商(避險)-賣出股數': 'dealer_hedge_sell', '自營商(避險)-買賣超股數': 'dealer_hedge_net',
                                '三大法人買賣超股數合計': 'tii_net'
                                })
        df['ft_net'] = df['fm_net'] + df['fd_net']
        df = df.drop(columns=['外資及陸資-買進股數', '外資及陸資-賣出股數', '外資及陸資-買賣超股數', '自營商-買進股數', '自營商-賣出股數'])
        df["date"] = pd.to_datetime(self.date)
        return df

    def crawl_rotc(self):
        r = requests.get(
            'https://www.tpex.org.tw/web/emergingstock/historical/daily/EMDaily_dl.php?l=zh-tw&f=EMdss006.' + self.
            date_str + '-C.csv')
        try:
            df = pd.read_csv(StringIO(r.text), header=3).dropna(how='all', axis=1).dropna(how='any')
        except pd.errors.ParserError:
            return None
        df = df.drop(columns=['HEADER'])
        df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
        df['證券代號'] = df['證券代號'].apply(lambda s: s[:4])
        df.iloc[:, 2:] = df.iloc[:, 2:].apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)
        df = df.rename(columns={'證券代號': 'stock_id', '證券名稱': 'stock_name',
                                '外資(股數)': 'ft_net', '投信(股數)': 'itc_net',
                                '自營商(股數)': 'dealer_net', '合計買賣超(股數)': 'tii_net',
                                })
        df["date"] = pd.to_datetime(self.date)
        df['stock_id'] = df['stock_id'].apply(lambda s: s[:s.index(' ')] if '" "' in s else s)
        df['stock_name'] = df['stock_name'].apply(lambda s: s[:s.index(' ')] if '" "' in s else s)
        return df

    def crawl_main(self):
        try:
            df = pd.concat([self.crawl_sii(), self.crawl_otc(), self.crawl_rotc()], sort=False)
        except ValueError:
            return None
        return df


class CrawlStockTiiMarketReportTW:
    def __init__(self, date):
        self.date = date
        self.date_str = date.strftime("%Y%m%d")
        self.target_name = "台股三大法人全市場日報資訊"
        self.sub_market = ["sii", "otc"]
        self.format = "time_series"

    def crawl_sii(self):
        r = requests.get('http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=' + self.date_str + '&type=day')
        try:
            df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
        except pd.errors.EmptyDataError:
            return None
        df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)
        df = df.rename(
            columns={'單位名稱': 'stock_id', '買進金額': 'buy_price',
                     '賣出金額': 'sell_price', '買賣差額': 'net'})
        df = df.set_index(['stock_id'])
        df = df.rename(index={
            '自營商(自行買賣)': '上市自營商_自行買賣', '自營商(避險)': '上市自營商_避險',
            '投信': '上市投信', '外資及陸資(不含外資自營商)': '上市外資及陸資_不含外資自營商',
            '外資自營商': '上市外資自營商', '合計': '上市三大法人合計'})
        df = df.T
        df['上市外資及陸資合計'] = df['上市外資及陸資_不含外資自營商'] + df['上市外資自營商']
        df = df.T.reset_index()
        df["date"] = pd.to_datetime(self.date)
        return df

    def crawl_otc(self):
        y = str(int(self.date.strftime("%Y")) - 1911)
        date_str = y + "/" + self.date.strftime("%m") + "/" + self.date.strftime("%d")
        r = requests.get(
            'http://www.tpex.org.tw/web/stock/3insti/3insti_summary/3itrdsum_result.php?l=zh-tw&o=csv&se=EW&t=D&d=' +
            date_str + '&s=0,asc')
        try:
            df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
        except pd.errors.ParserError:
            return None
        df = df.astype(str).apply(lambda s: s.str.replace(',', '').str.replace('', ''))
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)
        df = df.rename(
            columns={'單位名稱': 'stock_id', '買進金額(元)': 'buy_price',
                     '賣出金額(元)': 'sell_price', '買賣超(元)': 'net'})
        df["date"] = pd.to_datetime(self.date)
        df = df.set_index(['stock_id'])
        df = df.rename(index={'　外資及陸資(不含自營商)': '  上櫃外資及陸資＿不含自營商', '　外資自營商': '上櫃外資自營商',
                              '　自營商(自行買賣)': '上櫃自營商_自行買賣', '　自營商(避險)': '自營商_避險',
                              '三大法人合計*': '上櫃三大法人合計', '外資及陸資合計': '上櫃外資及陸資合計',
                              '投信': '上櫃投信', '自營商合計': '上櫃自營商合計'
                              })
        df = df.reset_index()
        return df

    def crawl_main(self):
        try:
            df = pd.concat([self.crawl_sii(), self.crawl_otc()], sort=False)
        except ValueError:
            return None
        return df


class CrawlStockTdccTW:
    def __init__(self):
        self.target_name = "台股集保餘額資訊"
        self.sub_market = ["sii", "otc"]

    @classmethod
    def crawl_main(cls, file=False):
        if file is False:
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
            0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            res = requests.get("https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5", headers=headers)
            df = pd.read_csv(StringIO(res.text))
        else:
            df = pd.read_csv(file)
        df = df.astype(str)
        df = df[~df['證券代號'].str.contains('YY|YE')]
        df = df.rename(columns={
            '證券代號': 'stock_id_fk', '持股分級': 'hold_class',
            '人數': 'people', '股數': 'hold_num', '占集保庫存數比例%': 'hold_pt'
        })
        df['stock_id'] = df['stock_id_fk'] + '_' + df['hold_class']
        df = df[df['hold_class'] != '16']
        df.iloc[:, 2:6] = df.iloc[:, 2:6].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        df['date'] = df[df.columns[0]].apply(lambda s: datetime.datetime.strptime(s, '%Y%m%d'))
        df = df.drop(columns=df.columns[0])
        return df
