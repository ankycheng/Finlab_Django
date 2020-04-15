import pandas as pd
from io import StringIO
import requests
import datetime
from crawlers.finlab.data_process_tools import year_transfer, last_month

"""""
爬蟲-台股上市櫃股價
"""""


def crawl_stock_price_tw(date):
    # 上市爬蟲,將 date 變成字串 舉例：'20180525'
    datestr = date.strftime('%Y%m%d')

    # 從網站上依照 datestr 將指定日期的股價抓下來
    r = requests.post(
        'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALLBUT0999')

    # 將抓下來的資料（r.text），其中的等號給刪除
    content = r.text.replace('=', '')

    # 將 column 數量小於等於 10 的行數都刪除
    lines = content.split('\n')
    lines = list(filter(lambda l: len(l.split('",')) > 10, lines))

    # 將每一行再合成同一行，並用肉眼看不到的換行符號'\n'分開
    content = "\n".join(lines)

    # 假如沒下載到，則回傳None（代表抓不到資料）
    if content == '':
        return None

    # 將content變成檔案：StringIO，並且用pd.read_csv將表格讀取進來
    df = pd.read_csv(StringIO(content))

    # 將表格中的元素都換成字串，並把其中的逗號刪除
    df = df.astype(str)
    df = df.apply(lambda s: s.str.replace(',', ''))

    # 將「證券代號」的欄位改名成「stock_id」
    df = df.rename(columns={'證券代號': 'stock_id'})
    # 設定date欄位
    df['date'] = pd.to_datetime(date)
    # 將 「stock_id」與「date」設定成index
    df = df.set_index(['stock_id', 'date'])

    # 保留證券名稱,將所有的表格元素都轉換成數字，error='coerce'的意思是說，假如無法轉成數字，則用 NaN 取代
    df = pd.concat([df.iloc[:, :1], df.iloc[:, 1:].apply(lambda s: pd.to_numeric(s, errors='coerce'))], axis=1)

    # 刪除不必要的欄位
    df = df[df.columns[df.isnull().all() == False]]

    # 新增欄位
    df = df.loc[:, ["證券名稱", '成交股數', '成交金額', '開盤價', '收盤價', '最高價', '最低價']]

    # 上櫃爬蟲，將 date 變成字串 舉例：'1071012'
    y = str(int(date.strftime('%Y')) - 1911)

    datestr = y + '/' + date.strftime('%m') + '/' + date.strftime('%d')
    link = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d=' + \
           datestr + '&s=0,asc,0'
    r = requests.get(link)

    lines = r.text.replace('\r', '').split('\n')
    df2 = pd.read_csv(StringIO("\n".join(lines[3:])), header=None)

    # 設定欄名
    df2.columns = list(map(lambda l: l.replace(' ', ''), lines[2].split(',')))

    # 資料處理
    df2 = df2.apply(lambda s: s.str.replace(',', '')).apply(lambda s: s.str.replace('+', ''))
    df2 = df2.rename(columns={'代號': '舊證券代號', "名稱": "證券名稱", "收盤": "收盤價", "漲跌": "漲跌價", "開盤": "開盤價",
                              "最高": "最高價", "最低": "最低價", '成交金額(元)': '成交金額'})
    df2['stock_id'] = df2['舊證券代號']
    df2 = pd.concat([df2.iloc[:, :1].apply(lambda s: pd.to_numeric(s, errors='coerce')), df2.iloc[:, 1:]], axis=1)
    df2 = df2[df2['舊證券代號'] < 9999]
    df2['date'] = pd.to_datetime(date)
    df2 = df2.set_index(['stock_id', 'date'])
    df2 = df2.drop(['舊證券代號'], axis=1)
    df2 = pd.concat([df2.iloc[:, :1], df2.iloc[:, 1:].apply(lambda s: pd.to_numeric(s, errors='coerce'))], axis=1)

    df2 = df2.loc[:, ["證券名稱", '成交股數', '成交金額', '開盤價', '收盤價', '最高價', '最低價']]

    # 上市與上櫃合體
    df3 = pd.concat([df, df2], axis=0)
    df3 = df3.rename(columns={'證券名稱': 'stock_name', "成交股數": "turnover_vol",
                              "成交金額": "turnover_price", "開盤價": "open_price",
                              "收盤價": "close_price", "最高價": "high_price",
                              "最低價": "low_price"})
    df3.iloc[:, 3:] = df3.iloc[:, 3:].apply(lambda s: pd.to_numeric(s, errors='coerce'))

    df3 = df3.where(pd.notnull(df3), None)
    df3 = df3.reset_index()
    return df3


"""""
爬蟲-台股月營收
"""""


def crawl_monthly_revenue_tw(date):
    url_date = last_month(date)
    data = []
    for i in ['sii', 'otc', 'rotc']:  # ,'otc','rotc'

        url = 'https://mops.twse.com.tw/nas/t21/' + i + '/t21sc03_' + str(url_date.year - 1911) + '_' + str(
            url_date.month) + '.html'
        print(url)

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
                df = pd.concat([df for df in html_df if df.shape[1] <= 11 and df.shape[1] > 5])

            if 'levels' in dir(df.columns):
                df.columns = df.columns.get_level_values(1)
            else:
                df = df[list(range(0, 10))]
                column_index = df.index[(df[0] == '公司代號')][0]
                df.columns = df.iloc[column_index]

            df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
            df = df[~df['當月營收'].isnull()]
            df = df[df['公司代號'] != '合計']

            df['date'] = datetime.date(date.year, date.month, 10)

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


def crawl_company_basic_info_tw():
    data = []
    market_category = ["sii", "otc", "rotc"]
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
