from django.db import models


class StockPriceTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="證券名稱")
    open_price = models.FloatField(blank=True, null=True, verbose_name="開盤價")
    close_price = models.FloatField(blank=True, null=True, verbose_name="收盤價")
    high_price = models.FloatField(blank=True, null=True, verbose_name="最高價")
    low_price = models.FloatField(blank=True, null=True, verbose_name="最低價")
    turnover_vol = models.FloatField(blank=True, null=True, verbose_name="成交股數")
    turnover_price = models.FloatField(blank=True, null=True, verbose_name="成交金額")
    market = models.CharField(default=None, max_length=100, verbose_name="市場別")


class MonthlyRevenueTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="公司名稱")
    this_month_rev = models.FloatField(blank=True, null=True, verbose_name="當月營收")
    last_month_rev = models.FloatField(blank=True, null=True, verbose_name="上月營收")
    last_year_rev = models.FloatField(blank=True, null=True, verbose_name="去年當月營收")
    cp_last_month_rev = models.FloatField(blank=True, null=True, verbose_name="上月比較增減(%)")
    cp_last_year_rev = models.FloatField(blank=True, null=True, verbose_name="去年同月增減(%)")
    cm_this_month_rev = models.FloatField(blank=True, null=True, verbose_name="當月累計營收")
    cm_last_month_rev = models.FloatField(blank=True, null=True, verbose_name="去年累計營收")
    cp_cm_rev = models.FloatField(blank=True, null=True, verbose_name="前期比較增減(%)")
    note = models.CharField(max_length=500, null=True, verbose_name="備註")


class CompanyBasicInfoTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    update_time = models.DateField(verbose_name="資料日期")
    name = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司名稱")
    short_name = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司簡稱")
    category = models.CharField(blank=True, null=True, max_length=100, verbose_name="產業類別")
    registered_country = models.CharField(blank=True, null=True, max_length=500, verbose_name="企業註冊地國")
    address = models.CharField(blank=True, null=True, max_length=500, verbose_name="公司總部地址")
    chairman = models.CharField(blank=True, null=True, max_length=100, verbose_name="董事長")
    ceo = models.CharField(blank=True, null=True, max_length=100, verbose_name="總經理")
    spokesman = models.CharField(blank=True, null=True, max_length=100, verbose_name="發言人")
    spokesman_title = models.CharField(blank=True, null=True, max_length=100, verbose_name="發言人職稱")
    phone = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司總機")
    establishment_date = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司成立日")
    sii_date = models.CharField(blank=True, null=True, max_length=100, verbose_name="上市日")
    otc_date = models.CharField(blank=True, null=True, max_length=100, verbose_name="上櫃日")
    rotc_date = models.CharField(blank=True, null=True, max_length=100, verbose_name="興櫃日")
    capital = models.BigIntegerField(blank=True, null=True, verbose_name="資本額")
    shares_issued = models.BigIntegerField(blank=True, null=True, verbose_name="普通股發行股本")
    private_shares = models.BigIntegerField(blank=True, null=True, verbose_name="私募普通股數")
    special_shares = models.BigIntegerField(blank=True, null=True, verbose_name="特別股數")
    dividend_frequency = models.CharField(blank=True, null=True, max_length=100,
                                          verbose_name="普通股盈餘分派或虧損撥補頻率")
    stock_transfer_institution = models.CharField(blank=True, null=True, max_length=100, verbose_name="股票過戶機構")
    visa_accounting_firm = models.CharField(blank=True, null=True, max_length=100, verbose_name="簽證會計師事務所")
    website = models.CharField(blank=True, null=True, max_length=500, verbose_name="公司網址")
    investor_relations_contact = models.CharField(blank=True, null=True, max_length=500,
                                                  verbose_name="投資人關係聯絡電話")
    investor_relations_email = models.CharField(blank=True, null=True, max_length=500,
                                                verbose_name="投資人關係聯絡電子郵件")
    english_abbreviation = models.CharField(blank=True, null=True, max_length=100, verbose_name="英文簡稱")
    longitude = models.FloatField(blank=True, null=True, default=None, verbose_name="經度")
    latitude = models.FloatField(blank=True, null=True, default=None, verbose_name="緯度")
    city = models.CharField(blank=True, null=True, default=None, max_length=100, verbose_name="縣市")
    district = models.CharField(blank=True, null=True, default=None, max_length=100, verbose_name="鄉鎮區")
    market = models.CharField(default=None, max_length=100, verbose_name="市場別")

    def __str__(self):
        return self.stock_id + ' ' + str(self.short_name)

    @property
    def stock_issued_num(self):
        return round(self.capital / 5000)

    stock_issued_num.fget.short_description = '發行股票張數'


class StockIndexPriceTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    index_price = models.FloatField(blank=True, null=True, verbose_name="指數點數")
    quote_change = models.FloatField(blank=True, null=True, verbose_name="漲跌百分比")


class StockIndexVolTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    turnover_vol = models.FloatField(blank=True, null=True, verbose_name="成交股數")
    turnover_price = models.FloatField(blank=True, null=True, verbose_name="成交金額")
    turnover_num = models.BigIntegerField(blank=True, null=True, verbose_name="成交筆數")


class BrokerInfoTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="券商代號")
    broker_name = models.CharField(max_length=100, verbose_name="券商名稱")
    date_of_establishment = models.CharField(max_length=100, verbose_name="券商建立日")
    address = models.CharField(max_length=500, verbose_name="券商地址")
    phone = models.CharField(max_length=100, verbose_name="市話")
    department = models.CharField(max_length=100, verbose_name="券商部門")
    longitude = models.FloatField(blank=True, null=True, verbose_name="經度")
    latitude = models.FloatField(blank=True, null=True, verbose_name="緯度")
    city = models.CharField(blank=True, null=True, default=None, max_length=100, verbose_name="縣市")
    district = models.CharField(blank=True, null=True, default=None, max_length=100, verbose_name="鄉鎮區")


class BrokerTradeTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="分點進出代號")
    date = models.DateField(verbose_name="資料日期")
    buy_num = models.FloatField(blank=True, null=True, verbose_name="買進")
    sell_num = models.FloatField(blank=True, null=True, verbose_name="賣出")
    net_bs = models.FloatField(blank=True, null=True, verbose_name="買賣超")
    net_bs_cost = models.FloatField(blank=True, null=True, verbose_name="買賣超金額")
    transactions_pt = models.FloatField(blank=True, null=True, verbose_name="成交占比")
    broker_name = models.CharField(max_length=100, default=None, verbose_name="券商名稱")
    broker_info = models.ForeignKey("BrokerInfoTW", blank=True, null=True, on_delete=models.SET_NULL,
                                    db_constraint=False, verbose_name="券商資訊")
    company_info = models.ForeignKey("CompanyBasicInfoTW", blank=True, null=True, on_delete=models.SET_NULL,
                                     db_constraint=False, verbose_name="公司資訊")


class StockTiiTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="證券名稱")
    fm_buy = models.FloatField(blank=True, null=True, verbose_name="外陸資買進股數(不含外資自營商)")
    fm_sell = models.FloatField(blank=True, null=True, verbose_name="外陸資賣出股數(不含外資自營商)")
    fm_net = models.FloatField(blank=True, null=True, verbose_name="外陸資買賣超股數(不含外資自營商)")
    fd_buy = models.FloatField(blank=True, null=True, verbose_name="外資自營商買進股數")
    fd_sell = models.FloatField(blank=True, null=True, verbose_name="外資自營商賣出股數")
    fd_net = models.FloatField(blank=True, null=True, verbose_name="外資自營商買賣超股數")
    ft_net = models.FloatField(blank=True, null=True, verbose_name="全外資買賣超股數")
    itc_buy = models.FloatField(blank=True, null=True, verbose_name="投信買進股數")
    itc_sell = models.FloatField(blank=True, null=True, verbose_name="投信賣出股數")
    itc_net = models.FloatField(blank=True, null=True, verbose_name="投信買賣超股數")
    dealer_ppt_buy = models.FloatField(blank=True, null=True, verbose_name="自營商買進股數(自行買賣)")
    dealer_ppt_sell = models.FloatField(blank=True, null=True, verbose_name="自營商賣出股數(自行買賣)")
    dealer_ppt_net = models.FloatField(blank=True, null=True, verbose_name="自營商買賣超股數(自行買賣)")
    dealer_hedge_buy = models.FloatField(blank=True, null=True, verbose_name="自營商買進股數(避險")
    dealer_hedge_sell = models.FloatField(blank=True, null=True, verbose_name="自營商賣出股數(避險")
    dealer_hedge_net = models.FloatField(blank=True, null=True, verbose_name="自營商買賣超股數(避險")
    dealer_net = models.FloatField(blank=True, null=True, verbose_name="全自營商買賣超股數")
    tii_net = models.FloatField(blank=True, null=True, verbose_name="三大法人買賣超股數")


class StockTiiMarketReportTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    buy_price = models.FloatField(blank=True, null=True, verbose_name="買進金額")
    sell_price = models.FloatField(blank=True, null=True, verbose_name="賣出金額")
    net = models.FloatField(blank=True, null=True, verbose_name="買賣超金額")
    market = models.CharField(default=None, max_length=100, verbose_name="市場別")


class StockTdccTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    hold_class = models.FloatField(blank=True, null=True, verbose_name="分級代號")
    people = models.FloatField(blank=True, null=True, verbose_name="人數")
    hold_num = models.FloatField(blank=True, null=True, verbose_name="持有股數")
    hold_pt = models.FloatField(blank=True, null=True, verbose_name="分級占比")


class StockMarginTransactionsTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, verbose_name="證券名稱")
    mt_buy = models.FloatField(blank=True, null=True, verbose_name="融資買進")
    mt_sell = models.FloatField(blank=True, null=True, verbose_name='融資賣出')
    cash_redemption = models.FloatField(blank=True, null=True, verbose_name="融資現償")
    mt_balance_pd = models.FloatField(blank=True, null=True, verbose_name="前日融資餘額")
    mt_balance_now = models.FloatField(blank=True, null=True, verbose_name="今日融資餘額")
    mt_quota = models.FloatField(blank=True, null=True, verbose_name="融資限額")
    short_covering = models.FloatField(blank=True, null=True, verbose_name="融券回補")
    short_sale = models.FloatField(blank=True, null=True, verbose_name="融券賣出")
    stock_redemption = models.FloatField(blank=True, null=True, verbose_name="融券現償")
    ss_balance_pd = models.FloatField(blank=True, null=True, verbose_name="前日融券餘額")
    ss_balance_now = models.FloatField(blank=True, null=True, verbose_name="今日融券餘額")
    ss_quota = models.FloatField(blank=True, null=True, verbose_name="券限額")
    offset = models.FloatField(blank=True, null=True, verbose_name="資券相抵")
    note = models.CharField(blank=True, null=True, max_length=500, verbose_name="官方註記")
    mt_use_rate = models.FloatField(blank=True, null=True, verbose_name="融資使用率")
    ss_use_rate = models.FloatField(blank=True, null=True, verbose_name="融券使用率")


class Stock3PRatioTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="證券名稱")
    dividend_yield = models.FloatField(blank=True, null=True, verbose_name="殖利率")
    pe = models.FloatField(blank=True, null=True, verbose_name="本益比")
    pb = models.FloatField(blank=True, null=True, verbose_name="本淨比")


class CommodityTaifex(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="商品代號")
    spot_id = models.CharField(max_length=100, null=True, verbose_name="現貨代號")
    stock_name = models.CharField(max_length=100, verbose_name="商品名稱")
    check_fc = models.BooleanField(default=False, verbose_name="期貨商品")
    check_opt = models.BooleanField(default=False, verbose_name="選擇權商品")
    check_sii = models.BooleanField(default=False, verbose_name="上市商品")
    check_otc = models.BooleanField(default=False, verbose_name="上櫃商品")
    check_etf = models.BooleanField(default=False, verbose_name="ETF商品")
    spot_unit = models.FloatField(blank=True, null=True, verbose_name="契約現貨單位")
    company_info = models.ForeignKey("CompanyBasicInfoTW", blank=True, null=True, on_delete=models.SET_NULL,
                                     db_constraint=False, verbose_name="公司資訊")


class FuturePriceTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="商品代號")
    date = models.DateField(verbose_name="資料日期")
    contract_date = models.CharField(max_length=100, null=True, verbose_name="契約日期")
    open = models.FloatField(blank=True, null=True, verbose_name="開盤價")
    high = models.FloatField(blank=True, null=True, verbose_name="最高價")
    low = models.FloatField(blank=True, null=True, verbose_name="最低價")
    close = models.FloatField(blank=True, null=True, verbose_name="收盤價")
    quote_change = models.FloatField(blank=True, null=True, verbose_name="漲跌幅")
    turnover_vol = models.FloatField(blank=True, null=True, verbose_name="成交量")
    settlement_price = models.FloatField(blank=True, null=True, verbose_name="結算價")
    open_interest = models.FloatField(blank=True, null=True, verbose_name="未沖銷契約數")
    best_bid = models.FloatField(blank=True, null=True, verbose_name="最後最佳買價")
    best_ask = models.FloatField(blank=True, null=True, verbose_name="最後最佳賣價")
    trading_halt = models.CharField(max_length=100, null=True, verbose_name="暫停交易")
    trading_session = models.CharField(max_length=100, null=True, verbose_name="交易時段")
    cross_contract_vol = models.FloatField(blank=True, null=True, verbose_name="價差對單式委託成交量")
    commodity_info = models.ForeignKey("CommodityTaifex", blank=True, null=True, on_delete=models.SET_NULL,
                                       db_constraint=False, verbose_name="公司資訊")


class StockInsiderHoldTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="證券名稱")
    issued_num = models.FloatField(blank=True, null=True, verbose_name="發行股數")
    director_add = models.FloatField(blank=True, null=True, verbose_name="董監增持")
    director_lower = models.FloatField(blank=True, null=True, verbose_name="董監減持")
    director_hold = models.FloatField(blank=True, null=True, verbose_name="董監持股")
    director_hold_ratio = models.FloatField(blank=True, null=True, verbose_name="董監持股比率")
    manager_hold = models.FloatField(blank=True, null=True, verbose_name="經理人持股")
    big10_hold = models.FloatField(blank=True, null=True, verbose_name="持有10趴以上持有人持股")


class StockInsiderHoldDetailTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateField(verbose_name="資料日期")
    title = models.CharField(max_length=100, null=True, verbose_name="職稱")
    name = models.CharField(max_length=100, null=True, verbose_name="姓名")
    act_hold = models.FloatField(blank=True, null=True, verbose_name="選任時持股")
    hold = models.FloatField(blank=True, null=True, verbose_name="目前持股")
    pledge = models.FloatField(blank=True, null=True, verbose_name="質押股數")
    pledge_ratio = models.FloatField(blank=True, null=True, verbose_name="質押比例")
    family_hold = models.FloatField(blank=True, null=True, verbose_name="配偶或未成年子女持股")
    family_pledge = models.FloatField(blank=True, null=True, verbose_name="配偶或未成年子女質押股數")
    family_pledge_ratio = models.FloatField(blank=True, null=True, verbose_name="配偶或未成年子女質押比例")
