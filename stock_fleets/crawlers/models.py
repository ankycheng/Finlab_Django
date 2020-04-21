from django.db import models


class StockPriceTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateTimeField(verbose_name="資料日期")
    stock_name = models.CharField(max_length=100, null=True, verbose_name="證券名稱")
    open_price = models.FloatField(blank=True, null=True, verbose_name="開盤價")
    close_price = models.FloatField(blank=True, null=True, verbose_name="收盤價")
    high_price = models.FloatField(blank=True, null=True, verbose_name="最高價")
    low_price = models.FloatField(blank=True, null=True, verbose_name="最低價")
    turnover_vol = models.FloatField(blank=True, null=True, verbose_name="成交股數")
    turnover_price = models.FloatField(blank=True, null=True, verbose_name="成交金額")


class MonthlyRevenueTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateTimeField(verbose_name="資料日期")
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
    update_time = models.DateTimeField(verbose_name="資料日期")
    name = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司名稱")
    short_name = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司簡稱")
    category = models.CharField(blank=True, null=True, max_length=100, verbose_name="產業類別")
    registered_country = models.CharField(blank=True, null=True, max_length=100, verbose_name="企業註冊地國")
    address = models.CharField(blank=True, null=True, max_length=100, verbose_name="公司總部地址")
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
    dividend_frequency = models.CharField(blank=True, null=True, max_length=500,
                                          verbose_name="普通股盈餘分派或虧損撥補頻率")
    stock_transfer_institution = models.CharField(blank=True, null=True, max_length=500, verbose_name="股票過戶機構")
    visa_accounting_firm = models.CharField(blank=True, null=True, max_length=500, verbose_name="簽證會計師事務所")
    website = models.CharField(blank=True, null=True, max_length=500, verbose_name="公司網址")
    investor_relations_contact = models.CharField(blank=True, null=True, max_length=500,
                                                  verbose_name="投資人關係聯絡電話")
    investor_relations_email = models.CharField(blank=True, null=True, max_length=500,
                                                verbose_name="投資人關係聯絡電子郵件")
    english_abbreviation = models.CharField(blank=True, null=True, max_length=500, verbose_name="英文簡稱")

    def __str__(self):
        return self.stock_id + ' ' + str(self.short_name)

    @property
    def stock_issued_num(self):
        return round(self.capital / 10000)

    @property
    def market(self):
        if self.sii_date is not None:
            return '上市'
        elif self.otc_date is not None:
            return '上櫃'
        else:
            return '興櫃'

    # custom fields  short_description
    market.fget.short_description = '市場別'
    stock_issued_num.fget.short_description = '發行股票張數'


class StockIndexPriceTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateTimeField(verbose_name="資料日期")
    index_price = models.FloatField(blank=True, null=True, verbose_name="指數點數")
    quote_change = models.FloatField(blank=True, null=True, verbose_name="漲跌百分比")


class StockIndexVolTW(models.Model):
    stock_id = models.CharField(max_length=100, verbose_name="證券代號")
    date = models.DateTimeField(verbose_name="資料日期")
    turnover_vol = models.FloatField(blank=True, null=True, verbose_name="成交股數")
    turnover_price = models.FloatField(blank=True, null=True, verbose_name="成交金額")
    turnover_num = models.BigIntegerField(blank=True, null=True, verbose_name="成交筆數")
