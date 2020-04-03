from django.db import models
from django.contrib.auth.models import User
from django.db.models import Sum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import math


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
