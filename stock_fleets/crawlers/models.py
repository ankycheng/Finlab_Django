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
