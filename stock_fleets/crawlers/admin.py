from django.contrib import admin
from .models import CompanyBasicInfoTW
import datetime

"""
後台資料庫管理
"""


@admin.register(CompanyBasicInfoTW)
class CompanyBasicInfoTWAdmin(admin.ModelAdmin):
    list_display = ['stock_id', 'short_name', 'market', 'stock_issued_num', 'category', 'capital', 'private_shares',
                    'special_shares', 'establishment_date', 'update_time']
    ordering = ['stock_id']
    search_fields = ['stock_id', 'short_name', 'category']
