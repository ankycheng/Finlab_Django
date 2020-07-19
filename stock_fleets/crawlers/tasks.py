# python manage.py qcluster, activate Django-Q
from crawlers.finlab.pioneers import *
from crawlers.models import *
from crawlers.finlab.import_tools import *
import logging

logging.basicConfig(level=logging.DEBUG)


def hello(i):
    logging.info('test:', i)


def crawl_stock_price_tw():
    crawler = CrawlerProcess(CrawlStockPriceTW, 'crawl_main', StockPriceTW, 'date_range')
    logging.info(crawler)
    logging.info(crawler.auto_update_crawl())


def crawl_monthly_revenue_tw():
    crawler = CrawlerProcess(CrawlMonthlyRevnueTW, 'crawl_main', MonthlyRevenueTW, 'month_range')
    logging.info(crawler)
    logging.info(crawler.auto_update_crawl())
