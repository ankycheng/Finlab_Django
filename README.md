# Finlab_Django

## 專案目的
1. 整合Hahow Finlab課程所學內容，以Django為中心框架，架構系統
2. 模組化爬蟲程式
3. 選股視覺化工具
4. 策略與模型選股呈現
5. 資產管理工具
6. 雲端計算回測工具
7. 資料整合API

## 主要框架與套件
![](https://i.imgur.com/6cute0O.jpg)




1. Django 3.0:網頁後端
    * 官方文件連結:
    https://docs.djangoproject.com/en/3.0/
    - **fastapi**：取代django restful framework處理API，更簡潔高效，支援異步，使用mount功能將django-admin納入unicorn server-https://fastapi.tiangolo.com/
    - **django-extensions**：在Django環境內使用Jupyter編輯與測試程式，方便與課程內容接軌
    - **django-import-export**：後台資料匯出匯入
    - **django-Q**：排程管理，使用django-admin設定tasks
3. Pandas:爬蟲、資料計算
4. MySQL:資料庫
    - **sqlalchemy**
    - **PyMySQL==0.9.3**
    需要手動解決 Django的 MySql 問題，參考 https://blog.csdn.net/weixin_33127753/article/details/89100552
    
```
    問題解法： 
    1.brew install mysql
    2. 在以下路徑venv/lib/python3.6/site-packages/django/db/backends/mysql/base.py
    把下列程式區塊註解
    
    if version < (1, 3, 13):
   raise ImproperlyConfigured(
       'mysqlclient 1.3.13 or newer is required; you have %s.'
       % Database.__version__
   )
```
   
   
4. Swagger:API文件編輯，Fastapi內建測試文檔
5. Git:版本控制
6. IDE:Pycharm or VScode
7. 前後端分離開發

## 安裝流程

1. git clone or folk
` https://github.com/benbilly3/Finlab_Django.git`
3. cd 到 Finlab 目錄，創造虛擬環境並啟動。
```
virtualenv venv
source venv/bin/activate
brew install mysql
```
3. 安裝套件module目錄
`pip install -r requirements.txt`
4. config.jason檔設定資料庫連線資訊，路徑放在Finlab-Django/stock_fleets/
## 開發環境
1. develope:在manage.py中預設的runserver環境
2. production:上線環境，連結到正式資料庫，下以下指令進入環境
```
python manage.py migrate --settings=stock_fleets.settings.production
```
## 常用Django指令
```
# 啟動網頁伺服器
python manage.py runserver 

# 開啟Jupyter
python manage.py shell_plus --notebook 

# 建立django project
django-admin.py startproject project-name

# 建立django app,依功能建立
python manage.py startapp fbCrawler

# 建立管理者
python manage.py createsuperuser

# 建立makemigrations檔,結尾接app名稱
python manage.py makemigrations xxx

# 執行資料庫migrate
python manage.py migrate

#啟用python編輯器
python manage.py shell

# 啟用fastapi
uvicorn stock_fleets.wsgi:app --reload

# 啟用django-Q
python manage.py qcluster
```

## 專案架構
1. crawlers app(爬蟲相關)
* finlab dir(工具集):
* pioneers:爬蟲部隊
    - import_tools:爬蟲資料匯入工具
    - -CrawlerProcess物件選擇抓取時間設定

2. config(環境設定)
3. requirements.txt(安裝套件清單)
`pip install -r requirements.txt`

## 台股功能研發目錄
### 1. 基本面:
* 公司資訊彙整:
     - 基本資料
     - 產業鏈資訊
     - 概念股編輯、編彙指數
     - 海外轉投資、海外工廠
     - 大股東名單
* 營收選股:
     - 爆發成長股
     - 穩定成長股
     - 轉機股
     - **每月營收細項變化(觀察高毛利產品占比)**
     - 族群數據統計比較
* 財務指標選股:
     - 獲利率
     - 業外損益占比
     - 存貨、應收週轉觀察
     - 現金流指標
     - 資產安全性指標
     - 同行公司比較
     - 有的沒的大師指標

* 配息相關資訊
     - 高股息選股
* 水餃股特區

### 2. 新聞面:
* 重訊關鍵字查詢API
    - 整合上市櫃資訊
    - NLP文本分析
    - 推播出現特定關鍵字公司
    - 母公司賣股
* 新聞NLP分析
* 法說會期程
* 警示股與注意股
* 興櫃IPO

### 3. 技術面:
* 技術指標線圖
* 群組比較
* 自訂指標選股
* 機器學習模型選股
* 還原權習計算
* 波動模型
* ETF折溢價專區

### 4. 籌碼面:
* 集保餘額選股
    - 長線籌碼趨勢
    - 整體市場籌碼消長
* 分點進出選股
     -  籌碼K線
     -  中小型短線主力股
     -  大戶分點進出明細
     -  地緣券商
* 法人買賣超
* 投信初進場
* 投信作帳股：國內股票基金明細+近期買賣超變化
* 融資主力或散戶股
* 借券與融券異常
    -   外資主力放空
    -   融券回補行情
* 庫藏股
* 董監增持、改選行情
* 可轉債
* 股票期貨

### 5. 大盤面:
* 國際指數
* 台股類股指數
* 整體法人、融資券、借券、集保大戶籌碼變化
* 小台期貨籌碼
* 台指特法、十大籌碼
* 原物料
### 6. 管理工具:
* 爬蟲模組
* 資料提取、選股API
* 視覺化模組
* 績效觀察工具 
* 回測工具
* 資產管理工具
* 後台編輯Admin
* 網站部署
* APP或第三方部署、推播