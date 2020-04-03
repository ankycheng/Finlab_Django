# Finlab_Django

## 專案目的
1. 整合Hahow Finlab課程所學內容，以網頁化系統性編排
2. 部署財經資料自動化爬蟲
3. 選股視覺化工具
4. 策略與模型選股呈現
5. 資產管理工具
6. 雲端計算回測工具

## 主要框架與套件
1. Django
2. Pandas
3. MySQL

## 專案架構
1. crawlers app(爬蟲相關)
* finlab dir(工具集):
** pioneers:爬蟲部隊
** import_tools:爬蟲資料匯入工具-CrawlerProcess物件選擇抓取時間設定

2. config(環境設定)
3. requirements.txt(安裝套件清單)
`pip install -r requirements.txt`
