from django.contrib import admin
from django.urls import path
from fastapi import APIRouter
from tdcc_tw.urls import routeList as tdcc_urls

router = APIRouter()
routeLists = [
    tdcc_urls,
]
for routeList in routeLists:
    for route in routeList:
        getattr(router, route[0])(route[1], tags=[route[2]])(route[3])

urlpatterns = [
    path('admin/', admin.site.urls),
]
