from django.contrib import admin
from django.urls import path, include
from fastapi import APIRouter
from tdcc_tw.urls import routeList as tdcc_urls

router = APIRouter()
routeLists = [
    tdcc_urls,
]
for routeList in routeLists:
    for route in routeList:
        router.get(route[0])(route[1])

urlpatterns = [
    path('admin/', admin.site.urls),
]
