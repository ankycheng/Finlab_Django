from django.contrib import admin
from django.urls import path, include
from tdcc_tw import urls as tdcc_tw_urls
from rest_framework import routers

routeLists = [
    tdcc_tw_urls.routeList,
]

# router reference
# ref: https://www.django-rest-framework.org/api-guide/routers/
router = routers.DefaultRouter()

for routeList in routeLists:
    for route in routeList:
        router.register(route[0], route[1], basename=route[2])

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include((router.urls, 'api'), namespace='api')),
]
