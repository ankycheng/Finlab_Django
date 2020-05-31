import os
from django.core.wsgi import get_wsgi_application
from fastapi import FastAPI

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stock_fleets.settings.development")
application = get_wsgi_application()

from stock_fleets.urls import router as main_router

app = FastAPI()
app.include_router(main_router, prefix="/api")
