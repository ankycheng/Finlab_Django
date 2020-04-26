from .base import *

# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

with open(os.path.join(BASE_DIR, "config.json"), 'r', encoding='utf8') as file:
    CONFIG_DATA = json5.load(file)
    CONFIG_DATA["PRODUCTION"] = True
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': CONFIG_DATA['DBNAME'],
        'USER': CONFIG_DATA['DBACCOUNT'],
        'PASSWORD': CONFIG_DATA['DBPASSWORD'],
        'HOST': CONFIG_DATA['DBHOST'],
        'PORT': CONFIG_DATA['DBPORT'],
        'OPTIONS': {
            'charset': 'utf8mb4',
            'use_unicode': True,
        }
    }
}
