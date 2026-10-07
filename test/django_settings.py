SECRET_KEY = "test-secret-key"

INSTALLED_APPS = [
    "test.django_app",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "/tmp/flask_async_celery_test.sqlite3",
    }
}

USE_TZ = True