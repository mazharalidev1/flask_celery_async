from django.db import models


class TestItem(models.Model):
    name = models.CharField(max_length=100)