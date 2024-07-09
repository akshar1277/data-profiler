from django.contrib import admin
from .models import *
# Register your models here.
admin.site.register(Service)
admin.site.register(Mysql)
admin.site.register(Mssql)
admin.site.register(Postgres)
admin.site.register(Pipeline)
admin.site.register(TableMetaData)
admin.site.register(TableRelation)
admin.site.register(Metrics)
admin.site.register(Analysis)
# admin.site.register(Connecter)