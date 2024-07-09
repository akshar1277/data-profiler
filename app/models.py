from django.db import models
from django.contrib.auth.models import User
# Create your models here.

class Service(models.Model):
    service_name=models.CharField(max_length=100,null=False)
    conn_name=models.CharField(max_length=100,null=False)
    pipeline_name=models.CharField(max_length=100,null=False)
    user=models.ForeignKey(User,on_delete=models.CASCADE,null=True)

    def __str__(self):
        return str(self.service_name)


# class Sqlconnector(models.Model):

#     user=models.CharField(max_length=100,null=False)
#     password=models.CharField(max_length=100,null=False)
#     host=models.CharField(max_length=100,null=False)
#     database_name=models.CharField(max_length=100,null=False)
#     service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)


#     def __str__(self):
#         return str(self.user)

class Mysql(models.Model):
    user=models.CharField(max_length=100,null=False)
    password=models.CharField(max_length=100,null=False)
    host=models.CharField(max_length=100,null=False)
    port=models.IntegerField(null=False)
    database_name=models.CharField(max_length=100,null=False)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)
    def __str__(self):
        return str(self.user)

class Postgres(models.Model):
    user=models.CharField(max_length=100,null=False)
    password=models.CharField(max_length=100,null=False)
    host=models.CharField(max_length=100,null=False)
    port=models.IntegerField(null=False)
    database_name=models.CharField(max_length=100,null=False)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)
    def __str__(self):
        return str(self.user)

class Mssql(models.Model):
    user=models.CharField(max_length=100,null=False)
    password=models.CharField(max_length=100,null=False)
    host=models.CharField(max_length=100,null=False)
    port=models.IntegerField(null=False)
    database_name=models.CharField(max_length=100,null=False)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)
    def __str__(self):
        return str(self.user)

        
class Pipeline(models.Model):
    # pipeline_id=models.AutoField(primary_key=True,editable=False)
    pipeline_name=models.CharField(max_length=100,null=False)
    ingestion_status=models.BooleanField(default=False)
    cron_exp= models.CharField(max_length=100)
    profiler_status=models.BooleanField(default=False)
    cron_exp=models.CharField(max_length=100,null=False)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)

    def __str__(self):
        return str(self.pipeline_name)

class TableMetaData(models.Model):
    # table_id=models.AutoField(primary_key=True,editable=False)
    row_count=models.IntegerField(null=False,default=0)
    col_count=models.IntegerField(null=False,default=0)
    table_name=models.CharField(max_length=100,null=False)
    duplicates=models.IntegerField(null=False,default=0)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)

    def __str__(self):
        return str(self.table_name)

class TableRelation(models.Model):
    table_one=models.ForeignKey(TableMetaData,on_delete=models.CASCADE,null=True,related_name='table_one_relations')
    table_two=models.ForeignKey(TableMetaData,on_delete=models.CASCADE,null=True,related_name='table_two_relations')
    relation=models.CharField(null=False)
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)
    def __str__(self):
        return f"{self.table_one} - {self.relation} - {self.table_two}"

class Metrics(models.Model):
    # metrics_id=models.AutoField(primary_key=True,editable=False)
    col_name=models.CharField(max_length=100,null=False)
    col_type=models.CharField(max_length=100,null=False)
    is_numeric=models.BooleanField(default=False)
    null_values=models.IntegerField(null=False)
    unique=models.IntegerField(null=False)
    maximum=models.IntegerField(null=True)
    minimum=models.IntegerField(null=True)
    mean=models.IntegerField(null=True)
    # median=models.IntegerField(null=True)
    standard_deviation=models.IntegerField(null=True)
    histogram_data=models.TextField(null=True)
    bar_plot_data=models.TextField(null=True)
    table=models.ForeignKey(TableMetaData,on_delete=models.CASCADE,null=True)

    def __str__(self):
        return str(self.col_name)

class Analysis(models.Model):
    # table_id=models.AutoField(primary_key=True,editable=False)
    table=models.ForeignKey(TableMetaData,on_delete=models.CASCADE,null=True)
    correlation=models.JSONField()
    outliers=models.JSONField()
    service=models.ForeignKey(Service,on_delete=models.CASCADE,null=True)

    def __str__(self):
        return str(self.correlation)


