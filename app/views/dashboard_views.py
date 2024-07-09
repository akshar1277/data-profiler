from django.shortcuts import render,redirect
from django.views.decorators.http import require_http_methods
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login,logout
from django.contrib.auth.decorators import login_required
from app.models import *
import pymysql
from django.db.models import Q
from app.reusable_connector import MySQLConnector
import matplotlib.pyplot as plt
import io
import urllib, base64
import pandas as pd
from django.db import connection
# Create your views here.

@login_required
@require_http_methods(["GET"])
def dashboard_view(request):
    services=Service.objects.filter(user=request.user)

    return render(request,'dashboard/dashboard.html',{'services':services})

@login_required
@require_http_methods(["GET","POST"])
def service_tables_view(request,id):
    service=Service.objects.get(pk=id)
    referential_integrity=0
    if request.user==service.user:
        if request.method=='POST':
            table_relation=request.POST.get('table_relation','')
            string_split=table_relation.split('-')
            print("hello",string_split)
            parent_table=string_split[0].strip()+str(service.id)
            child_table=string_split[2].strip()+str(service.id)
            with connection.cursor() as cursor:
                # cursor.execute("SELECT * FROM " + string_split[0])   
                # table1_data =  [row[0] for row in cursor.fetchall()]
                # cursor.execute("SELECT * FROM " + string_split[2])   
                # table2_data =  [row[0] for row in cursor.fetchall()]
                if string_split[1].strip() =='one_to_many':
                    print(string_split[1])
                    cursor.execute("SELECT * FROM " + child_table + " WHERE id NOT IN (SELECT id FROM " + parent_table + ")")
                    referential_integrity =  [row[0] for row in cursor.fetchall()]
                    print("ref",referential_integrity)
                    # cursor.execute("SELECT * FROM " + child_table + " WHERE NOT EXISTS (SELECT 1 FROM " + parent_table + " WHERE " +  parent_table.id + " = ChildTable.parent_id);")
                    # broken_realtionship =  [row[0] for row in cursor.fetchall()]
                    # print("ref",referential_integrity)
            table_relations=TableRelation.objects.filter(service=service)
            print(service.service_name)
            service_tables=TableMetaData.objects.filter(service=service)
            return render(request,'dashboard/service_tables.html',{'tables':service_tables,'table_relations':table_relations,'referential_integrity':referential_integrity})

            # return redirect('service_tables',{'id':id,'referential_integrity':referential_integrity})
        if request.method=='GET':
            table_relations=TableRelation.objects.filter(service=service)
            print(service.service_name)
            service_tables=TableMetaData.objects.filter(service=service)
            table_name=[table.table_name for table in service_tables]
            # print("table_relation",table_relations)
            return render(request,'dashboard/service_tables.html',{'tables':service_tables,'table_relations':table_relations})
    return redirect('dashboard')

@login_required
@require_http_methods(["GET"])
def profiler_view(request,id):
    service_table=TableMetaData.objects.get(pk=id)
    columns=Metrics.objects.filter(table=service_table)
    print(service_table)
    return render(request,'dashboard/profiler.html',{'columns':columns,'id':id})

@login_required
@require_http_methods(["GET"])
def column_detail_view(request,id,col_name):
    service_table=TableMetaData.objects.get(pk=id)
    # columns=Metrics.objects.filter(table=service_table)
    table_name=service_table.table_name+str(service_table.service.pk)
    with connection.cursor() as cursor:
        cursor.execute("SELECT " + col_name + " FROM " + table_name)     
        # cursor.execute("select * from " + table_name)   
        col_data =  [row[0] for row in cursor.fetchall()]
    return render(request,'dashboard/column_detail.html',{'col_data':col_data,'col_name':col_name})

@login_required
@require_http_methods(["GET"])
def analysis_view(request,id):
    # service=Service.objects.get(pk=id)
    table=TableMetaData.objects.get(pk=id)
    service=table.service
    correlation_matrices=dict()
    # for table in tables:
    table_name=table.table_name+str(service.id)
    columns=table.metrics_set.filter(is_numeric=True)
    numeric_columns = [column.col_name for column in columns]
    print("numeric",numeric_columns)
   
    # for column in columns:
    #     if column.is_numeric:
    #             numeric_col.append(column.col_name)
    #     numeric_str = ', '.join(numeric_col)
    with connection.cursor() as cursor:
        cursor.execute("SELECT " + ", ".join(numeric_columns) + " FROM " + table_name)     
        # cursor.execute("select * from " + table_name)   
        df_data = cursor.fetchall()
        df=pd.DataFrame(df_data, columns=numeric_columns)
        print("df",df)
        correlation=df.corr()
        print(correlation)
        # correlation_matrices[table.table_name]=correlation
        # print(correlation)

        for column in numeric_columns:
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)][column]
            outliers=outliers.to_json()
            print(f"**Lower Bound:** {lower_bound}, **Upper Bound:** {upper_bound}")
            print(f"**Number of Outliers:** {len(outliers)}")
            print(outliers)
        # analysis=Analysis.objects.filter(service=service)
        print(table.table_name)
    # print(correlation_matrices)
    return render(request,'dashboard/analysis.html',{'correlation':correlation,'column_name':numeric_columns,'table_data':df.to_json(),'table_name':table.table_name.upper})

@login_required
@require_http_methods(["GET"])
def search_view(request):
    query=request.GET.get('query')
    services=Service.objects.filter(user=request.user).filter(Q(service_name__icontains=query)|Q(conn_name__icontains=query))
    return render(request,'dashboard/dashboard.html',{'services':services})

@login_required
@require_http_methods(["GET"]) #delete
def delete_service_view(request,id):
    #show old dataset
    service=Service.objects.get(pk=id)
    service.delete()
    return redirect('dashboard')


