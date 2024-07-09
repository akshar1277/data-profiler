from django.shortcuts import render,redirect
from django.views.decorators.http import require_http_methods
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login,logout
from django.contrib.auth.decorators import login_required
from app.models import *
import pymysql
from app.reusable_connector import MySQLConnector,PostgreSQLConnector


def get_form_feilds(connecter):
    if connecter =='mysql':
        form=[field.name for field in Mysql._meta.fields if field.name not in ['id','service']]
    if connecter =='postgres':
        form=[field.name for field in Postgres._meta.fields if field.name not in ['id','service']]
    if connecter =='mssql':
        form=[field.name for field in Mssql._meta.fields if field.name not in ['id','service']]
    return form

@login_required
@require_http_methods(["GET","POST"])
def service_view(request):
    if request.method=='POST':
        user=request.user
        service_name=request.POST.get('service','')
        conn_name=request.POST.get('connecter','')
        pipeline_name=service_name+"_dag"
        if Service.objects.filter(service_name=service_name).exists():
            return render(request,'service/service.html',{'message':"{} name service already exists.".format(service_name)})
        service=Service(service_name=service_name,conn_name=conn_name,pipeline_name=pipeline_name,user=user)
        service.save()
        return redirect('connecter',id=service.pk)
    return render(request,'service/service.html',{'message':"enter unique service name."})

@login_required
@require_http_methods(["GET","POST"])
def connecter_view(request,id):
    service = Service.objects.get(pk=id)
    conn_name=service.conn_name

    if(request.user==service.user):
        if request.method=='POST':
            try:
                if conn_name=='mysql':
                    user=request.POST.get('user','')
                    password=request.POST.get('password','')
                    host=request.POST.get('host','')
                    port=request.POST.get('port','')
                    database_name=request.POST.get('database_name','')
                    mysql, created = Mysql.objects.update_or_create(
                    service=service,
                    defaults={
                        'user': user,
                        'password': password,
                        'host': host,
                        'port':port,
                        'database_name': database_name
                    }
                    )
                    print
                    connecter=Mysql.objects.get(service=service)
                    mysql_connector=MySQLConnector(host=connecter.host,
                    port=connecter.port,
                    user=connecter.user,
                    password=connecter.password,
                    database=connecter.database_name)
                    mysql_connector.connect()
                    tables = mysql_connector.execute_query("SHOW TABLES")
                    if tables is not None:
                        table_names = [table[0] for table in tables]
                        print(table_names)
                        for table_name in table_names:
                            TableMetaData.objects.create(
                            table_name=table_name,
                            service_id=service.id,)
        
                    else:
                        raise Exception('No Tables Are available.')
                    mysql_connector.disconnect()

                if conn_name=='postgres':
                    user=request.POST.get('user','')
                    password=request.POST.get('password','')
                    host=request.POST.get('host','')
                    port=request.POST.get('port','')
                    database_name=request.POST.get('database_name','')
                    postgres, created = Postgres.objects.update_or_create(
                    service=service,
                    defaults={
                        'user': user,
                        'password': password,
                        'host': host,
                        'port':port,
                        'database_name': database_name
                    }
                    )
                    connecter=Postgres.objects.get(service=service)
                    postgres_connector=PostgreSQLConnector(host=connecter.host,
                    port=connecter.port,
                    user=connecter.user,
                    password=connecter.password,
                    database=connecter.database_name)
                    postgres_connector.connect()
                    tables = postgres_connector.execute_query("""SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE';""")
                    if tables is not None:
                        table_names = [table[0] for table in tables]
                        for table_name in table_names:
                            print("tables",table_name)
                    else:
                        raise Exception('No Tables Are available.')
                    postgres_connector.disconnect()
             
                # if connecter=='mssql':
                #     user=request.POST.get('user','')
                #     password=request.POST.get('password','')
                #     host=request.POST.get('host','')
                #     database_name=request.POST.get('database_name','')
                #     update_count=Mssql.objects.filter(service=service).update(user=user,password=password,host=host,database_name=database_name)
                #     if update_count==0:
                #         mssql=Mssql(user=user,password=password,host=host,database_name=database_name,service=service)
                #         mssql.save()
            except Exception as e:
                form=get_form_feilds(conn_name)
                return render(request,'service/connecter.html',{'form':form,'id':service.id,'message':e})
            # return redirect('pipeline',id=service.pk)
            return redirect('tables_relation',id=service.pk)
        if request.method=='GET':
            form=get_form_feilds(conn_name)
            return render(request,'service/connecter.html',{'form':form ,'id':service.pk})
    return redirect('service')


@login_required
@require_http_methods(["GET","POST"])
def tables_relation_view(request,id):
    service=Service.objects.get(pk=id)
    if(request.user==service.user):
        if request.method=='POST':
            table1=request.POST.get('table1','')
            table2=request.POST.get('table2','')
            relation=request.POST.get('relation','')
            print(table2,table1,relation)
            table_one=TableMetaData.objects.get(service=service,table_name=table1)
            table_two=TableMetaData.objects.get(service=service,table_name=table2)
            if table_one==table_two:
                tables_metadata=TableMetaData.objects.filter(service=service).values_list('table_name',flat=True)
                return render(request,'service/tables_relation.html',{'tables':tables_metadata,'id':service.pk,'message':'Tables name are same.'})
            TableRelation.objects.create(table_one=table_one,table_two=table_two,relation=relation,service=service) 
            if request.POST.get('action','')=='save_and_add':
                return redirect('tables_relation',id=service.pk)
            if request.POST.get('action','')=='save':
                return redirect('pipeline',id=service.id)
        if request.method=='GET':
            tables_metadata=TableMetaData.objects.filter(service=service).values_list('table_name',flat=True)
            return render(request,'service/tables_relation.html',{'tables':tables_metadata,'id':service.pk})
    return render(request,'unauthorized_access.html',{'message':'You do not have authorization to access this resource.'})


# @login_required
# @require_http_methods(["GET","POST"])
# def test_connecter_view(request,id):
#     service=Service.objects.get(pk=id)
#     if(request.user==service.user):
#         if request.method=='POST':
#             try:
#                 if service.conn_name=='mysql':
#                     connecter=Mysql.objects.get(service=service)
#                     mysql_connector=MySQLConnector(host=connecter.host,
#                     port=connecter.port,
#                     user=connecter.user,
#                     password=connecter.password,
#                     database=connecter.database_name)
#                     mysql_connector.connect()
#                     tables = mysql_connector.execute_query("SHOW TABLES")
#                     table_names = [table[0] for table in tables]
#                     for table_name in table_names:
#                         print(table_name)
#                     mysql_connector.disconnect()

#                 if service.conn_name=='postgres':
#                     connecter=Postgres.objects.get(service=service)
#                     mysql_connection = pymysql.connect(
#                     host=connecter.host,
#                     # port=connecter.port
#                     user=connecter.user,
#                     password=connecter.password,
#                     database=connecter.database_name)
#                     mysql_cursor = mysql_connection.cursor()
#                     mysql_cursor.execute('SHOW TABLES')
#                     tables = mysql_cursor.fetchall()
#                     table_names = [table[0] for table in tables]
#                     for table_name in table_names:
#                         print(table_name)
#                 if service.conn_name=='mssql':
#                     connecter=Mssql.objects.get(service=service)
#                     mysql_connection = pymysql.connect(
#                     host=connecter.host,
#                     user=connecter.user,
#                     password=connecter.password,
#                     database=connecter.database_name)
#                     mysql_cursor = mysql_connection.cursor()
#                     mysql_cursor.execute('SHOW TABLES')
#                     tables = mysql_cursor.fetchall()
#                     table_names = [table[0] for table in tables]
#                     for table_name in table_names:
#                         print(table_name)
#             except Exception as e:
#                 form=get_form_feilds(service.conn_name)
#                 print(e)
#                 return render(request,'service/connecter.html',{'form':form,'id':service.id,'message':"Not Connecting to database please check your configurations."})
#             return redirect('dashboard')
#         if request.method=='GET':
#             return render(request,'service/test-connecter.html',{'id':service.pk})
#     return redirect('service')
