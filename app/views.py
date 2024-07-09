from django.shortcuts import render,redirect
from django.views.decorators.http import require_http_methods
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login,logout
from django.contrib.auth.decorators import login_required
from .models import *
import pymysql
from .reusable_connector import MySQLConnector

# Create your views here.


@require_http_methods(["GET", "POST"])
def login_view(request):
    if request.method=='POST':
        username=request.POST.get('username','')
        password=request.POST.get('password','')
        user = authenticate(request,username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('dashboard')
        else:
            return render(request,'login.html',{'messages':'User does not exists'})
    if request.method=='GET':
        return render(request,'login.html')

@require_http_methods(["GET", "POST"])
def register_view(request):
    if request.method=='POST':
        username=request.POST.get('username','')
        email=request.POST.get('email','')
        password=request.POST.get('password','')
        if not username or not password:
            return render(request,'register.html',{'messages':'Username and Password is required'})
        if User.objects.filter(username=username).exists():
            return render(request,'register.html',{'messages':'Username already exists'})
        user = User.objects.create_user(username=username,email=email, password=password)
        return redirect('login')
    if request.method=='GET':
        return render(request,'register.html')

@login_required
@require_http_methods(["GET"])
def dashboard_view(request):
    #show old dataset
    return render(request,'dashboard.html')
    
@login_required
@require_http_methods(["GET","POST"])
def service_view(request):
    if request.method=='POST':
        user=request.user
        service_name=request.POST.get('service','')
        conn_name=request.POST.get('connecter','')
        service=Service(service_name=service_name,conn_name=conn_name,user=user)
        service.save()
        print(service)
        return redirect('connecter',id=service.pk)
    return render(request,'service.html')

@login_required
@require_http_methods(["GET","POST"])
def connecter_view(request,id):
    service = Service.objects.get(pk=id)
    print(service.user.id,request.user.id)
    if(request.user==service.user):
        if request.method=='POST':
            connecter=service.conn_name
            print(connecter)
            if connecter=='mysql':
                user=request.POST.get('user','')
                password=request.POST.get('password','')
                host=request.POST.get('host','')
                database_name=request.POST.get('database_name','')
                mysql=Mysql(user=user,password=password,host=host,database_name=database_name,service=service)
                mysql.save()
                # id=mysql.pk
            if connecter=='postgres':
                user=request.POST.get('user','')
                password=request.POST.get('password','')
                host=request.POST.get('host','')
                database_name=request.POST.get('database_name','')
                postgres=Postgres(user=user,password=password,host=host,database_name=database_name,service=service)
                postgres.save()
                # id=postgres.pk
            if connecter=='mssql':
                user=request.POST.get('user','')
                password=request.POST.get('password','')
                host=request.POST.get('host','')
                database_name=request.POST.get('database_name','')
                mssql=Mssql(user=user,password=password,host=host,database_name=database_name,service=service)
                mssql.save()
                # id=mssql.pk
            return redirect('test_connecter',id=service.pk)
        if request.method=='GET':
            connecter=service.conn_name
            if connecter =='mysql':
                form=[field.name for field in Mysql._meta.fields if field.name not in ['id','service']]
            if connecter =='postgres':
                form=[field.name for field in Postgres._meta.fields if field.name not in ['id','service']]
            if connecter =='mssql':
                form=[field.name for field in Mssql._meta.fields if field.name not in ['id','service']]
            # if connecter=='snowfleck':
            #     for
            return render(request,'connecter.html',{'form':form,'id':service.pk})
    return redirect('service')

@login_required
@require_http_methods(["GET","POST"])
def test_connecter_view(request,id):
    service=Service.objects.get(pk=id)
    if(request.user==service.user):
        if request.method=='POST':
            if service.conn_name=='mysql':
                connecter=Mysql.objects.get(service=service)
                mysql_connector=MySQLConnector(host=connecter.host,
                user=connecter.user,
                password=connecter.password,
                database=connecter.database_name)
                mysql_connector.connect()
                tables = mysql_connector.execute_query("SHOW TABLES")
                table_names = [table[0] for table in tables]
                for table_name in table_names:
                    print(table_name)
                mysql_connector.disconnect()

            if service.conn_name=='postgres':
                connecter=Postgres.objects.get(service=service)
                mysql_connection = pymysql.connect(
                host=connecter.host,
                user=connecter.user,
                password=connecter.password,
                database=connecter.database_name)
                mysql_cursor = mysql_connection.cursor()
                mysql_cursor.execute('SHOW TABLES')
                tables = mysql_cursor.fetchall()
                table_names = [table[0] for table in tables]
                for table_name in table_names:
                    print(table_name)
            if service.conn_name=='mssql':
                connecter=Mssql.objects.get(service=service)
                mysql_connection = pymysql.connect(
                host=connecter.host,
                user=connecter.user,
                password=connecter.password,
                database=connecter.database_name)
                mysql_cursor = mysql_connection.cursor()
                mysql_cursor.execute('SHOW TABLES')
                tables = mysql_cursor.fetchall()
                table_names = [table[0] for table in tables]
                for table_name in table_names:
                    print(table_name)
        if request.method=='GET':
            return render(request,'test-connecter.html',{'id':service.pk})
    return redirect('service')

def logout_view(request):
    logout(request)
    return redirect('login')
