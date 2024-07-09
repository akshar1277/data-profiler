from django.shortcuts import render,redirect
from django.views.decorators.http import require_http_methods
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login,logout
from django.contrib.auth.decorators import login_required
from app.models import *
import pymysql
from app.reusable_connector import MySQLConnector

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
            return render(request,'auth/login.html',{'message':'User does not exists'})
    if request.method=='GET':
        return render(request,'auth/login.html')

@require_http_methods(["GET", "POST"])
def register_view(request):
    if request.method=='POST':
        username=request.POST.get('username','')
        email=request.POST.get('email','')
        password=request.POST.get('password','')
        confirm_password=request.POST.get('confirm_password','')
        if not username or not password:
            return render(request,'auth/register.html',{'message':'Username and Password is required'})
        if User.objects.filter(username=username).exists():
            return render(request,'auth/register.html',{'message':'Username already exists'})
        if password!=confirm_password:
            return render(request,'auth/register.html',{'message':'Password do not match.'})
        user = User.objects.create_user(username=username,email=email, password=password)
        return redirect('login')
    if request.method=='GET':
        return render(request,'auth/register.html')


def logout_view(request):
    logout(request)
    return redirect('login')
