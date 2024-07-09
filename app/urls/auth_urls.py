from django.urls import path
from app.views import auth_views as views

urlpatterns = [
    path('login/', views.login_view,name='login'),
    path('register/', views.register_view,name='register'),
    path('logout/', views.logout_view,name='logout'),
]