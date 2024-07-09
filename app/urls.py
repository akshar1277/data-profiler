from django.urls import path
from .views import *

urlpatterns = [
    path('login/', login_view,name='login'),
    path('register/', register_view,name='register'),
    path('dashboard/', dashboard_view,name='dashboard'),
    path('connecter/<int:id>/', connecter_view,name='connecter'),
    path('test-connecter/<int:id>/', test_connecter_view,name='test_connecter'),
    path('service/', service_view,name='service'),
    path('logout/', logout_view,name='logout'),
]