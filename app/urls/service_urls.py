from django.urls import path
from app.views import service_views as views

urlpatterns = [
    path('connecter/<int:id>/', views.connecter_view,name='connecter'),
    path('tables-relation/<int:id>/', views.tables_relation_view,name='tables_relation'),
    # path('', views.service_view,name='service'),
    path('', views.service_view,name='service'),
]