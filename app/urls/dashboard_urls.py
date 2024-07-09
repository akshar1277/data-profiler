from django.urls import path
from app.views import dashboard_views as views

urlpatterns = [
    path('', views.dashboard_view,name='dashboard'),
    path('delete/<int:id>/', views.delete_service_view,name='delete'),
    path('<int:id>/', views.service_tables_view,name='service_tables'),
    path('profiler/<int:id>/', views.profiler_view,name='profiler'),
    path('<int:id>/<str:col_name>/', views.column_detail_view,name='column_detail'),
    path('analysis/<int:id>/', views.analysis_view,name='analysis'),
    path('search/', views.search_view,name='search'),
]