from django.urls import path
from app.views import pipeline_views as views

urlpatterns = [
    path('<int:id>/', views.pipeline_view,name='pipeline'),
    path('', views.pipelines_view,name='pipelines'),
    path('tirgger/<str:dag_id>/', views.pipeline_trigger_view,name='pipeline_trigger'),
    path('pipeline_details/<str:dag_id>/', views.pipeline_details_view,name='piepline_details'),
    path('pipeline_logs/<str:dag_id>/', views.pipeline_logs_view,name='pipeline_logs'),
    # path('logs/<str:dag_id>/', views.piepline_logs_view,name='piepline_logs'),
]