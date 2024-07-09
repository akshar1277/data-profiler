from django.shortcuts import render,redirect
import requests
from django.views.decorators.http import require_http_methods
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login,logout
from django.contrib.auth.decorators import login_required
from app.models import *
import pymysql
import os
import pytz
import json
from app.reusable_connector import MySQLConnector
from datetime import datetime
# import sys
# sys.path.append('//to/external_directory')
# \\wsl.localhost\Ubuntu\home\akshali\data_profiler\airflow\dags\ingestion_pipeline.py
def create_connection_url(service):
    if service.conn_name == 'mysql':
        mysql_conn=Mysql.objects.get(service=service)
        connection_url = f'mysql+pymysql://{mysql_conn.user}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.database_name}'
    elif service.conn_name == 'postgres':
        postgres_conn=Postgres.objects.get(service=service)
        connection_url = f'postgresql://{postgres_conn.user}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.database_name}'
    elif service.conn_name == 'mssql':
        mssql_conn=Mssql.objects.get(service=service)
        connection_url = f'mssql+pyodbc://{mssql_conn.user}:{mssql_conn.password}@{mssql_conn.host}:{mssql_conn.port}/{mssql_conn.database_name}?driver=ODBC+Driver+17+for+SQL+Server'
    # elif service.conn_name == 'snowflake':
    #     connection_url = f'snowflake://{username}:{password}@{host}:{port}/{database_name}'
    return connection_url

def create_dag_for_service(service,schedule_interval,start_date):
    dag_code="""
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import XCom
from datetime import datetime
import matplotlib.pyplot as plt
import io
import base64
from sqlalchemy import create_engine,inspect,select,func,text,insert,MetaData,Table,update,Column
import logging
import pandas as pd
default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '{}',
    'email_on_failure': False,
    'email_on_retry': False,
    'timezone': 'Asia/Kolkata',
}}


with DAG(
    dag_id='{}',
    default_args=default_args,
    schedule_interval='{}',
    catchup=False
)as dag:
    def load_data(**kwargs):
        source_db_connection="{}"
        service_id="{}"
        # kwargs['ti'].xcom_push(key='service_id', value=service_id)
        dest_db_connection="postgresql://akshali:Akshali3208@localhost:5432/data_profiler"
        source_engine = create_engine(source_db_connection)
        dest_engine = create_engine(dest_db_connection)
        source_conn=source_engine.connect()
        dest_conn=dest_engine.connect()
        source_metadata = MetaData(bind=source_engine)
        source_metadata.reflect(bind=source_engine)
        dest_metadata = MetaData(bind=dest_engine)
        # for table_name,table in source_metadata.tables.items():
        #     dest_table_name=table_name+service_id
        #     # source_table = Table(table_name, source_metadata, autoload=True, autoload_with=source_engine)
        #     dest_table = Table(dest_table_name, destination_metadata, autoload=True, autoload_with=source_engine)
        #     dest_table.create_all(dest_engine)
        #     source_data = source_conn.execute(dest_table.select()).fetchall()
        #     dest_conn.execute(dest_table.insert(), [dict(row) for row in source_data])
        #     # dest_table = Table(dest_table_name, destination_metadata,*(Column(column.name, column.type) for column in source_table.columns))
        #     # dest_table.create(dest_engine, checkfirst=True)
        #     # source_data = source_table.select().execute().fetchall()
        #     # dest_conn.execute(dest_table.insert(), [dict(row) for row in source_data]) 
        #     result= dest_conn.execute(select(text(table_name)))
        #     print("countttt",result.fetchall())
        for table_name, source_table in source_metadata.tables.items():
            dest_table_name = table_name+service_id
            if not dest_engine.dialect.has_table(dest_conn, dest_table_name):
                source_table = Table(table_name, source_metadata, autoload=True, autoload_with=source_engine)
                dest_table = Table(dest_table_name, dest_metadata, *(Column(column.name, column.type) for column in source_table.columns))
                dest_table.create(dest_engine)
    
        for table_name, source_table in source_metadata.tables.items():
            dest_table_name = table_name+service_id
            dest_table = Table(dest_table_name, dest_metadata, autoload=True, autoload_with=dest_engine)
            source_data = source_conn.execute(select([source_table])).fetchall()
            dest_conn.execute(dest_table.insert(), [dict(row) for row in source_data])
            result = dest_conn.execute(select([dest_table]))
            print("countttt",dest_table_name, result.fetchall())
                
        return service_id 
        
    def extract_data(**kwargs):
        ti = kwargs['ti']
        service_id=ti.xcom_pull(task_ids='load_data')
        print("service_id",service_id)
        db_connection="postgresql://akshali:Akshali3208@localhost:5432/data_profiler"
        # source_engine = create_engine(source_db_connection)
        engine = create_engine(db_connection)
        # source_conn=source_engine.connect()
        conn=engine.connect()
        # source_metadata=MetaData()
        metadata=MetaData()
        app_tablemetadata = Table('app_tablemetadata', metadata, autoload=True, autoload_with=engine)
        app_metrics = Table('app_metrics', metadata, autoload=True, autoload_with=engine)
        # inspector = inspect(engine)
        result=conn.execute(select(app_tablemetadata.c.table_name).where(app_tablemetadata.c.service_id==service_id))
        table_names=[row[0] for row in result.fetchall()]
        print("table name ",table_names)
        # table_names = inspector.get_table_names()
        # source_metadata.reflect(bind=source_engine)
        for table_name in table_names:
            table_name_with_id=table_name+str(service_id)
            row_count = conn.execute(select([func.count()]).select_from(text(table_name_with_id))).scalar()
            print(row_count)
            # columns = inspector.get_columns(table_name)
            # col_count = len(columns)
            # print(col_count)
            metadata = MetaData()
            metadata.reflect(bind=engine, only=[table_name_with_id])
            reflected_table = metadata.tables[table_name_with_id]
            col_count = len(reflected_table.columns)
            print(col_count)
            # duplicates = source_conn.execute(select([])).scalar()
            duplicates=0
            if conn.execute(select(app_tablemetadata).where((app_tablemetadata.c.table_name==table_name)&(app_tablemetadata.c.service_id==service_id))).scalar()==None: 
                conn.execute(insert(app_tablemetadata).values(row_count=row_count,col_count=col_count, table_name=table_name, duplicates=duplicates,service_id=service_id))
            else: 
                conn.execute(update(app_tablemetadata).where((app_tablemetadata.c.table_name==table_name)&(app_tablemetadata.c.service_id==service_id)).values(row_count=row_count,col_count=col_count,table_name=table_name,duplicates=duplicates))
            table_id = conn.execute(select(app_tablemetadata).where((app_tablemetadata.c.service_id==service_id)&(app_tablemetadata.c.table_name==table_name))).scalar()
            # columns_info = inspector.get_columns(table_name)
            # data_to_insert = []
            for column in reflected_table.columns:
                col_name=column.name
                # result_data=conn.execute(select([column]))
                # col_data=[row[column] for row in result_data]
                null_values = conn.execute(select([func.count()]).where(column==None)).scalar()
                unique=conn.execute(select([func.count(func.distinct(column))])).scalar()
                column_data_type=column.type
                is_numeric=False
                base_type = column_data_type.__class__.__name__.split('(')[0]
                numeric_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE']
                # plt.figure()
                # plt.bar(range(len(col_data)), col_data)
                # buf = io.BytesIO()
                # plt.savefig(buf, format='png')
                # buf.seek(0)
                # bar_plot_data = base64.b64encode(buf.read()).decode('utf-8')
                # plt.hist(col_data, bins=10)
                # buf = io.BytesIO()
                # plt.savefig(buf, format='png')
                # buf.seek(0)
                # histogram_data = base64.b64encode(buf.read()).decode('utf-8')
                # plt.close()
                if base_type.upper() in numeric_types:
                    is_numeric=True
                    maximum = conn.execute(select([func.max(column)])).scalar()
                    minimum = conn.execute(select([func.min(column)])).scalar()
                    mean = conn.execute(select([func.avg(column)])).scalar()
                    # median = source_conn.execute(select([func.max(text(col_name))]).select_from(text(table_name))).scalar()
                    standard_deviation = conn.execute(select([func.stddev(column)])).scalar()

                if conn.execute(select(app_metrics).where((app_metrics.c.col_name==col_name)&(app_metrics.c.table_id==table_id))).scalar()==None:
                    if not is_numeric:
                        insert_stmt = insert(app_metrics).values(col_name=col_name, col_type=base_type.upper(), null_values=null_values, unique=unique, table_id=table_id, is_numeric=is_numeric)
                    else:
                        insert_stmt = insert(app_metrics).values(col_name=col_name, col_type=base_type.upper(), null_values=null_values, unique=unique, table_id=table_id, is_numeric=is_numeric, maximum=maximum, minimum=minimum, mean=mean,  standard_deviation=standard_deviation)
                    conn.execute(insert_stmt)
                else:
                    if not is_numeric:
                        update_stmt = update(app_metrics).where((app_metrics.c.col_name==col_name)&(app_metrics.c.table_id==table_id)).values(col_name=col_name, col_type=base_type.upper(), null_values=null_values, unique=unique, is_numeric=is_numeric)
                    else:
                        update_stmt = update(app_metrics).where((app_metrics.c.col_name==col_name)&(app_metrics.c.table_id==table_id)).values(col_name=col_name, col_type=base_type.upper(), null_values=null_values, unique=unique, is_numeric=is_numeric, maximum=maximum, minimum=minimum, mean=mean,  standard_deviation=standard_deviation)
                    conn.execute(update_stmt)


        # def mark_success():
        #     service_id = kwargs['ti'].xcom_pull(task_ids='extract_data_task', key='service_id')
        #     dest_db_connection="postgresql://akshali:Akshali3208@localhost:5432/data_profiler"
        #     dest_engine = create_engine(dest_db_connection)
        #     dest_conn=dest_engine.connect()
        #     metadata=MetaData()
        #     app_pipeline = Table('app_pipeline', metadata, autoload=True, autoload_with=dest_engine)
        #     success_time=datetime.now()
        #     status=True
        #     insert_stmt = insert(app_pipeline).values(success_time=success_time, status=status,)
        #     dest_conn.execute(insert_stmt)



    load_data_task=PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  
    dag=dag
    )

    extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,  
    dag=dag
    )
    
    # mark_success_task = PythonOperator(
    # task_id='mark_success',
    # python_callable=mark_success,
    # provide_context=True,  
    # dag=dag
    # )
    load_data_task>>extract_data_task


"""

    dag_id=f'{service.service_name.replace(" ", "_")}_dag'
    db_connection=create_connection_url(service)
    dag_code=dag_code.format(start_date,dag_id,schedule_interval,db_connection,service.id)
    dag_folder ='/home/akshali/data_profiler/airflow/dags' 
    dag_file_path = os.path.join(dag_folder, f"{dag_id}.py")
    with open(dag_file_path, 'w') as f:
        f.write(dag_code)

def generate_cron_expression(schedule_interval):
    cron_expression=""
    if schedule_interval == 'daily':
        cron_expression = '0 0 * * *'  
    elif schedule_interval == 'monthly':
        cron_expression = '0 0 1 * *'  
    elif schedule_interval == 'weekly':
        cron_expression = '0 0 * * 0'  
    elif schedule_interval == 'hourly':
        cron_expression = '0 * * * *'
    elif schedule_interval == 'yearly':
        cron_expression = '0 0 1 1 *' 
    return cron_expression

@login_required
@require_http_methods(["GET","POST"])
def pipeline_view(request,id):
    if request.method=='POST':
        service=Service.objects.get(pk=id)
        schedule_interval = request.POST.get('schedule_interval')
        cron_expression = generate_cron_expression(schedule_interval) 
        in_start_date = datetime.now()
        start_date=pytz.timezone('UTC').localize(in_start_date)
        print(start_date)
        dag_folder ='/home/akshali/data_profiler/airflow/dags'
        dag_file_path = os.path.join(dag_folder, f"{service.service_name.replace(' ', '_')}_dag.py")
        if not os.path.exists(dag_file_path):
            # service_name=service.service_name
            create_dag_for_service(service,cron_expression,start_date)
        return redirect('dashboard')
    if request.method=='GET':
        return render(request,'pipeline/pipeline.html',{'id':id})
    
@login_required
@require_http_methods(["GET","POST"])
def pipelines_view(request):
    # services=Service.objects.filter(user=request.user)
    # return render(request,'pipeline/pipeline_list.html',{'services':services})
    return redirect('dashboard')


@login_required
@require_http_methods(["GET","POST"])
def pipeline_trigger_view(request,dag_id):
    # dag_id=dag_id+"_dag"
    url=f'http://localhost:8081/api/v1/dags/{dag_id}/dagRuns'
    headers={
        'Content-Type':'application/json',
        'Authorization':'Basic YWRtaW46YWRtaW4='
    }
    payload = {
        "dag_run_id": f"{dag_id}",
        'conf': {'dbid': dag_id}
    }
    json_payload = json.dumps(payload)
    print(url)
    response = requests.post(url, headers=headers, data=json_payload)
    print(response)
    return redirect('piepline_details',dag_id=dag_id)

@login_required
@require_http_methods(["GET","POST"])
def pipeline_details_view(request,dag_id):
    # dag_id=dag_id+"_dag"
    url=f'http://localhost:8081/api/v1/dags/{dag_id}/dagRuns'
    headers={
        'Content-Type':'application/json',
        'Authorization':'Basic YWRtaW46YWRtaW4='
    }
    payload = {
        "dag_run_id": f"{dag_id}",
        'conf': {'dbid': dag_id}
    }
    json_payload = json.dumps(payload)
    print(url)
    response = requests.get(url, headers=headers)
    pipeline_details = response.json()
    # print(response_data["dag_id"])
    print(pipeline_details["dag_runs"])
    return render(request,'pipeline/pipeline_details.html',{'pipeline_details':pipeline_details["dag_runs"],'dag_id':dag_id})

@login_required
@require_http_methods(["GET","POST"])
def pipeline_logs_view(request,dag_id):
    # dag_id=dag_id+"_dag"
    url=f'http://localhost:8081/api/v1/dags/{dag_id}/dagRuns/{dag_id}/taskInstances/extract_data/logs/1'
    headers={
        'Content-Type':'application/json',
        'Authorization':'Basic YWRtaW46YWRtaW4='
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        print(url) 
        print(response.content.decode('utf-8'))# Raise an error for HTTP errors (4xx or 5xx)
        # pipeline_details = response.json()
        # print(pipeline_details)  # For debugging purposes
        return render(request, 'pipeline/pipeline_logs.html', {'logs': response.text})
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return render(request, 'pipeline/pipeline_logs.html', {'error_message': str(e)})

# @login_required
# @require_http_methods(["GET","POST"])
# def piepline_logs_view(request,dag_id):
#     dag_folder ='/home/akshali/data_profiler/airflow/logs/log_file.log' 
#     dag_file_path = os.path.join(dag_folder, f"{dag_id}.py")
#     with open(dag_file_path, 'w') as f:
#         f.write(dag_code)
#     return render(request,'pipeline/pipeline_details.html',pipeline_details)
