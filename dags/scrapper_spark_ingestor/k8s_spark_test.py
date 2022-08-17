from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta
from airflow import DAG
from pathlib import Path
from airflow.operators.python import PythonOperator
from scrapper_spark_ingestor.utils import _eval_missing_data
from scrapper_spark_ingestor.utils.update_detector_v2 import _scrap
from airflow.operators.mysql_operator import MySqlOperator

local_tz = pendulum.timezone("America/Mexico_City")
queries_path = Path(__file__).parent / 'queries'
default_args = {
    'owner': 'Erick',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 14, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='scrapper_spark_ingestor',
    default_args=default_args,
    catchup=False,
    schedule_interval="@monthly",
    template_searchpath=str(queries_path),
) as dag:

    scrap = PythonOperator(
        task_id="scrap_data",
        python_callable=_scrap
    )

    create_tables = MySqlOperator(
        task_id="create_tables_ddl",
        mysql_conn_id='mysql_conn_id', 
        sql='DDL.sql',
    )
    
    filter_procesed_files = PythonOperator(
        task_id="filter_procesed_files",
        python_callable=_eval_missing_data,
    )

    operation = SparkSubmitOperator(
        task_id='operator_test',
        conn_id='spark_conn_id',
        application=str(Path(__file__).parent /  'utils' / 'spark_script.py'),
        py_files=str(Path(__file__).parent /  'utils' / 'downloader.py') + ',' +
            str(Path(__file__).parent /  'utils' / 'file_interpreter.py') + ',' +
            str(Path(__file__).parent /  'utils' / 'pyspark_utils.py'),
        files='{{ task_instance.xcom_pull(task_ids="scrap_data") }}' +
            ',' + str(Path(__file__).parent /  'utils' / 'custom-ground-359609-409ce5a04fea._json'),
        jars=str(Path(__file__).parent /  'utils' / 'mysql-connector-java-5.1.47.jar'),
        total_executor_cores=4,
        # packages="com.google.cloud.spark:spark-3.1-bigquery:0.26.0-preview",
        executor_cores=3,
        executor_memory='4g',
        driver_memory='4g',
        name='spk_ingestion',
        execution_timeout=timedelta(minutes=10),
    )

    mysql_clean = MySqlOperator(
        task_id="mysql_clean_and_expose",
        mysql_conn_id='mysql_conn_id', 
        sql='CLEAN_DATA.sql',
    )

    scrap >> filter_procesed_files >> operation >> mysql_clean
    create_tables >> filter_procesed_files