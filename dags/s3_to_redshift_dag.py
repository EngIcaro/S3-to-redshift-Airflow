# Instructions
# Similar to what you saw in the demo, copy and populate the trips table. Then, add another operator which creates a traffic analysis table from the trips table you created. Note, in this class, we won’t be writing SQL -- all of the SQL statements we run against Redshift are predefined and included in your lesson. Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn the previous exercise off, then turn this exercise on. Wait a moment and refresh the UI to see Airflow automatically run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials", client_type="redshift")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))


dag = DAG(
    # Setting the Dag name
    's3_to_redshift',
    # Scheduling start time
    start_date=datetime.datetime.now()
)


# -----------------
# Amazon Redshift is based on PostgreSQL. Amazon Redshift and PostgreSQL have 
# a number of very important differences that you must be aware of as you design
# and develop your data warehouse applications.
# -----------------
# To use the PostgresOperator to carry out SQL request, two parameters are required:
# sql and postgres_conn_id. These two parameters are eventually fed to the PostgresHook
# object that interacts directly with the Postgres database.
# -----------------
create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    # Create trips table in redshift
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

 
# -----------------
# Using Python Operator give us more flexibility to build complex functions
# and take advantages of the python language
# -----------------
copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    # Load data to S3 From trips tables in redshift
    python_callable=load_data_to_redshift
)



location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    # Creates a traffic analysis table from the trips table we created
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task