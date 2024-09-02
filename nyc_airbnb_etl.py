import os
from datetime import datetime
import pandas as pd
import logging
import csv

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator


def _read_csv_file_to_df():   #where correct path for the file which I have moved to the docker using next command docker cp /Users/<my name>/moodlecloud/moodlecloud_pandas/AirBnB_NY/AB_NYC_2019.csv minio:/AB_NYC_2019.csv where 'minio' should be replaced with docker name. To get all containers name run docker ps --format "{{.Names}}"
    try:
        # if file exists
        if not os.path.exists('/AB_NYC_2019.csv'):
            raise FileNotFoundError(f"File not found: {'/AB_NYC_2019.csv'}")

        # is readable
        if not os.access('/AB_NYC_2019.csv', os.R_OK):
            raise PermissionError(f"File is not readable: {'/AB_NYC_2019.csv'}")

        df = pd.read_csv('/AB_NYC_2019.csv')

    except FileNotFoundError as e:
        print(f"Error: {e}")

    except PermissionError as e:
        print(f"Error: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return df


#Transformation data
def _transform_df_data(ti):
    df = ti.xcom_pull(task_ids="read_file")
    df_transformed = df[df['price'] > 0].dropna()
    df_transformed['last_review'] = pd.to_datetime(df_transformed['last_review'])
    df_transformed = df_transformed.dropna(subset=['latitude', 'longitude'])

    default_date = pd.Timestamp('1900-01-01')
    df_transformed['last_review'].fillna(default_date, inplace=True)

    df_transformed['reviews_per_month'].fillna(0, inplace=True)
    logging.info(df_transformed.head(5))

    #df_transformed['reviews_per_month'].round()
    df_transformed.to_csv('./transformed_ab_nyc_2019.csv', index=None, header=False)

#Load data to postgres database
def _load_df_to_postgres():
    hook = PostgresHook(
        postgres_conn_id='postgres_airbnb'
    )
    hook.copy_expert(
        sql="COPY airbnb_listings FROM stdin WITH DELIMITER as ',' CSV HEADER",
        filename='./transformed_ab_nyc_2019.csv'
    )

#####  VALIDATION  #####
#comparison csv and populated table
def _get_csv_row_count():
    file_path = './transformed_ab_nyc_2019.csv'
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        return sum(1 for row in reader) - 1

def _get_postgres_row_count():
    hook = PostgresHook(
        postgres_conn_id='postgres_airbnb'
        )
    result = hook.get_first(f"select count(*) from airbnb_listings;")
    return result[0]

def _compare_counts():
    csv_count = _get_csv_row_count()
    postgres_count = _get_postgres_row_count()

    if csv_count == postgres_count:
        print(f"Row counts match: {csv_count}")
    else:
        raise ValueError(f"Row counts do not match! CSV: {csv_count}, Postgres: {postgres_count}")

#looking for nulls in critacal columns
#get amount of nulls in columns price, minimum_nights, availability_365
def _get_amt_of_nulls():
    hook = PostgresHook(
        postgres_conn_id='postgres_airbnb'
        )
    result = hook.get_first(
        '''
            select count(*) from
              airbnb_listings
            where
              price is null
              or minimum_nights is null
              or availability_365 is null
        '''
    )
    return result[0]

#Show result from function above and send it to  BranchPythonOperator
def _check_nulls_in_columns():
    amt_of_nulls = _get_amt_of_nulls()

    if amt_of_nulls == 0:
        return 'success_task'
    else:
        return 'fail_task_into_logging_file'
    #     print("No one critical column has NULLs")
    # else:
    #     raise ValueError("Some of the columns 'price' or 'minimum_nights' or 'availability_365' has NULLs")




with DAG(
    'nyc_airbnb_etl',
    start_date = datetime(2024, 8, 31),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # create_db = PostgresOperator(
    #     task_id='create_db',
    #     postgres_conn_id='postgres',
    #     sql='''
    #         CREATE DATABASE airflow_etl;
    #         '''
    # )
    #create table in postgres database
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_airbnb',
        sql='''
            DROP TABLE IF EXISTS airbnb_listings;
            CREATE TABLE airbnb_listings (
                id SERIAL PRIMARY KEY,
                name TEXT,
                host_id INTEGER,
                host_name TEXT,
                neighbourhood_group TEXT,
                neighbourhood TEXT,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                room_type TEXT,
                price INTEGER,
                minimum_nights INTEGER,
                number_of_reviews INTEGER,
                last_review DATE,
                reviews_per_month DECIMAL(5,2),
                calculated_host_listings_count INTEGER,
                availability_365 INTEGER
                );
            '''
    )

    #read data from airbnb file
    read_file = PythonOperator(
        task_id = 'read_file',
        python_callable=_read_csv_file_to_df
    )

    #transform airbnb data according to the business rules
    transform_df_data = PythonOperator(
         task_id = 'transform_df_data',
         python_callable=_transform_df_data
    )

    #load transformed data to postgres datbase
    load_df_to_postgres = PythonOperator(
        task_id = 'load_df_to_postgres',
        python_callable=(_load_df_to_postgres)
    )

    # error_notification = EmailOperator(
    #     task_id='error_notification',
    #     to='prokopivap@ukr.net',
    #     subject='Airflow Task Failed',
    #     html_content='<p>Your Airflow task has failed.</p>',
    #     trigger_rule='one_failed',  # Send email only if previous tasks fail
    #     dag=dag,
    # )

    #Compare counts in transformed tfile and in DB table
    validation_compare_counts = PythonOperator(
        task_id='validation_compare_counts',
        python_callable=_compare_counts,
        provide_context=True
    )

    # validation_check_for_nulls = PythonOperator(
    #     task_id = 'validation_check_for_nulls',
    #     python_callable = _check_nulls_in_columns,
    #     provide_context=True
    # )

    # check for nulls in column and redirect data in different cases (when validation ok then success_task else log the error in file)
    branch_validation_check_for_nulls = BranchPythonOperator(
        task_id='branch_validation_check_for_nulls',
        python_callable=_check_nulls_in_columns,
        provide_context=True
    )

    #when success
    success_task = DummyOperator(
        task_id='success_task',
        dag=dag,
    )

    #function on fail
    def log_failure():
        with open('./log_failures.txt', 'a') as log_file:
            log_file.write("Found NULL values in specific columns. Please check the data.")
        logging.info(f"Logged failure to 'log_failures.txt'")

    # call fail function
    fail_task_into_logging_file = PythonOperator(
        task_id='fail_task_into_logging_file',
        python_callable = log_failure
    )

    [create_table, read_file] >> transform_df_data >> load_df_to_postgres >> validation_compare_counts >> branch_validation_check_for_nulls
    branch_validation_check_for_nulls >> success_task
    branch_validation_check_for_nulls >> fail_task_into_logging_file