from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def fetch_current_date(**kwargs):
    from datetime import date
    current_date = date.today().isoformat()
    print(f"Today's date is: {current_date}")
    # Push date to XCom (optional twist)
    kwargs['ti'].xcom_push(key='current_date', value=current_date)


def log_date_and_sleep(**kwargs):
    # Pull date from XCom
    ti = kwargs['ti']
    current_date = ti.xcom_pull(key='current_date', task_ids='fetch_current_date')
    print(f"Received date from XCom: {current_date}")
    import time
    time.sleep(10)
    print("Sleep complete.")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}
    
with DAG(
    dag_id='beginner_challenge_dag',
    default_args=default_args,
    description='A beginner practice DAG with multiple tasks',
    start_date=datetime(2024, 3, 6),
    schedule_interval='0 8 * * *',  # Every day at 8 AM
    catchup=False,
    tags=['practice'],
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    fetch_current_date = PythonOperator(
        task_id='fetch_current_date',
        python_callable=fetch_current_date,
        provide_context=True,
    )

    sleep_task = PythonOperator(
        task_id='log_date_and_sleep',
        python_callable=log_date_and_sleep,
        provide_context=True,
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> fetch_current_date >> sleep_task >> end
