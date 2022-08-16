from airflow import DAG
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.python_operator import PythonOperator


from pendulum import timezone
import datetime

from insight_engine_etl.ie_ep724.ie_ep724_observation import Datasource, TransformToObservations, TransformToSeries
from insight_engine_etl.common import get_airflow_default_args, DAY
from insight_engine_etl.database import engine, load_observations_from_csv, load_series_from_csv


DAG_NAME = 'ie_ep724_observation_update_weekly'
tz = timezone('America/New_York')
attachment_path = 's3://{{ var.value.dataops_bucket }}/airflow/{{ dag.dag_id }}/{{ ts_nodash }}/'
target_date = "{{next_ds}}"
excute_date = "{{ts_nodash}}"
# get the data
ep724_data = Datasource()
file_name = "{{ next_ds }}.xlsx"

default_args = get_airflow_default_args(
    start_date=datetime.datetime(2022, 6, 15, 13, 0, tzinfo=tz)
)

dag = DAG(DAG_NAME,
          default_args=default_args,
          catchup=True,
          max_active_runs=1,
          schedule_interval='0 13 * * 3')  # "minute hour day(month) month day(week)" "0 13 * * 3" At 13:00 on Wednesday.

with dag:
    wait_for_data = PythonSensor(
        task_id='wait_for_data',
        python_callable=ep724_data.wait_for_data,
        # provide_context=True,
        poke_interval=DAY,
        timeout=DAY * 5,
        op_kwargs={"target_date": target_date},
        mode='reschedule'
    )

    fetch = PythonOperator(
        task_id="fetch",
        python_callable=ep724_data.fetch,
        op_kwargs={
            'target_path': attachment_path + file_name
        }
    )

    transform_to_series = PythonOperator(
        task_id='transform_to_series',
        python_callable=TransformToSeries().run,
        op_kwargs={
            'source_path': attachment_path + file_name,
            'target_path': attachment_path + 'series.csv.gz',
        }
    )

    load_series = PythonOperator(
        task_id='load_series',
        python_callable=load_series_from_csv,
        provide_context=False,
        op_kwargs={
            'engine': engine,
            'src': attachment_path + 'series.csv.gz',
            'compression': 'gzip'
        }
    )

    transform_to_observations = PythonOperator(
        task_id='transform_to_observations',
        python_callable=TransformToObservations().run, # why don't call TransformToObservations.transform_dataframe(), but use .run
        op_kwargs={
            'source_path': attachment_path + file_name,
            'target_path': attachment_path + 'observation.csv.gz',
            'target_date': target_date,
            'release_date': datetime.datetime.utcnow().strftime('%Y-%m-%d')
        }
    )

    load_observations = PythonOperator(
        task_id='load_observations',
        python_callable=load_observations_from_csv,
        provide_context=False,
        op_kwargs={
            'engine': engine,
            'src': attachment_path + 'observation.csv.gz',
            'compression': 'gzip'
        }
    )




    

    # wait_for_data >> fetch >> update_series >> transform_to_observations >> load_observation

    wait_for_data >> fetch
    fetch >> [transform_to_series, transform_to_observations]
    transform_to_series >> load_series
    [load_series, transform_to_observations] >> load_observations
