# блок импорта
from datetime import datetime, timedelta
# импорт бизнес-логики - класса с описанием методов для работы с базой данных
from airflow_logic import ReloadData

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data_from_db() -> None:
    """
    Функция для выгрузки данных из базы
    :return: ничего не возвращает, ничего не изменяет, данные передаются через ХСоm Airflow
    """
    db = 'MSSQL'
    reload_data = ReloadData(db)
    reload_data.connect_to_db()
    reload_data.get_data_from_db()
    reload_data.disconnect()

def load_data_to_db() -> None:
    """
    Функция для загрузки данных в базу
    :return: ничего не возвращает, изменяет состояние базы данных
    """
    db = 'PostgreSQL'
    reload_data = ReloadData(db)
    reload_data.connect_to_db()
    reload_data.clear_table()
    reload_data.load_data_to_other_db()
    reload_data.disconnect()

# стандартный набор аргументов
args = {
         'owner': 'airflow',
         'retries': 1,
         'retry_delay': timedelta(seconds=7),
         'provide_context': True
         }

# настройки DAG
dag = DAG(
        dag_id='Reload_data_from_MSSQL_to_PostgreSQL',
        schedule=None,
        start_date=datetime(2023, 11, 23),
        default_args=args,
        tags=["backend"],
        catchup=False,
        description='' )

# описание задач дага с подключением оператора
task_get_data_from_db = PythonOperator(
        task_id='get_data_from_db_task',
        python_callable=get_data_from_db,
        dag=dag)

task_load_data_to_db = PythonOperator(
        task_id='load_data_to_db_task',
        python_callable=load_data_to_db,
        dag=dag)

# собственно DAG
task_get_data_from_db >> task_load_data_to_db