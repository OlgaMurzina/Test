import pyodbc
import psycopg2
import pandas as pd
import connect_MSSQL
import connect_PostgreSQL


class ReloadData():
    """
    Класс для работы с базой данных
    """

    def __init__(self, db):
        """
        Конструктор класса
        :param db: название базы данных, с которой работаем
        """
        self.db = db
        self.conn = None
        self.cursor = None
        self.get_data_from_db_task_name = 'get_data_from_db_task'

    def connect_to_db(self) -> None:
        """
        Метод подключения к базе данных
        :return: ничего не возвращает, изменяет состояние глобальных переменных self.conn, self.cursor
        """
        try:
            if self.db == 'MSSQL':
                connection = connect_MSSQL.connection_string()
                self.conn = pyodbc.connect(connection)
            elif self.db == 'PostgreSQL':
                connection = connect_PostgreSQL.connection_string()
                self.conn  = psycopg2.connect(connection)
            print(f'Соединение с базой данных {self.db} установлено')
            self.cursor = self.conn.cursor()
            self.conn.autocommit = False
        except Exception as e:
            print(f'{e}')

    def get_data_from_db(self, **kwargs) -> None:
        """
        Метод выгрузки данных из базы, кладет данные в ХСоm Airflow
        :return: ничего не возвращает, работает через ХСom Airflow
        """
        ti = kwargs['ti']
        try:
            name_table = 'A'
            query = f"""SELECT * FROM {name_table}"""
            response = self.cursor.execute(query).fetchall()
            # на всякий случай для унификации данных преобразуем их в датафрейм
            names = [x[0] for x in self.cursor.description]
            df = pd.DataFrame(response, columns=names)
            # выгружаем датафрейм в  ХСom Airflow
            ti.xcom_push(task_id=self.get_data_from_db_task_name, key='data_from_db', value=df)
        except Exception as e:
            raise(f'{e}')

    def clear_table(self) -> None:
        """
        Метдод очистки таблицы-приемника
        :return: ничего, изменяет состояние таблицы-приемника
        """
        try:
            # костыль на случай попытки случайной очистки таблицы-источника
            if self.db == 'PostgreSQL':
                name_table = "A'"
                query = f"""DELETE FROM {name_table}"""
                self.cursor.execute(query)
                self.conn.commit()
        except Exception as e:
            print(f'{e}')

    def load_data_to_other_db(self, **kwargs) -> None:
        """
        Метод загружает данные в таблицу-приемник
        :param kwargs: для чрения из ХСоm Airflow читаем контекст Airflow
        :return: ничего, изменяет состояние таблицы-приемника
        """
        ti = kwargs['ti']
        df = ti.xcom_pull(task_id=self.get_data_from_db_task_name, key='data_from_db')
        try:
            for record in df:
                query = """INSERT INTO (field1, field2, field3)
                           VALUES(?, ?, ?)',
                           (record.field1, record.field2, record.field3) """
                self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            raise(f'{e}')

    def disconnect(self) -> None:
        """
        Метод разрывает соединение с базой данных
        :return: ничего, закрывает соединение с базой данных
        """
        print(f'Соединение с базой данных {self.db} закрыто')
        self.cursor.close()
        self.conn.close()


