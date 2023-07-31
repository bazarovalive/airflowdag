
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# функция подключения к базе данных Postgres
def connect_to_database():
    url = f'postgresql+psycopg2://admin:admin@localhost:5432/airflow.db'
    engine = create_engine(url)
    conn = engine.connect()
    return conn


def check_connection(connection):
    try:
        connection.execute("SELECT 1")
        print("Соединение с базой данных успешно установлено.")
    except Exception as e:
        print("Ошибка соединения с базой данных:", str(e))


def convert_date_columns(df, keyword):
    date_formats = ['%Y-%m-%d', '%d.%m.%Y']
    for column_name in df.columns:
        if keyword in column_name.lower():
            for format_ in date_formats:
                try:
                    df[column_name] = pd.to_datetime(df[column_name], format=format_)
                    break  # если формат совпадает, выходим из цикла и идем к следующему столбцу
                except ValueError:
                    continue  # если формат не совпадает, просто продолжаем и ищем совпадение в следующем формат


def read_csv_file(filepath, sep, index_col, keep_default_na):
    encodings = ["ibm866", "utf-8", "cp866"]
    for encoding_ in encodings:
        try:
            df = pd.read_csv(filepath_or_buffer=filepath, sep=sep, index_col=index_col, keep_default_na=keep_default_na, encoding=encoding_)
            return df
        except UnicodeDecodeError:
            print(f"Ошибка чтения файла: {filepath} с кодировкой {encoding_}")


def load_data_to_table(table_name, conn,  filepath, sep, index_col, keep_default_na):
    df = read_csv_file(filepath, sep, index_col, keep_default_na)
    convert_date_columns(df, keyword='date')
    check_connection(conn)
    try:
        # Попытка выполнить импорт данных в базу данных
        df.to_sql(table_name, conn, if_exists='replace', index=False, schema='ds')
        print("Импорт данных успешно выполнен")
    except Exception as e:
        print("Ошибка при импорте данных:", str(e))


# Определим функции для каждой задачи
def taskbalance():
    conn = connect_to_database()
    load_data_to_table('ft_balance_f', conn,
                   '/data/ft_balance_f.csv',
                   sep=';', index_col=0, keep_default_na=False)

def taskposting():
    conn = connect_to_database()
    load_data_to_table('ft_posting_f', conn,
                   '/Users/olgastash/airflow/data/ft_posting_f.csv',
                   sep=';', index_col=0, keep_default_na=False)

def taskaccount():
    conn = connect_to_database()
    load_data_to_table('md_account_d', conn,
                       '/data/md_account_d.csv',
                       sep=';', index_col=0, keep_default_na=False)

def taskcurrency():
    conn = connect_to_database()
    load_data_to_table('md_currency_d', conn,
                       '/data/md_currency_d.csv',
                       sep=';', index_col=0, keep_default_na=False)

def taskexchange():
    conn = connect_to_database()
    load_data_to_table('md_exchange_rate_d', conn,
                       '/data/md_exchange_rate_d.csv',
                       sep=';', index_col=0, keep_default_na=False)

def taskledger():
    conn = connect_to_database()
    load_data_to_table('md_ledger_account_s', conn,
                       '/data/md_ledger_account_s.csv',
                       sep=';', index_col=0, keep_default_na=False)
# query = "SELECT * FROM DS.md_exchange_rate_d"
# result = pd.read_sql_query(query, conn)
# # Проверяем наличие данных
# if not result.empty:
#     print("Таблица успешно создана и содержит данные.")
# else:
#     print("Произошла ошибка, таблица пуста или не создана.")


def log_loading_start():
    conn = connect_to_database()
    start_time = datetime.now()
    check_connection(conn)
    data = {'process_name': ['data loading'], 'start_time': [start_time], 'end_time': [None]}
    df = pd.DataFrame(data)
    try:
        df.to_sql('logtable', conn, if_exists='append', index=False, schema='logs')
        print("Импорт данных успешно выполнен")
    except Exception as e:
        print("Ошибка при импорте данных:", str(e))
    conn.close()


def log_loading_end():
    conn = connect_to_database()
    # Добавление таймера (паузы) на 5 секунд
    time.sleep(5)
    end_time = datetime.now()
    df = pd.read_sql_table('logtable', con=conn, schema='logs')
    df.loc[(df['process_name'] == 'data loading') & (df['end_time'].isnull()), 'end_time'] = end_time
    df.to_sql(name='logtable', con=conn, if_exists='replace', index=False, schema='logs')
    conn.close()

log_loading_start()
log_loading_end()

# DAG определение
with DAG(
    dag_id='bank_data_loading_dag',
    description='DAG для загрузки данных из CSV файла в таблицы базы данных',
    schedule ='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

 # Определение задач логирования начала и окончания загрузки данных
    log_start_task = PythonOperator(
        task_id='log_loading_start',
        python_callable=log_loading_start,
        dag=dag
    )

# Определение задач загрузки данных для каждой таблицы
    load_ft_balance_data = PythonOperator(
        task_id='load_ft_balance_data',
        python_callable=taskbalance,
        dag=dag
    )

    load_ft_posting_data = PythonOperator(
        task_id='load_ft_posting_data',
        python_callable=taskposting,
        dag=dag
    )

    load_md_account_d_data = PythonOperator(
        task_id='load_md_account_d_data',
        python_callable=taskaccount,
        dag=dag
    )

    load_md_currency_d_data = PythonOperator(
        task_id='load_md_currency_d_data',
        python_callable=taskcurrency,
        dag=dag
    )

    load_md_exchange_rate_d_data = PythonOperator(
        task_id='load_md_exchange_rate_d_data',
        python_callable=taskexchange,
        dag=dag
    )


    load_md_ledger_account_s_data = PythonOperator(
        task_id='load_md_ledger_account_s_data',
        python_callable=taskledger,
        dag=dag
    )

    log_end_task = PythonOperator(
        task_id='log_loading_end',
        python_callable=log_loading_end,
        dag=dag
    )

log_start_task >> [load_ft_balance_data, load_ft_posting_data, load_md_account_d_data, load_md_currency_d_data, load_md_exchange_rate_d_data, load_md_ledger_account_s_data] >> log_end_task
