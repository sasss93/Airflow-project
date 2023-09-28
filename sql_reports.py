from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 6, 23),
        }


dag = DAG(
    'sql_reports', 
    schedule_interval='@daily',
    default_args=default_args
)


def first_query():
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = """
        SELECT 
            MAX(average_height)
        FROM 
            myDB.dbo.species
        """
    return hook.get_records(sql)


def second_query():
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = """
        SELECT 
            COUNT(species)
        FROM 
            myDB.dbo.species
        GROUP BY
            classification    
        """
    return hook.get_records(sql)


def third_query():
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = """
        SELECT 
            AVG(height)
        FROM 
            myDB.dbo.people
        GROUP BY
            gender    
        """
    return hook.get_records(sql)


def fourth_query():
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = """
        SELECT
        (SELECT 
            MIN(height)
        FROM 
            myDB.dbo.people)
        -
        (SELECT 
            MIN(height)
        FROM 
            myDB.dbo.species)
        AS Difference;    
        """
    return hook.get_records(sql)


def fifth_query():
    hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = """
        SELECT 
            NAME
        FROM 
            myDB.dbo.species
        WHERE language = 
                (SELECT 
                    foreign_language
                FROM 
                    myDB.dbo.people
                WHERE name = "Obi-Wan Kenobi");
        """
    return hook.get_records(sql)


first = PythonOperator(task_id='firstSQLQuery', python_callable=first_query, dag=dag)
second = PythonOperator(task_id='secondSQLQuery', python_callable=second_query, dag=dag)
third = PythonOperator(task_id='thirdSQLQuery', python_callable=third_query, dag=dag)
fourth = PythonOperator(task_id='fourthSQLQuery', python_callable=fourth_query, dag=dag)
fifth = PythonOperator(task_id='fifthSQLQuery', python_callable=fifth_query, dag=dag)


first >> second >> third >> fourth >> fifth 
