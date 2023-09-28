import requests
import json
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023, 6, 23)
}

thedag = DAG(
    'species_and_people_ETL_Dag',
    schedule_interval='@wdaily',
    default_args=default_args
)


def species_data():
    api_response = requests.get('https://swapi.dev/api/species/?format=json')
    data = api_response.json()
    results = data['results']
    dataframe = pd.json_normalize(results)
    dataframe = dataframe[['name', 'classification', 'average_height' , 'language', 'designation']]
    dataframe.replace('n/a', '"', inplace=True)
    return dataframe.values.tolist()


def people_data():
    dataframe = pd.read_json("/content/people.json")
    dataframe.replace('n/a', 'empty', inplace=True)
    dataframe = dataframe[['name','height','mass','foreign_language','gender','birth_year','eye_color']]
    return dataframe.values.tolist()


def create_db():
    mssql_hook1 = MsSqlHook(mssql_conn_id = 'sqlserver')
    create_db = """ 
        IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'myDB')
        BEGIN
            CREATE DATABASE myDB
        END;
        """
    
    mssql_hook1.run(create_db, autocommit = True)



def create_tables():    
    mssql_hook = MsSqlHook(mssql_conn_id='sqlserver')    
    
    create_sql_tables = """
        USE myDB;
        
        IF OBJECT_ID('species', 'U') IS NOT NULL AND OBJECT_ID('people', 'U') IS  NOT NULL
        BEGIN 
            DROP TABLE dbo.species;
            DROP TABLE dbo.people;

            CREATE TABLE dbo.species(
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
                name varchar(100),
                classification varchar(100),
                average_height varchar(100),
                language varchar(100), 
                designation varchar(100)
            );
            

            CREATE TABLE dbo.people(
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
                name varchar(100),
                height INT,
                mass INT,
                foreign_language varchar(100),
                gender varchar(100),
                birth_year varchar(100),
                eye_color varchar(100) 
            );
        END  

        ELSE
        BEGIN
            CREATE TABLE dbo.species(
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
                name varchar(100),
                classification varchar(100),
                average_height varchar(100),
                language varchar(100), 
                designation varchar(100)
                );

            CREATE TABLE dbo.people(
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
                name varchar(100),
                height INT,
                mass INT,
                foreign_language varchar(100),
                gender varchar(100),
                birth_year varchar(100),
                eye_color varchar(100) 
            );
        END
        """
      
    mssql_hook.run(create_sql_tables, autocommit = True) 


def load_species_data_to_MSSQL(ti):
    species_data = ti.xcom_pull(task_ids='species_data_filtering_task')
    species_df = pd.DataFrame(species_data, columns=['name', 'classification', 'average_height' , 'language', 'designation'])
    species_result = species_df.values.tolist()
    species_hook = MsSqlHook(mssql_conn_id='sqlserver')
    species_hook.insert_rows('myDB.dbo.species', species_result, target_fields=['name', 'classification', 'average_height' , 'language', 'designation'])


def load_people_data_to_MSSQL(ti):
    people_data = ti.xcom_pull(task_ids='people_data_filtering_task')
    people_df = pd.DataFrame(people_data, columns=['name','height','mass','foreign_language','gender','birth_year','eye_color'])
    people_result = people_df.values.tolist()
    people_hook = MsSqlHook(mssql_conn_id='sqlserver')
    people_hook.insert_rows('myDB.dbo.people', people_result, target_fields=['name','height','mass','foreign_language','gender','birth_year','eye_color'])


def load_data_to_MongoDB():
    hookSql = MsSqlHook(mssql_conn_id='sqlserver')
    sql = '''
        SELECT
            name, 
            height, 
            mass, 
            gender, 
            foreign_language
        FROM 
            myDB.dbo.people
        UNION ALL   
        SELECT
            name, 
            classification, 
            language 
        FROM 
            myDB.dbo.species;
    '''

    df = hookSql.get_pandas_df(sql)    
    hookMongo = MongoHook(conn_id='mongodb')
    client = hookMongo.get_conn()
    db = client['myMongoDB']
    db.myMongoDB.drop()
    coll=db['starwars']
    coll.insert_many(df.to_dict('records'))



species_data_filtering_task = PythonOperator(task_id = 'species_data_filtering_task', python_callable = species_data, dag=thedag)
people_data_filtering_task = PythonOperator(task_id = 'people_data_filtering_task', python_callable = people_data, dag=thedag)
sql_db_creation = PythonOperator(task_id = 'create_sql_db', python_callable = create_db, dag=thedag)
sql_table_creation = PythonOperator(task_id = 'create_sql_tables', python_callable = create_tables, dag=thedag)
load_speciesdata_to_sql = PythonOperator(task_id = 'speciesdata_to_SQL', python_callable=load_species_data_to_MSSQL, dag=thedag)
load_peopledata_to_sql = PythonOperator(task_id = 'peopledata_to_SQL', python_callable=load_people_data_to_MSSQL, dag=thedag)
load_data_to_mongo = PythonOperator(task_id = 'data_to_MongoDB', python_callable=load_data_to_MongoDB, dag=thedag)

species_data_filtering_task >> people_data_filtering_task >> sql_db_creation >> sql_table_creation >> load_speciesdata_to_sql >> load_data_to_mongo





