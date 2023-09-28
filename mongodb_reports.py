from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 6, 23),
        }


dag = DAG(
    'mongodb_reports', 
    schedule_interval='@daily',
    default_args=default_args
)

connection = "mongodb://mongo:mongopass@mongodb:27017"

def first_mongo():
    client = MongoClient(connection)
    db = client["myMongoDB"]
    coll = db["starwars"]
    result = coll.find({"gender": "empty"}, {"name": 1, "mass": 1, "_id": 0}).sort( { "height": -1 } )
    return list(result)


def second_mongo():
    client = MongoClient(connection)
    db = client["myMongoDB"]
    coll = db["starwars"]
    result = coll.find().count()
    return list(result)


firstmongoquery = PythonOperator(task_id='first_mongo_report', python_callable=first_mongo, dag=dag)
secondmongoquery = PythonOperator(task_id='second_mongo_report', python_callable=second_mongo, dag=dag)

firstmongoquery >> secondmongoquery
