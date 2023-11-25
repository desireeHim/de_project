from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
#%pip install requests
import requests
import json

def read_in_data(output_folder):
    data = []
    i = 0
    for line in open('/data/arxiv-metadata-oai-snapshot.json', 'r', encoding='utf-8', errors="replace"):
        data.append(json.loads(line))
        i = i + 1
        if i == 10:    # you can change the limit of reading in data objects by changing the number in if- condition
            break
    
    ## if everything went well this line should print out the first element of the list
    #print(json.dumps(data[0], indent=2))   
    # data cleaning example: "droping" data with one word titles
    data = [obj for obj in data if len(obj['title'].split(" ")) > 1]
    modified_data = []

    # TODO: add data cleaning
    ## line by line data modification
    for line in data:
        modified_data.append(line)

    with open(f'/data/staging/cleaned_data.json', 'w', encoding='utf-8', errors="replace") as json_file:
        json.dump(modified_data, json_file, indent=2)
    #return data     

#def clean_data(output_folder):

#    with open(f'{output_folder}/cleaned_data.json', 'r', encoding='utf-8', errors="replace") as f:
#        data = json.load(f)
    # data cleaning example: "droping" data with one word titles
#    data = [obj for obj in data if len(obj['title'].split(" ")) > 1]
#    modified_data = []

    ## line by line data modification
#    for line in data:
#        modified_data.append(line)

def getArticleResource(output_folder):
    
    with open(f'/data/staging/cleaned_data.json', 'r', encoding='utf-8', errors="replace") as f:
        data = json.load(f)
    #url = "https://serpapi.com/search"
    url = "https://dblp.org/search/publ/api"

    ##params = {
    ##"engine": "google_scholar",
    ##"q": title,
    ##"api_key": "95e72264387e5901953190dbc9608e0a4bc625ae3e2d4b3b3dca1c3995d628d8"
    ##}
    for obj in data:
        params = {
        "q": obj.get("title"),
        "format": "json",
        "h": 20
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("result").get("hits")
            if int(results.get("@total")) == 0:
                break
            for hit in results.get("hit"):
                if hit.get("info").get("title") == obj.get("title"):
                    obj['url'] = hit.get("info").get("url")

    with open(f'{output_folder}/augmented_data.json', 'w', encoding='utf-8', errors="replace") as json_file:
        json.dump(data, json_file, indent=2)

DEFAULT_ARGS = {
    'owner': 'DE proj. team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DATA_FOLDER = '/tmp/data'

dag = DAG(
    dag_id='ingestion', # name of dag
    #schedule_interval='01 0 * * *', # execute every day at 1 AM
    schedule_interval='2 * * * *', # execute every day at 1 AM
    start_date=datetime(2023,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER, # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS, # args assigned to all operators
)

first_task = PythonOperator(
   task_id='get_data',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=read_in_data,
    op_kwargs={
        'output_folder': DATA_FOLDER
    },
)

file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/data/staging',
    fs_conn_id='data_path',
    poke_interval=5,
    retries=5,
    timeout=30,
    dag=dag
)

first_task >> file_sensor

second_task = PythonOperator(
    task_id='augment_data',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=getArticleResource,
    op_kwargs={
        'output_folder': DATA_FOLDER
    },
)

# TODO: remove file from staging
delete_src_file = BashOperator(
    task_id='delete_src_file',
    bash_command='rm /data/staging/cleaned_data.json',
    dag=dag,
)

file_sensor >> second_task

second_task >> delete_src_file

def insert_categories(output_folder):
    with open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace") as f:
        data = json.load(f)
    sql_file = ''
    unique_cat = set()

    for row in data:
        if row.get("categories") not in unique_cat:
            unique_cat.add(row.get("categories"))
    
    
    for cat in unique_cat:
        sql_file = sql_file + f'INSERT INTO category (name) VALUES (\'{cat}\') ON CONFLICT (name) DO NOTHING;\n'
    
    with open(f'dags/sql/insert_categories.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_authors(output_folder):
    with open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace") as f:
        data = json.load(f)
    sql_file = ''
    unique_authors = set()

    for row in data:
        authors = row.get("authors_parsed")
        for author in authors:
            if author[1] + author[0] not in unique_authors:
                sql_file = sql_file + f'INSERT INTO author (first_name, last_name) VALUES (\'{author[1]}\', \'{author[0]}\') ON CONFLICT (first_name, last_name) DO NOTHING;\n'
                unique_authors.add(author[1] + author[0])
    
    with open(f'dags/sql/insert_authors.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_articles(output_folder):
    with open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace") as f:
        data = json.load(f)
    sql_file = ''
    
    for row in data:
        sql_file = sql_file + (f'INSERT INTO article (article_id, title, doi, update_date, journal_ref, category_id)' 
        f'VALUES (\'{row.get("id")}\', \'{row.get("title")}\',\'{row.get("doi")}\',\'{row.get("update_date")}\',\'{row.get("journal-ref")}\',' 
        f'(SELECT ID FROM category WHERE name = \'{row.get("categories")}\')) ON CONFLICT (article_id, update_date) DO NOTHING;\n')
    
    with open(f'dags/sql/insert_articles.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_articles_authors(output_folder):
    with open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace") as f:
        data = json.load(f)
    sql_file = ''
    
    for row in data:
        authors = row.get("authors_parsed")
        for author in authors:
            sql_file = sql_file + (f'INSERT INTO article_author (article_id, author_id) '  
            f'VALUES (\'{row.get("id")}\', (SELECT ID FROM author WHERE first_name = \'{author[1]}\' AND last_name=\'{author[0]}\')) ON CONFLICT (article_id, author_id) DO NOTHING;\n')
                
    with open(f'dags/sql/insert_article_author.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


create_tables = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="airflow_pg",
    sql="sql/create_tables.sql",
    dag=dag,
    trigger_rule='none_failed',
    autocommit=True,
)

second_task >> create_tables

third_task = PythonOperator(
    task_id='insert_categories',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_categories,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

create_tables >> third_task

fourth_task = PostgresOperator(
    task_id='insert_categories_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql='sql/insert_categories.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

third_task >> fourth_task

fifth_task = PythonOperator(
    task_id='insert_authors',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_authors,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

fourth_task >> fifth_task

sixth_task = PostgresOperator(
    task_id='insert_authors_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql='sql/insert_authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

fifth_task >> sixth_task

seventh_task = PythonOperator(
    task_id='insert_articles',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_articles,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

sixth_task >> seventh_task

eightth_task = PostgresOperator(
    task_id='insert_articles_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql=f'sql/insert_articles.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

seventh_task >> eightth_task

ninth_task = PythonOperator(
    task_id='insert_article_author',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_articles_authors,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

eightth_task >> ninth_task

tenth_task = PostgresOperator(
    task_id='insert_article_authors_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql=f'sql/insert_article_author.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

ninth_task >> tenth_task
