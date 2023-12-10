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
        obj = json.loads(line)
        i = i + 1

        # data cleaning example: "droping" data with one word titles
        if len(obj['title'].split(" ")) == 1:
            continue

        data.append(obj)
        if i == 10:  # you can change the limit of reading in data objects by changing the number in if- condition
            break
    
    ## if everything went well this line should print out the first element of the list
    #print(json.dumps(data[0], indent=2))   
    
    modified_data = []
    # TODO: add data cleaning
    ## line by line data modification
    for obj in data:
        obj.pop('abstract')
        obj.pop('comments')
        obj.pop('versions')
        modified_data.append(obj)

    with open(f'/data/staging/cleaned_data.json', 'w', encoding='utf-8', errors="replace") as json_file:
        for obj in modified_data:
            json_file.write(json.dumps(obj) + "\n")

def getArticleResource(output_folder):
    augmented_data = []
    for line in open('/data/staging/cleaned_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)

        # augment from dblp database
        url = "https://dblp.org/search/publ/api"
        params = {
            "q": obj.get("title"),
            "format": "json",
            "h": 20
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("result").get("hits")
            if int(results.get("@total")) > 0:
                for hit in results.get("hit"):
                    if hit.get("info").get("title") == obj.get("title"):
                        obj['url'] = hit.get("info").get("url")

        # augment from crossref database
        if obj['doi']:
            url = f"https://api.crossref.org/works/{obj['doi']}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()['message']
                obj['type'] = data.get('type')
                obj['reference_count'] = data.get('reference-count')
                obj['references_doi'] = [ref['DOI'] for ref in data.get('reference', []) if 'DOI' in ref]
                obj['is_referenced_by_count'] = data.get('is-referenced-by-count')

        augmented_data.append(obj)

    with open(f'{output_folder}/augmented_data.json', 'w', encoding='utf-8', errors="replace") as json_file:
        for obj in augmented_data:
            json_file.write(json.dumps(obj) + "\n")

def escape(value):
    if value is None:
        return None
    return value.replace("'", "''")

def ifnull(value):
    if value is None:
        return 'NULL'
    return value

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
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
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
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = ''
    unique_authors = set()

    for row in data:
        authors = row.get("authors_parsed")
        for author in authors:
            if author[1] + author[0] not in unique_authors:
                sql_file = sql_file + f'INSERT INTO author (first_name, last_name) VALUES (\'{escape(author[1])}\', \'{escape(author[0])}\') ON CONFLICT (first_name, last_name) DO NOTHING;\n'
                unique_authors.add(author[1] + author[0])
    
    with open(f'dags/sql/insert_authors.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_articles(output_folder):
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = ''
    
    for row in data:
        sql_file = sql_file + f"""
        INSERT INTO article (article_id, title, doi, update_date, journal_ref, category_id, type, reference_count, is_referenced_by_count, references_doi)
        VALUES (
            '{row['id']}',
            '{escape(row['title'])}',
            '{ifnull(row.get('doi'))}',
            '{row.get('update_date')}',
            '{ifnull(escape(row.get('journal-ref')))}',
            (SELECT ID FROM category WHERE name = '{row.get("categories")}'),
            '{ifnull(row.get('type'))}',
            {ifnull(row.get('reference_count'))},
            {ifnull(row.get('is_referenced_by_count'))},
            {'array[' if row.get('references_doi') else 'NULL'} {','.join([f"'{doi}'" for doi in row.get('references_doi', [])])} {']' if row.get('references_doi') else ''}
        )
        ON CONFLICT (article_id, update_date) DO NOTHING;\n
        """
    
    with open(f'dags/sql/insert_articles.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_articles_authors(output_folder):
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = ''
    
    for row in data:
        authors = row.get("authors_parsed")
        for author in authors:
            sql_file = sql_file + (f'INSERT INTO article_author (article_id, author_id) '  
            f'VALUES (\'{row.get("id")}\', (SELECT ID FROM author WHERE first_name = \'{escape(author[1])}\' AND last_name=\'{escape(author[0])}\')) ON CONFLICT (article_id, author_id) DO NOTHING;\n')
                
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
