from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
import requests
import json

ARTICLES_TO_READ = 100


def sql_query(query):
    postgres_hook = PostgresHook(postgres_conn_id='airflow_pg')
    result = postgres_hook.get_records(query)
    return result


def read_in_data(output_folder):
    data = []
    i = 0
    for line in open('/data/arxiv-metadata-oai-snapshot.json', 'r', encoding='utf-8', errors="replace"):
        obj = json.loads(line)
        i = i + 1

        if i >= ARTICLES_TO_READ:  # you can change the limit of reading in data objects by changing the ARTICLES_TO_READ value
            break

        # skipping the articles that have already been read during the previous runs
        record_exists = len(sql_query(f"SELECT 1 FROM article WHERE article_id = '{obj['id']}'")) > 0
        if record_exists:
            continue

        # data cleansing
        ## "dropping" data with one word titles
        if len(obj['title'].split(" ")) == 1:
            continue

        ## dropping data with empty authors list
        if not obj.get('authors'):
            continue
        
        data.append(obj)

    
    ## if everything went well this line should print out the first element of the list
    #print(json.dumps(data[0], indent=2))   
    
    modified_data = []
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
    i = 0
    for line in open('/data/staging/cleaned_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        if i % 50 == 0:
            print(f'Augmenting article {i} ...')
        i += 1

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
                
                if obj['type'] == 'journal-article':
                    issns = data.get('ISSN', [])
                    obj['journal_issn'] = issns[0] if len(issns) > 0 else None
                    journal_names = data.get('container-title', [])
                    obj['journal_name'] = journal_names[0] if len(journal_names) > 0 else None

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

augment_data_task = PythonOperator(
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

file_sensor >> augment_data_task

augment_data_task >> delete_src_file

def insert_categories(output_folder):
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = 'SELECT 1;\n'
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
    sql_file = 'SELECT 1;\n'
    unique_authors = set()

    for row in data:
        authors = row.get("authors_parsed")
        for author in authors:
            if author[1] + author[0] not in unique_authors:
                sql_file = sql_file + f'INSERT INTO author (first_name, last_name) VALUES (\'{escape(author[1])}\', \'{escape(author[0])}\') ON CONFLICT (first_name, last_name) DO NOTHING;\n'
                unique_authors.add(author[1] + author[0])
    
    with open(f'dags/sql/insert_authors.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_journals(output_folder):
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = 'SELECT 1;\n'
    unique_issns = set()

    for row in data:
        issn = row.get('journal_issn')
        name = row.get('journal_name')
        if issn and issn not in unique_issns:
            sql_file = sql_file + f'INSERT INTO journal (issn, name) VALUES (\'{escape(issn)}\', \'{escape(name)}\') ON CONFLICT (issn) DO NOTHING;\n'
            unique_issns.add(issn)
    with open(f'dags/sql/insert_journals.sql', 'w', encoding='utf-8', errors="replace") as f:
        f.write(sql_file)


def insert_articles(output_folder):
    data = []
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        data.append(json.loads(line))
    sql_file = 'SELECT 1;\n'
    
    for row in data:
        sql_file = sql_file + f"""
        INSERT INTO article (article_id, title, doi, update_date, journal_ref, category_id, journal_id, type, reference_count, is_referenced_by_count, references_doi)
        VALUES (
            '{row['id']}',
            '{escape(row['title'])}',
            '{ifnull(escape(row.get('doi')))}',
            '{row.get('update_date')}',
            '{ifnull(escape(row.get('journal-ref')))}',
            (SELECT ID FROM category WHERE name = '{row.get("categories")}'),
            (SELECT ID FROM journal WHERE issn = '{row.get("journal_issn")}'),
            '{ifnull(row.get('type'))}',
            {ifnull(row.get('reference_count'))},
            {ifnull(row.get('is_referenced_by_count'))},
            {'array[' if row.get('references_doi') else 'NULL'} {','.join([f"'{escape(doi)}'" for doi in row.get('references_doi', [])])} {']' if row.get('references_doi') else ''}
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
    sql_file = 'SELECT 1;\n'
    
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

augment_data_task >> create_tables

insert_categories_task = PythonOperator(
    task_id='insert_categories',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_categories,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

create_tables >> insert_categories_task

insert_categories_to_db_task = PostgresOperator(
    task_id='insert_categories_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql='sql/insert_categories.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

insert_categories_task >> insert_categories_to_db_task

insert_authors_task = PythonOperator(
    task_id='insert_authors',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_authors,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_categories_to_db_task >> insert_authors_task

insert_authors_to_db_task = PostgresOperator(
    task_id='insert_authors_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql='sql/insert_authors.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

insert_authors_task >> insert_authors_to_db_task

insert_journals_task = PythonOperator(
    task_id = 'insert_journals',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_journals,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_authors_to_db_task >> insert_journals_task

insert_journals_to_db_task = PostgresOperator(
    task_id='insert_journals_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql='sql/insert_journals.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

insert_journals_task >> insert_journals_to_db_task

insert_articles_task = PythonOperator(
    task_id='insert_articles',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_articles,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_journals_to_db_task >> insert_articles_task

insert_articles_to_db_task = PostgresOperator(
    task_id='insert_articles_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql=f'sql/insert_articles.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

insert_articles_task >> insert_articles_to_db_task

insert_article_author_task = PythonOperator(
    task_id='insert_article_author',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_articles_authors,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_articles_to_db_task >> insert_article_author_task

insert_article_authors_to_db_task = PostgresOperator(
    task_id='insert_article_authors_to_db',
    dag=dag,
    postgres_conn_id='airflow_pg',
    sql=f'sql/insert_article_author.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

insert_article_author_task >> insert_article_authors_to_db_task

def run_neo4j_query(cypher_query):
    hook = Neo4jHook(conn_id='airflow_neo4j')
    return hook.run(cypher_query)

def insert_categories_to_graph(output_folder):
    unique_categories = set()
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)

        categories = obj.get("categories")
        if categories and categories not in unique_categories:
            run_neo4j_query(
                'MERGE (c:Category {name: "%(categories)s"})' % {
                    "categories": categories,
                }
            )
            unique_categories.add(categories)


def insert_journals_to_graph(output_folder):
    unique_issns = set()
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)

        issn = obj.get('journal_issn')
        name = obj.get('journal_name')
        if issn and issn not in unique_issns:
            run_neo4j_query(
                'MERGE (j:Journal {issn: "%(issn)s", name: "%(name)s"})' % {
                    "issn": issn,
                    "name": name,
                }
            )
            unique_issns.add(issn)


def insert_authors_to_graph(output_folder):
    unique_authors = set()
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)

        authors = obj.get("authors_parsed", [])
        for author in authors:
            if author[1] + author[0] not in unique_authors:
                run_neo4j_query(
                    'MERGE (a:Author {first_name: "%(first_name)s", last_name: "%(last_name)s"})' % {
                        "first_name": author[1].replace('"',"'"),
                        "last_name": author[0].replace('"',"'"),
                    }
                )
                unique_authors.add(author[1] + author[0])

def insert_articles_to_graph(output_folder):
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)
        query = '''
            MERGE (a:Article {article_id: "%(article_id)s"})
            ON CREATE
            SET a.title = "%(title)s",
                a.doi = %(doi)s,
                a.update_date = "%(update_date)s",
                a.journal_ref = %(journal_ref)s,
                a.type = %(type)s,
                a.reference_count = %(reference_count)s,
                a.is_referenced_by_count = %(is_referenced_by_count)s
        ''' % {
            "article_id": obj['id'],
            "title": escape(obj['title']).replace('"',"'"),
            "doi": '"' + escape(obj.get('doi')) + '"' if obj.get('doi') else 'null',
            "update_date": obj['update_date'],
            "journal_ref": '"' + escape(obj.get('journal_ref')) + '"' if obj.get('journal_ref') else 'null',
            "type": '"' + escape(obj.get('type')) + '"' if obj.get('journal_ref') else 'null',
            "reference_count": obj.get('reference_count') if obj.get('reference_count') is not None else 'null',
            "is_referenced_by_count": obj.get('is_referenced_by_count') if obj.get('is_referenced_by_count') is not None else 'null',
        }
        if obj.get('journal_issn'):
            query += '''
                WITH a
                MATCH (j:Journal {issn: "%(journal_issn)s"})
                MERGE (a)-[:PUBLISHED_IN]->(j)
            ''' % {
                "journal_issn": obj['journal_issn']
            }
        if obj.get('categories'):
            query += '''
                WITH a
                MATCH (c:Category {name: "%(name)s"})
                MERGE (a)-[:BELONGS_TO]->(c)
            ''' % {
                "name": obj['categories']
            }
        authors = []
        for author in obj['authors_parsed']:
            authors.append(
                '{first_name: "%(first_name)s", last_name: "%(last_name)s"}' % {
                    "first_name": author[1].replace('"',"'"),
                    "last_name": author[0].replace('"',"'"),
                }
            )
        query += '''
            WITH a, [%(authors)s] as authors
            UNWIND authors as author
            MATCH (au:Author {first_name: author.first_name, last_name: author.last_name})
            MERGE (a)-[:AUTHORED_BY]->(au)
        ''' % {
            "authors": ',\n'.join(authors)
        }
        run_neo4j_query(query)


def insert_article_references_to_graph(output_folder):
    for line in open(f'{output_folder}/augmented_data.json', 'r', encoding='utf-8', errors="replace"):
        if not line:
            continue
        obj = json.loads(line)
        doi = obj.get('doi')
        references = obj.get('references_doi')
        if not doi or not references:
            continue
        references_list = ', '.join([f'"{ref}"' for ref in references])
        query = '''
            WITH [%(references_list)s] as dois
            UNWIND dois as doi2
            MATCH (a1:Article {doi: "%(doi)s"}), (a2:Article {doi: doi2})
            MERGE (a1)-[:REFERENCES]->(a2)
        ''' % {
            "references_list": references_list,
            "doi": escape(doi)
        }
        run_neo4j_query(query)
        

insert_categories_to_graph_task = PythonOperator(
    task_id='insert_categories_to_graph',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_categories_to_graph,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

augment_data_task >> insert_categories_to_graph_task

insert_journals_to_graph_task = PythonOperator(
    task_id='insert_journals_to_graph',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_journals_to_graph,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_categories_to_graph_task >> insert_journals_to_graph_task

insert_authors_to_graph_task = PythonOperator(
    task_id='insert_authors_to_graph',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_authors_to_graph,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_journals_to_graph_task >> insert_authors_to_graph_task

insert_articles_to_graph_task = PythonOperator(
    task_id='insert_articles_to_graph',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_articles_to_graph,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_authors_to_graph_task >> insert_articles_to_graph_task

insert_article_references_to_graph_task = PythonOperator(
    task_id='insert_article_references_to_graph',
    dag=dag,
    trigger_rule='none_failed',
    python_callable=insert_article_references_to_graph,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

insert_articles_to_graph_task >> insert_article_references_to_graph_task
