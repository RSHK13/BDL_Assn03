from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random

base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
year = 2023  
num_data_files = 3  
url = base_url + str(year) + "/"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'noaa_data_pipeline',
    default_args=default_args,
    description='Pipeline for fetching and processing NOAA data',
    schedule_interval=timedelta(days=1),
)

fetch_csv_links = BashOperator(
        task_id='fetch_csv_links',
        bash_command=f'curl -s {url} | grep -Eo "(http|https)://[a-zA-Z0-9./?=_-]*" | grep -E "\.(csv)$" > links.txt' 
        dag = dag
    )

def select_random_data_files():
    num_files = 5
    total_links = []
    with open('links.txt','r') as files:
        for file in files:
           total_links.append(file)
    links = random.sample(total_links,num_files)
    return links

def zip_csvs():
    csv_dir = os.path.expanduser('~/csv_files/')  
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    
    with ZipFile('csv_archive.zip','w') as zip_file:
         for file in csv_files:
             zip_file.write(os.path.join(csv_dir, file))

fetch_csv_files = BashOperator(
    task_id='fetch_csv_files',
    bash_command='curl -L -o ~/csv_files/{{ ti.xcom_pull(task_ids="select_random_data_files")[0] | basename }} {% for link in task_instance.xcom_pull(task_ids="select_random_data_files") %} {{ link }} {% endfor %}'
    dag = dag
)

select_files = PythonOperator(
    task_id='select_files',
    python_callable=select_random_data_files,
    op_args=[num_data_files],
    provide_context=False,
    dag=dag,
)

zip_csv_files = PythonOperator(
    task_id='zip_csv_files',
    python_callable=zip_csvs,
    dag = dag
)

def clean_up_function():
    selected_files = Variable.get("selected_files")
    for file in selected_files:
        os.remove(file)




# clean_up_task = PythonOperator(
#     task_id='clean_up',
#     python_callable=clean_up_function,
#     op_args=[],
#     provide_context=False,
#     dag=dag,
# )

fetch_csv_files >> select_files >> zip_csv_files 