from datetime import datetime, timedelta                            

from airflow import DAG                                      
from docker.types import Mount                                    

from airflow.operators.python_operator import PythonOperator        
from airflow.operators.bash import BashOperator                    
from airflow.providers.docker.operators.docker import DockerOperator    

import subprocess                                        



# default configuration options for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,                
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,                             
    # 'retry_delay': timedelta(minutes=3),          
}



# function to run the elt_script.py
def run_elt_script():
    script_path = "/opt/airflow/elt_script/elt_script.py"           # mounting/mapping volume of elt_script.py inside docker container
    result = subprocess.run(["python", script_path], capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"ELT Script failed with error: {result.stderr}")
    else:
        print(result.stdout)



# Creating/Declaring a DAG object
dag = DAG(
    'elt_and_dbt',                            
    default_args=default_args,              
    description='An ELT workflow with dbt',  
    start_date=datetime(2024, 2, 13),       
    catchup=False,                             
)



# Setting our first task t1 (i.e To run elt_script.py from docker container)
t1 = PythonOperator(                           
    task_id='run_elt_script',                   
    python_callable=run_elt_script,            
    dag=dag,                              
)


# setting 2nd task t2 (i.e To run 'DBT' from docker container)
t2 = DockerOperator(                               
    task_id='dbt_run',                              
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',    
    command=[                                 
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh"
    ],
    auto_remove=True,                               
    docker_url="unix://var/run/docker.sock",        
    network_mode="bridge",
    mounts=[                                        
        Mount(source='/home/dhiraj/Desktop/data-pipeline-with-python-dbt-airflow/data_transformations', target='/dbt', type='bind'),
        Mount(source='/home/dhiraj/.dbt', target='/root', type='bind'),
    ],
    dag=dag                                        
)




# Task dependencies for execution of all DAG tasks in Order

t1 >> t2                    # Task (t1) is completed before Task (t2)

