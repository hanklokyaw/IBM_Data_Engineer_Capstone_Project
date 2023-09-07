# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# Task 1.1
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'JOhn Doe',
    'start_date': days_ago(0),
    'email': ['john@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1.2
# defining the DAG
# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL Toll Data',
    schedule_interval=timedelta(days=1),
)

# Task 1.3
# define unzip tasks
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/tolldata.tgz -C /home/project/tolldata/',
    dag=dag,
)

# Task 1.4
# define extract task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/tolldata/vehicle-data.csv > /home/project/tolldata/csv_data.csv',
    dag=dag,
)

# Task 1.5
# define extract task from tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d$\'\\t\' -f5,6,7 /home/project/tolldata/tollplaza-data.tsv > /home/project/tolldata/tsv_data.csv',
    dag=dag,
)

# Task 1.6
# define extract data from fixed width task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d' ' -f9,10 /home/project/tolldata/payment-data.txt > /home/project/tolldata/fixed_width_data.csv',
    dag=dag,
)

# Task 1.7
# define consolidate_data task
consolidate_task = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d',' /home/project/tolldata/csv_data.csv \
                            /home/project/tolldata/tsv_data.csv \
                            /home/project/tolldata/fixed_width_data.csv |\
                            awk -F',' '{print $1,$2,$3,$4,$5,$6,$7,$8,$9}' >\
                            /home/project/tolldata/extracted_data.csv",
    dag=dag
)

# Task 1.8
# Define transform_data task
transform_task = BashOperator(
    task_id='transform_data',
    bash_command="awk 'BEGIN {FS=OFS=\",\"} \
                {$4 = toupper($4)} 1' \
                /home/project/tolldata/extracted_data.csv > \
                /home/project/tolldata/transformed_data.csv",
    dag=dag
)

# task pipeline
unzip_data >> extract_data_from_csv >> \
    extract_data_from_tsv >> extract_data_from_fixed_width >> \
    consolidate_task >> transform_task