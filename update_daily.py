from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sdk import Connection

from airflow.models import Variable  

# --- 기본 설정 ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG 정의 ---
dag = DAG(
    'update_daily',
    default_args=default_args,
    description='Daily update of Upload data',
    schedule='29 9 * * *',  # 매일 9시 20분 
    catchup=False,  # 과거 실행 생략
)

date = (datetime.now()).strftime("%Y%m%d")
aws_ip = Variable.get("AWS_IP")
# --- 방법 1: FileSensor 사용 (권장) ---
file_sensor_task = FileSensor(
    task_id='file_sensor',
    filepath=f"{date}.csv",
    fs_conn_id='fs_default',  # 파일시스템 연결 ID (기본값 사용)
    poke_interval=600,  # 30초마다 파일 확인
    timeout=300,  # 5분 후 타임아웃
    dag=dag,
)

upload_task = BashOperator(
    task_id='bash_task',
    bash_command=f"scp -i /opt/airflow/keys/cw_app.pem -o StrictHostKeyChecking=no \
        /opt/airflow/data/do_project_per_dataID_list/{date}.csv ubuntu@{aws_ip}:/home/ubuntu/cw_app/data/{date}.csv",
    dag=dag,
)

check_file_time = BashOperator(
    task_id="check_file_time",
    bash_command=f"""
    ssh -i /opt/airflow/keys/cw_app.pem -o StrictHostKeyChecking=no ubuntu@{aws_ip} \
    "stat -c '%y' /home/ubuntu/cw_app/data/{date}.csv"
    """,
    dag=dag
)

def alert_command(**context):
    date = context['task_instance'].xcom_pull(task_ids='check_file_time')
    print("-------"*444)
    print(f"File updated at {date}")

alert_task = PythonOperator(
    task_id="alert_task",
    python_callable=alert_command,
    dag=dag,
)
# --- 태스크 순서 지정 (방법 선택) ---

file_sensor_task >> upload_task >> check_file_time  >> alert_task
