from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import pendulum
from datetime import timedelta
import requests
import json  
from google.cloud import storage
from datetime import datetime
import os
import pandas as pd
# 서울 시간대 정의
seoul_tz = pendulum.timezone("Asia/Seoul")

# --- 기본 설정 ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 9, 8, tz=seoul_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG 정의 ---
dag = DAG(
    'update_daily',
    default_args=default_args,
    description='Daily update of Upload data',
    schedule='35 09 * * *',  # 매일 9시 29분 
    catchup=False,  # 과거 실행 생략
)

date = (datetime.now()).strftime("%Y%m%d")
aws_ip = Variable.get("AWS_IP")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")

file_download_task = BashOperator(
    task_id='file_download_task',
    bash_command=f"""
    export PATH=/opt/airflow/google-cloud-sdk/bin:$PATH
    latest_file=$(gsutil ls -l gs://{GCS_BUCKET_NAME}/cw/07-mbc-bq/mbc_share_data/ \
        | grep -v TOTAL \
        | sort -k2 -r \
        | head -n 1 \
        | awk '{{print $3}}')
    
    echo "최신 파일: $latest_file"

    gsutil cp "$latest_file" /mnt/c/Users/김령래/Desktop/cw_app/do_project_per_dataID_list/{date}.csv
    """,
    dag=dag,
)
# --- FileSensor ---
file_sensor_task = FileSensor(
    task_id='file_sensor',
    filepath=f"/mnt/c/Users/김령래/Desktop/cw_app/do_project_per_dataID_list/{date}.csv",
    fs_conn_id='fs_default', # 파일시스템 연결 ID
    poke_interval=600, 
    timeout=300, 
    dag=dag,
)

def remove_project_ids_command(date):
    import pandas as pd
    df = pd.read_csv(f"/mnt/c/Users/김령래/Desktop/cw_app/do_project_per_dataID_list/{date}.csv")
    project_ids = [26636, 26654, 26655, 26656, 26658,  26659, 26661, 26662, 26663, 26664, 26665, 26667]
    df = df[~df['project_id'].isin(project_ids)]
    df.to_csv(f"{date}.csv", index=False)

# --- Alert Task --- 

def task_alert(context):
    WEBHOOK_URL = Variable.get("WEBHOOK_URL")
    ti = context.get('task_instance')
    dag = context.get('dag')
    dag_id = dag.dag_id if dag else "Unknown_DAG"
    task_id = ti.task_id if ti else "Unknown_Task"
    state = ti.state if ti else "Unknown_State"
    exec_date = context.get('logical_date', 'N/A')
    log_url = getattr(ti, 'log_url', 'N/A')

    state_parts = state.split('.') if state else ['unknown']
    state_display = state_parts[1] if len(state_parts) > 1 else state

    exec_date_parts = str(exec_date).split(' ') if exec_date else ['N/A']
    exec_date_display = exec_date_parts[0] if exec_date_parts[0] else 'N/A'

    message = {
        "text": f"📊 DAG 알림\nDAG: {dag_id}\nTask: {task_id}\n상태: {state_display}\n실행일: {exec_date_display}\nLog: {log_url}"
    }

    response = requests.post(
        WEBHOOK_URL,
        json=message,
        headers={"Content-Type": "application/json; charset=UTF-8"}
    )
    print(f"[DEBUG] Webhook response: {response.status_code} {response.text}")

    if response.status_code != 200:
        print(f"Failed to send alert: {response.status_code} - {response.text}")
    else:
        print(f"Alert sent successfully: {response.status_code}")


remove_project_ids_task = PythonOperator(
    task_id='remove_project_ids',
    python_callable=remove_project_ids_command,
    op_kwargs={'date': date},
    dag=dag,
)

# --- Upload Task ---
upload_task = BashOperator(
    task_id='upload_task',
    bash_command=f"scp -i /home/kim/key/cw_app.pem -o StrictHostKeyChecking=no \
        /mnt/c/Users/김령래/Desktop/cw_app/do_project_per_dataID_list/{date}.csv ubuntu@{aws_ip}:/home/ubuntu/cw_app/data/{date}.csv",
    dag=dag,
    on_success_callback=task_alert,
    on_failure_callback=task_alert,
    
)

# --- Check File Time ---
check_file_time = BashOperator(
    task_id="check_file_time",
    bash_command=f"""
    ssh -i /home/kim/key/cw_app.pem -o StrictHostKeyChecking=no ubuntu@{aws_ip} \
    "stat -c '%y' /home/ubuntu/cw_app/data/{date}.csv"
    """,
    dag=dag
)

   
# --- 태스크 순서 지정 ---
file_download_task >> file_sensor_task >> remove_project_ids_task >> upload_task >> check_file_time 
