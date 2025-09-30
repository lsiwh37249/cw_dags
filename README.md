yaml 파일
```
 77     - /home/kim/app/cw_app/data:/opt/airflow/data
 78     - /mnt/c/Users/김령래/Desktop/cw_app:/opt/airflow/data
 79     - ${AIRFLOW_PROJ_DIR:-.}/ssh:/opt/airflow/.ssh
 80     - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
 81     - ${AIRFLOW_PROJ_DIR:-.}/keys:/opt/airflow/keys
 82     - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
 83     - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
 84     - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
 85     - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
```
