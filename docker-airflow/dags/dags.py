from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


with DAG(dag_id="kataflight",
         start_date=datetime(2023, 12, 6),
         schedule_interval="0 */2 * * *") as dag:


    extract_task = SSHOperator(
        task_id="extract",
        ssh_conn_id='acil-ssh',
        command='"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" run -e CURRENT_IP=172.17.0.2 --name flight-extract-container flight-extract',
        cmd_timeout=240,
        dag=dag
    )

    transform_task = SSHOperator(
        task_id="transform",
        ssh_conn_id='acil-ssh',
        command='"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" run -e CURRENT_IP=172.17.0.2 --name flight-transform-container flight-transform',
        cmd_timeout=240,
        dag=dag
    )

    collect_task = SSHOperator(
        task_id="analyse",
        ssh_conn_id='acil-ssh',
        command='"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" run -e CURRENT_IP=172.17.0.2 --name flight-collect-container flight-collect',
        cmd_timeout=240,
        dag=dag
    )


    # Définir les dépendances entre les tâches
    extract_task >> transform_task >> collect_task

    cleanup = SSHOperator(
        task_id="cleanup",
        ssh_conn_id='acil-ssh',
        command="""
            # Cleanup all containers
            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" stop flight-extractor-container || true
            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" stop flight-transformer-container || true
            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" stop flight-collector-container || true

            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-extractor-container || true
            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-transformer-container || true
            '"C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-collector-container || true
        """,
        dag=dag
    )


    # Définir les dépendances entre les tâches
    extract_task >> transform_task >> collect_task >> cleanup
