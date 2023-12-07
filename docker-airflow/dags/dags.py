from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(dag_id="kataflight",
         start_date=datetime(2023, 12, 7),
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

    cleanup = SSHOperator(
        task_id="cleanup",
        ssh_conn_id='acil-ssh',
        command="""
                # Function to wait for a container to exit
                wait_for_exit() {
                    local container_name=$1
                    until "C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" ps -a --filter "name=${container_name}" --filter "status=exited" | grep "${container_name}"; do
                        echo "Waiting for ${container_name} to exit..."
                        sleep 5
                    done
                }

                # Wait for both containers to exit
                wait_for_exit flight-extractor-container
                wait_for_exit flight-transformer-container
                wait_for_exit flight-analyse-container
                
                sleep 10


                # Remove the containers
                "C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-extractor-container
                "C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-transformer-container
                "C:\\Program Files\\Docker\\Docker\\resources\\bin\\docker.exe" rm flight-analyse-container
            """,
        cmd_timeout=600,
        dag=dag
    )

    # Définir les dépendances entre les tâches
    extract_task >> transform_task >> collect_task >> cleanup
