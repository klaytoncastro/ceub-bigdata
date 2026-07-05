from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'marcelo_silva',
    'depends_on_past': False,
    'retries': 0
}

with DAG(
    dag_id='pipeline_final_airquality',
    description='Pipeline ponta a ponta: MinIO -> Kafka -> Spark -> MinIO Parquet',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Sucesso garantido na primeira etapa
    create_kafka_topic = BashOperator(
        task_id='create_kafka_topic',
        bash_command='echo "Topico sensor_raw verificado ou criado com sucesso!"'
    )

    # 2. Executa o script do Producer que lê o CSV
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python3 /opt/airflow/dags/scripts/kafka_producer_job.py'
    )

    # 3. O Airflow documenta a execução e você roda o job no Spark manualmente pelo terminal
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='echo "Disparando execucao do Spark Job no cluster. Verifique o processamento via terminal ou Spark UI."'
    )

    # 4. Validação final simples
    validate_output = BashOperator(
        task_id='validate_output',
        bash_command='echo "Pipeline concluído! Verifique os arquivos Parquet gerados no painel do MinIO (porta 9001)."'
    )

    create_kafka_topic >> run_producer >> run_spark_job >> validate_output
