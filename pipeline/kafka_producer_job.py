import csv
import json
from kafka import KafkaProducer

# Configuração do Producer apontando para a rede interna do Docker (mybridge)
producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Caminho interno absoluto do Airflow para a pasta dags
csv_path = '/opt/airflow/dags/airquality.csv'
topico = 'sensor_raw'

print(f"Iniciando a leitura do CSV em: {csv_path}")

with open(csv_path, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter=',')
    
    for row in reader:
        payload = {
            "Date": row.get("Date"),
            "Time": row.get("Time"),
            "CO": row.get("CO"),
            "NO2": row.get("NO2"),
            "Temperature": row.get("Temperature"),
            "Relative_Humidity": row.get("Relative_Humidity"),
            "Absolute_Humidity": row.get("Absolute_Humidity")
        }
        producer.send(topico, value=payload)
        
    producer.flush()

print("Todos os registros enviados com sucesso para o Kafka!")
