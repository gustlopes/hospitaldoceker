import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'hospital_eventos'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Conectado ao Kafka em {KAFKA_BROKER}. Enviando eventos para o tópico '{KAFKA_TOPIC}'...")

pacientes = [f'PID_{100+i}' for i in range(10)]
alas = ['Cardiologia', 'Ortopedia', 'Geral', 'Pediatria']

def gerar_evento_admissao(paciente_id):
    return {
        'paciente_id': paciente_id,
        'evento_tipo': 'paciente_admitido',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'metadata': {'ala': random.choice(alas)}
    }

def gerar_evento_exame(paciente_id):
    return {
        'paciente_id': paciente_id,
        'evento_tipo': 'solicitacao_exame',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'metadata': {'exame_id': f'EX_{random.randint(1000, 9999)}'}
    }

def gerar_evento_resultado(paciente_id, exame_id):
    return {
        'paciente_id': paciente_id,
        'evento_tipo': 'resultado_exame_pronto',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'metadata': {'exame_id': exame_id}
    }
    
def gerar_evento_alta(paciente_id):
    return {
        'paciente_id': paciente_id,
        'evento_tipo': 'leito_liberado',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'metadata': {'motivo': 'alta_medica'}
    }

try:
    while True:
        paciente = random.choice(pacientes)
        
        admissao_event = gerar_evento_admissao(paciente)
        print(f"Enviando evento: {admissao_event}")
        producer.send(KAFKA_TOPIC, value=admissao_event, key=paciente.encode('utf-8'))
        time.sleep(random.uniform(1, 3))
        
        exame_event = gerar_evento_exame(paciente)
        exame_id = exame_event['metadata']['exame_id']
        print(f"Enviando evento: {exame_event}")
        producer.send(KAFKA_TOPIC, value=exame_event, key=paciente.encode('utf-8'))
        
        time.sleep(random.uniform(3, 8))
        
        resultado_event = gerar_evento_resultado(paciente, exame_id)
        print(f"Enviando evento: {resultado_event}")
        producer.send(KAFKA_TOPIC, value=resultado_event, key=paciente.encode('utf-8'))
        time.sleep(random.uniform(1, 3))

        alta_event = gerar_evento_alta(paciente)
        print(f"Enviando evento: {alta_event}")
        producer.send(KAFKA_TOPIC, value=alta_event, key=paciente.encode('utf-8'))

        producer.flush()
        print("--- Ciclo para um paciente concluído, aguardando... ---")
        time.sleep(5)

except KeyboardInterrupt:
    print("Produtor interrompido.")
finally:
    producer.close()