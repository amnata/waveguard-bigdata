import time, json
from confluent_kafka import Consumer

BROKER = 'localhost:29092'
METRICS_FILE = '/tmp/waveguard_metrics.json'

conf = {
    'bootstrap.servers': BROKER,
    'group.id': 'metrics-exporter',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe(['fraud-alerts'])

velocity_count = 0
volume_count = 0
fraudsters = {}

print('[MetricsExporter] Démarré...')

while True:
    try:
        msgs = consumer.consume(num_messages=100, timeout=5.0)
        for msg in msgs:
            if msg.error():
                continue
            try:
                d = json.loads(msg.value().decode())
                ft = d.get('fraud_type', '')
                if ft == 'VELOCITY_FRAUD':
                    velocity_count += 1
                elif ft == 'VOLUME_FRAUD':
                    volume_count += 1
                sid = d.get('sender_id', '')
                if sid:
                    fraudsters[sid] = fraudsters.get(sid, 0) + 1
            except Exception:
                pass

        top = max(fraudsters, key=fraudsters.get) if fraudsters else 'N/A'
        metrics = {
            'timestamp': time.time(),
            'velocity_alerts': velocity_count,
            'volume_alerts': volume_count,
            'top_fraudster': top,
            'top_count': fraudsters.get(top, 0)
        }

        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f)

        print(f'[OK] {metrics}')

    except Exception as e:
        print(f'[ERREUR] {e}')

    time.sleep(15)
