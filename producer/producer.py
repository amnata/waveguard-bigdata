from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime, timezone

fake = Faker('fr_FR')

BROKER = 'localhost:29092'
TOPIC = 'transactions'

ACCOUNTS = [f'SN_{i:04d}' for i in range(1, 51)]
FRAUD_ACCOUNTS = ['SN_0042', 'SN_0007', 'SN_0013']

conf = {'bootstrap.servers': BROKER}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'[ERREUR] Livraison échouée : {err}')
    else:
        print(f'[OK] Topic={msg.topic()} | Key={msg.key().decode()} | Partition={msg.partition()} | Offset={msg.offset()}')

def generate_transaction(sender, fraud=False):
    amount = random.randint(800_000, 2_000_000) if fraud else random.randint(500, 150_000)

    return {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'sender_id': sender,
        'receiver_id': random.choice(ACCOUNTS),
        'amount_fcfa': amount,
        'transaction_type': random.choice(['P2P', 'PAIEMENT_MARCHAND', 'RETRAIT']),
        'location': random.choice(['Dakar', 'Thiès', 'Saint-Louis', 'Ziguinchor', 'Kaolack']),
        'is_flagged': fraud
    }

print('=== WaveGuard Producer démarré === Ctrl+C pour arrêter')

try:
    while True:
        is_fraud = random.random() < 0.10

        # CAS FRAUDE → BURST
        if is_fraud:
            sender = random.choice(FRAUD_ACCOUNTS)

            print(f'\n[ALERTE] Burst frauduleux pour {sender}')

            for _ in range(8):  # 8 transactions en rafale
                tx = generate_transaction(sender, fraud=True)

                producer.produce(
                    TOPIC,
                    key=sender.encode(),  # clé = partitionnement
                    value=json.dumps(tx).encode(),
                    callback=delivery_report
                )

                producer.poll(0)
                time.sleep(0.05)  # 50ms

        # CAS NORMAL
        else:
            sender = random.choice(ACCOUNTS)
            tx = generate_transaction(sender, fraud=False)

            producer.produce(
                TOPIC,
                key=sender.encode(),
                value=json.dumps(tx).encode(),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(random.uniform(0.05, 0.3))

except KeyboardInterrupt:
    print('Arrêt du producer...')
    producer.flush()