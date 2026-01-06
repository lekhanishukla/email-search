import asyncio
import json
import random
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer

from common.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC
from common.normalize import normalize_email, is_valid_email

DOMAINS = ["fortinet.com", "gmail.com", "yahoo.com", "example.org", "company.io"]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def random_email():
    user = "user" + str(random.randint(1, 5_000_00))
    domain = random.choice(DOMAINS)
    return f"{user}@{domain}"

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for _ in range(5000):
            email = normalize_email(random_email())
            if not is_valid_email(email):
                continue

            payload = {
                "email": email,
                "source": "worker-sim",
                "first_seen_ts": now_iso()
            }
            await producer.send_and_wait(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"))
        print("Sent 5000 messages to Kafka topic:", KAFKA_TOPIC)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
