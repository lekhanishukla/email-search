import asyncio
import json
import time
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, UnknownTopicOrPartitionError
from opensearchpy import helpers

from common.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_GROUP, INDEX_NAME
from common.opensearch_client import get_os_client
from common.normalize import normalize_email, is_valid_email, split_email, stable_id


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_action(payload: dict) -> dict:
    email = normalize_email(payload.get("email", ""))
    if not is_valid_email(email):
        return {}

    user, domain = split_email(email)
    _id = stable_id(email)

    doc = {
        "email": email,
        "domain": domain,
        "user": user,
        "source": payload.get("source", "unknown"),
        "first_seen_ts": payload.get("first_seen_ts") or now_iso(),
        "last_seen_ts": now_iso(),
        "count": payload.get("count", 1),
    }

    return {
        "_op_type": "index",
        "_index": INDEX_NAME,
        "_id": _id,
        "_source": doc,
    }


async def wait_for_kafka(bootstrap: str, timeout_s: int = 180):
    """
    Wait until Kafka bootstrap is reachable (metadata request succeeds).
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=bootstrap,
                group_id=None,  # no group needed for metadata probe
            )
            await consumer.start()
            # If this succeeds, broker is reachable.
            await consumer.topics()
            return
        except Exception as e:
            print(f"Waiting for Kafka... {type(e).__name__}: {e}")
            await asyncio.sleep(3)
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:
                    pass
    raise RuntimeError(f"Kafka not reachable after {timeout_s}s: {bootstrap}")


async def wait_for_topic(bootstrap: str, topic: str, timeout_s: int = 180):
    """
    Wait until topic shows up in Kafka metadata.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=bootstrap,
                group_id=None,
            )
            await consumer.start()
            topics = await consumer.topics()
            if topic in topics:
                print(f"Topic is ready: {topic}")
                return
            print(f"Waiting for topic '{topic}' to appear in metadata...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Waiting for topic... {type(e).__name__}: {e}")
            await asyncio.sleep(3)
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:
                    pass
    raise RuntimeError(f"Topic '{topic}' not visible after {timeout_s}s")


async def run_indexer():
    os_client = get_os_client()

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=2000,
    )

    print("Starting Kafka consumer...")
    await consumer.start()
    print("Indexer started... consuming from:", KAFKA_TOPIC)

    try:
        batch = []
        BATCH_SIZE = 1

        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))
                print(f"Consumed message at offset {msg.offset}")
            except Exception:
                continue

            action = build_action(payload)
            if action:
                batch.append(action)

            if len(batch) >= BATCH_SIZE:
                helpers.bulk(os_client, batch, request_timeout=60)
                batch.clear()
                await consumer.commit()
                print("Indexed batch and committed offsets")

    finally:
        await consumer.stop()


async def main():
    await wait_for_kafka(KAFKA_BOOTSTRAP)
    print("Kafka is reachable")

    await wait_for_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    # Now it's safe to start the real consumer
    while True:
        try:
            await run_indexer()
        except (KafkaConnectionError, UnknownTopicOrPartitionError) as e:
            print(f"Consumer failed, will retry in 5s: {type(e).__name__}: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error, will retry in 5s: {type(e).__name__}: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
