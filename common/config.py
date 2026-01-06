import os

def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default

KAFKA_BOOTSTRAP = env_str("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = env_str("KAFKA_TOPIC", "email_findings")
KAFKA_GROUP = env_str("KAFKA_GROUP", "email-indexers_debug")

OPENSEARCH_HOST = env_str("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = env_int("OPENSEARCH_PORT", 9200)
INDEX_NAME = env_str("INDEX_NAME", "emails_v1")

REDIS_HOST = env_str("REDIS_HOST", "localhost")
REDIS_PORT = env_int("REDIS_PORT", 6379)
