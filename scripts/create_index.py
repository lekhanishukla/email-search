from common.opensearch_client import get_os_client
from common.config import INDEX_NAME

INDEX_BODY = {
    "settings": {
        "number_of_shards": 6,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "email": {"type": "keyword"},
            "domain": {"type": "keyword"},
            "user": {"type": "keyword"},
            "source": {"type": "keyword"},
            "first_seen_ts": {"type": "date"},
            "last_seen_ts": {"type": "date"},
            "count": {"type": "long"}
        }
    }
}

def main():
    client = get_os_client()
    if client.indices.exists(INDEX_NAME):
        print(f"Index already exists: {INDEX_NAME}")
        return
    client.indices.create(index=INDEX_NAME, body=INDEX_BODY)
    print(f"Created index: {INDEX_NAME}")

if __name__ == "__main__":
    main()
