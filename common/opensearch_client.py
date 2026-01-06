from opensearchpy import OpenSearch
from .config import OPENSEARCH_HOST, OPENSEARCH_PORT

def get_os_client() -> OpenSearch:
    return OpenSearch([{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}])
