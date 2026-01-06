from opensearchpy import OpenSearch

INDEX_NAME = "email_index"

def search_by_domain(
    client: OpenSearch,
    domain: str,
    page_size: int = 50,
    search_after: list | None = None
):
    query = {
        "size": page_size,
        "sort": [{"_id": "asc"}],
        "query": {
            "term": {"domain": domain}
        }
    }

    if search_after:
        query["search_after"] = search_after

    resp = client.search(
        index=INDEX_NAME,
        body=query
    )

    hits = resp["hits"]["hits"]

    next_cursor = None
    if hits:
        next_cursor = hits[-1]["sort"]

    return {
        "results": [h["_source"] for h in hits],
        "next_cursor": next_cursor
    }
