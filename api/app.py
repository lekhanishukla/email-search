from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List

from common.opensearch_client import get_os_client
from common.config import INDEX_NAME

app = FastAPI(
    title="Email Search API",
    description="High scale email search service using OpenSearch",
    version="1.0.0",
)

os_client = get_os_client()


@app.get("/health")
def health():
    """
    Health check endpoint
    """
    return {"status": "ok"}


@app.get("/search/email")
def search_by_email(email: str):
    email = email.lower().strip()

    resp = os_client.search(
        index=INDEX_NAME,
        body={
            "size": 1,
            "query": {
                "term": {
                    "email.keyword": email
                }
            }
        }
    )

    hits = resp["hits"]["hits"]
    if not hits:
        raise HTTPException(status_code=404, detail="Email not found")

    return hits[0]["_source"]



@app.get("/search/domain")
def search_by_domain(
    domain: str,
    size: int = Query(default=50, le=100),
    cursor: Optional[str] = None,
):
    """
    Paginated search by domain using search_after.
    Cursor is the last _id from previous page.
    """

    domain = domain.lower().strip()

    query = {
        "size": size,
        "sort": [{"_id": "asc"}],
        "query": {
            "term": {
                "domain": domain
            }
        }
    }

    if cursor:
        query["search_after"] = [cursor]

    resp = os_client.search(
        index=INDEX_NAME,
        body=query,
    )

    hits = resp["hits"]["hits"]

    results = [hit["_source"] for hit in hits]

    next_cursor = None
    if hits:
        next_cursor = hits[-1]["sort"][0]

    return {
        "count": len(results),
        "results": results,
        "next_cursor": next_cursor
    }
