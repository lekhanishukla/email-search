## Email Search System (Kafka + OpenSearch + Redis + FastAPI)

### Run (local)
1) Start stack:
   docker-compose up --build

2) Create OpenSearch index:
   docker-compose exec api python scripts/create_index.py

3) Produce sample data:
   docker-compose exec api python -m producer.producer

4) Search:
   - Health: http://localhost:8000/health
   - Exact:  http://localhost:8000/search/email?email=user1@fortinet.com
   - Domain: http://localhost:8000/search/domain?domain=fortinet.com&limit=20
