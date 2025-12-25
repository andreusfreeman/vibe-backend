from fastapi import FastAPI
from google.cloud import bigquery

app = FastAPI()
client = bigquery.Client()

@app.post("/recommend")
def recommend(payload: dict):
    mood = payload["mood"]
    filters = payload.get("filters", {})

    query = """
    SELECT name, category
    FROM `your-project.vibe_analytics.drink_catalog`
    LIMIT 5
    """

    job = client.query(query)
    return [dict(row) for row in job.result()]
