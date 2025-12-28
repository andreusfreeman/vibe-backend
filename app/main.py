from fastapi import FastAPI
from google.cloud import bigquery

app = FastAPI()
client = bigquery.Client()

BASELINE_QUERY = """
DECLARE target_mood STRING DEFAULT @mood;
DECLARE prefer_sweet FLOAT64 DEFAULT @sweetness;
DECLARE max_alcohol FLOAT64 DEFAULT @max_alcohol;

WITH scored AS (
  SELECT
    d.*,
    1 - ABS(d.sweetness - prefer_sweet) AS sweet_score,
    IF(d.strength <= max_alcohol, 1, 0) AS strength_score,
    CASE
      WHEN target_mood = 'chill' AND d.strength <= 15 THEN 1
      WHEN target_mood = 'party' AND d.strength >= 12 THEN 1
      ELSE 0.5
    END AS mood_score
  FROM `alcorec.vibe_analytics.drink_catalog` d
  WHERE d.strength <= max_alcohol
)
SELECT *
FROM scored
ORDER BY (sweet_score * 0.4 + mood_score * 0.4 + strength_score * 0.2) DESC
LIMIT 5;
"""


@app.post("/recommend")
def recommend(payload: dict):
    context = payload.get("context", {})
    prefs = context.get("preferences", {})

    mood = context.get("mood", "chill")
    sweetness = prefs.get("sweetness", 0.5)
    max_alcohol = prefs.get("maxAlcohol", 15)

    params = [
        bigquery.ScalarQueryParameter("mood", "STRING", mood),
        bigquery.ScalarQueryParameter("sweetness", "FLOAT64", sweetness),
        bigquery.ScalarQueryParameter("max_alcohol", "FLOAT64", max_alcohol),
    ]

    job = client.query(
        BASELINE_QUERY,
        job_config=bigquery.QueryJobConfig(query_parameters=params)
    )

    return [dict(row) for row in job.result()]
