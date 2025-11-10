from fastapi import FastAPI, Request
import psycopg2, os, json
from psycopg2.extras import RealDictCursor

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL, sslmode="require", cursor_factory=RealDictCursor)


@app.post("/add")
async def add_job(request: Request):
    data = await request.json()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queue_das (file_url, box_id, status)
            VALUES (%s, %s, 'pending')
            RETURNING id;
            """,
            (data.get("file_url"), data.get("box_id", "unknown")),
        )
        job_id = cur.fetchone()["id"]
    return {"status": "ok", "id": job_id}


@app.get("/next")
async def get_next_job():
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE queue_das
            SET status='processing'
            WHERE id = (
                SELECT id FROM queue_das WHERE status='pending'
                ORDER BY id ASC
                LIMIT 1
            )
            RETURNING id, file_url, box_id;
            """
        )
        job = cur.fetchone()
    return {"job": job}


@app.post("/update")
async def update_job(request: Request):
    data = await request.json()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE queue_das
            SET status=%s, updated_at=NOW(), last_error=%s
            WHERE id=%s;
            """,
            (data.get("status", "done"), data.get("error", None), data["id"]),
        )
    return {"status": "updated", "id": data["id"]}


@app.get("/stats")
async def get_stats():
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE status='pending') AS pending,
              COUNT(*) FILTER (WHERE status='processing') AS processing,
              COUNT(*) FILTER (WHERE status='done') AS done,
              COUNT(*) FILTER (WHERE status='failed') AS failed
            FROM queue_das;
        """)
        stats = cur.fetchone()
    return {"queue_stats": stats}
