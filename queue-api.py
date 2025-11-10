from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import os
import json

app = FastAPI(title="ExpertIQ Queue API")

# ----------------------------------------------------
# Databaseforbindelse
# ----------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            payload JSONB,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Initier tabell ved oppstart
init_db()

# ----------------------------------------------------
# Datamodeller
# ----------------------------------------------------
class Job(BaseModel):
    id: int | None = None
    payload: dict | None = None
    status: str | None = None

# ----------------------------------------------------
# API-ruter
# ----------------------------------------------------
@app.get("/")
def root():
    return {"message": "ExpertIQ Queue API is live!"}

@app.get("/health")
def health():
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ----------------------------------------------------
# Legg til ny jobb
# ----------------------------------------------------
@app.post("/add")
def add_job(job: Job):
    """
    Legg til ny jobb i køen.
    Payload bør inneholde:
    {
        "box_id": "Lofoten-01",
        "file_url": "https://simpledas-data.s3.eu-central-1.wasabisys.com/raw/Lofoten-01/063005_20251110T113155.hdf5",
        "timestamp": "2025-11-10T11:31:55Z"
    }
    """
    if not job.payload:
        raise HTTPException(status_code=400, detail="Missing payload")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO jobs (payload, status) VALUES (%s, %s) RETURNING id;",
        (psycopg2.extras.Json(job.payload), "pending")
    )
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Job added", "id": new_id}

# ----------------------------------------------------
# Hent neste jobb i køen
# ----------------------------------------------------
@app.get("/next")
def get_next_job():
    """
    Henter neste pending jobb, markerer den som 'processing'
    og returnerer payload + metadata til n8n.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("""
        SELECT id, payload
        FROM jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1;
    """)
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return {"job": None}

    job_id = row["id"]
    payload = row["payload"]

    # Marker som processing
    cur.execute(
        "UPDATE jobs SET status = 'processing', updated_at = NOW() WHERE id = %s;",
        (job_id,)
    )
    conn.commit()

    cur.close()
    conn.close()

    # Parse payload for felt
    box_id = payload.get("box_id", "Unknown")
    file_url = payload.get("file_url")
    timestamp = payload.get("timestamp")

    return {
        "job": {
            "id": job_id,
            "box_id": box_id,
            "file_url": file_url,
            "timestamp": timestamp,
            "status": "processing"
        }
    }

# ----------------------------------------------------
# Oppdater status for jobb
# ----------------------------------------------------
@app.post("/update")
def update_job(job: Job):
    if not job.id:
        raise HTTPException(status_code=400, detail="Missing job ID")

    new_status = job.status or "done"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s;",
        (new_status, job.id)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Job updated", "id": job.id, "status": new_status}
