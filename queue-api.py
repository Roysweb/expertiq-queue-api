from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os

app = FastAPI(title="ExpertIQ Queue API")

# Hent Render-database-URL fra environment
DATABASE_URL = os.getenv("DATABASE_URL")

# ----------------------------------------------------
# Databasehjelpere
# ----------------------------------------------------
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

# Kjør init ved oppstart
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

@app.post("/add")
def add_job(job: Job):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO jobs (payload, status) VALUES (%s, %s) RETURNING id;",
                (psycopg2.extras.Json(job.payload), "pending"))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Job added", "id": new_id}

@app.get("/next")
def get_next_job():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, payload FROM jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1;
    """)
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return {"job": None}
    job_id, payload = row
    # Sett status til "processing"
    cur.execute("UPDATE jobs SET status = 'processing', updated_at = NOW() WHERE id = %s;", (job_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"job": {"id": job_id, "payload": payload}}

@app.post("/update")
def update_job(job: Job):
    if not job.id:
        raise HTTPException(status_code=400, detail="Missing job ID")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s;",
                (job.status or "done", job.id))
    conn.commit()
    cur.close()
    conn.close()
    return {"message": "Job updated", "id": job.id, "status": job.status}
