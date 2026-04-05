"""
Database module for College Data Enrichment CRM.
Handles SQLite operations for jobs, colleges, and logs.
"""

import sqlite3
import os
import uuid
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'enrichment.db')


def get_db_connection():
    """Get a new database connection with row factory."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Initialize database tables."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            total_rows INTEGER DEFAULT 0,
            processed_rows INTEGER DEFAULT 0,
            active_count INTEGER DEFAULT 0,
            inactive_count INTEGER DEFAULT 0,
            not_found_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS colleges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            college_name TEXT,
            college_type TEXT,
            state TEXT,
            district TEXT,
            original_website TEXT,
            found_website TEXT,
            extracted_phone TEXT,
            extracted_email TEXT,
            extracted_principal TEXT,
            status TEXT DEFAULT 'Pending',
            search_method TEXT,
            error_log TEXT,
            processed_at TIMESTAMP,
            FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level TEXT DEFAULT 'INFO',
            message TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_colleges_job_id ON colleges(job_id);
        CREATE INDEX IF NOT EXISTS idx_logs_job_id ON logs(job_id);
        CREATE INDEX IF NOT EXISTS idx_colleges_status ON colleges(status);
    """)

    conn.commit()
    conn.close()


# ─── Job Operations ───────────────────────────────────────────────

def create_job(filename, total_rows):
    """Create a new processing job. Returns job_id."""
    job_id = str(uuid.uuid4())[:8]
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO jobs (id, filename, status, total_rows) VALUES (?, ?, 'pending', ?)",
        (job_id, filename, total_rows)
    )
    conn.commit()
    conn.close()
    return job_id


def update_job_status(job_id, status, error_message=None):
    """Update job status."""
    conn = get_db_connection()
    if status == 'completed':
        conn.execute(
            "UPDATE jobs SET status=?, completed_at=?, error_message=? WHERE id=?",
            (status, datetime.now().isoformat(), error_message, job_id)
        )
    else:
        conn.execute(
            "UPDATE jobs SET status=?, error_message=? WHERE id=?",
            (status, error_message, job_id)
        )
    conn.commit()
    conn.close()


def update_job_progress(job_id, processed_rows, active_count, inactive_count, not_found_count):
    """Update job progress counters."""
    conn = get_db_connection()
    conn.execute(
        """UPDATE jobs SET processed_rows=?, active_count=?, inactive_count=?, not_found_count=?
           WHERE id=?""",
        (processed_rows, active_count, inactive_count, not_found_count, job_id)
    )
    conn.commit()
    conn.close()


def get_job(job_id):
    """Get job details."""
    conn = get_db_connection()
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    conn.close()
    return dict(job) if job else None


def get_all_jobs():
    """Get all jobs ordered by creation date."""
    conn = get_db_connection()
    jobs = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(j) for j in jobs]


# ─── College Operations ──────────────────────────────────────────

def insert_colleges_batch(job_id, colleges_data):
    """Insert a batch of college rows from uploaded file."""
    conn = get_db_connection()
    for idx, row in enumerate(colleges_data):
        conn.execute(
            """INSERT INTO colleges (job_id, row_index, college_name, college_type, state, district,
               original_website, extracted_phone, extracted_email, extracted_principal, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending')""",
            (job_id, idx,
             row.get('College Name', ''),
             row.get('College Type', ''),
             row.get('State', ''),
             row.get('District', ''),
             row.get('Website', ''),
             row.get('Contact Number', ''),
             row.get('Mail ID', ''),
             row.get('Principal Name', ''))
        )
    conn.commit()
    conn.close()


def update_college_result(college_id, phone, email, principal, status, search_method, found_website=None, error_log=None):
    """Update a college row with enrichment results."""
    conn = get_db_connection()
    conn.execute(
        """UPDATE colleges SET extracted_phone=?, extracted_email=?, extracted_principal=?,
           status=?, search_method=?, found_website=?, error_log=?, processed_at=?
           WHERE id=?""",
        (phone, email, principal, status, search_method, found_website, error_log,
         datetime.now().isoformat(), college_id)
    )
    conn.commit()
    conn.close()


def get_pending_colleges(job_id, limit=None):
    """Get unprocessed colleges for a job."""
    conn = get_db_connection()
    query = "SELECT * FROM colleges WHERE job_id=? AND status='Pending' ORDER BY row_index"
    if limit:
        query += f" LIMIT {limit}"
    colleges = conn.execute(query, (job_id,)).fetchall()
    conn.close()
    return [dict(c) for c in colleges]


def get_colleges_by_job(job_id, status_filter=None, search_query=None):
    """Get all colleges for a job with optional filters."""
    conn = get_db_connection()
    query = "SELECT * FROM colleges WHERE job_id=?"
    params = [job_id]

    if status_filter and status_filter != 'all':
        query += " AND status=?"
        params.append(status_filter)

    if search_query:
        query += " AND (college_name LIKE ? OR district LIKE ? OR state LIKE ?)"
        like = f"%{search_query}%"
        params.extend([like, like, like])

    query += " ORDER BY row_index"
    colleges = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(c) for c in colleges]


def get_college_count_by_status(job_id):
    """Get count of colleges by status for a job."""
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT status, COUNT(*) as count FROM colleges WHERE job_id=? GROUP BY status",
        (job_id,)
    ).fetchall()
    conn.close()
    return {row['status']: row['count'] for row in rows}


# ─── Log Operations ──────────────────────────────────────────────

def add_log(job_id, level, message):
    """Add a log entry."""
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO logs (job_id, level, message, timestamp) VALUES (?, ?, ?, ?)",
        (job_id, level, message, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def get_logs(job_id, limit=100):
    """Get recent logs for a job."""
    conn = get_db_connection()
    logs = conn.execute(
        "SELECT * FROM logs WHERE job_id=? ORDER BY timestamp DESC LIMIT ?",
        (job_id, limit)
    ).fetchall()
    conn.close()
    return [dict(l) for l in logs]


# Initialize on import
init_db()
