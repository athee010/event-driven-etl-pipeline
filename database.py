# database.py
import psycopg2
from datetime import datetime


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_last_uploaded_time(cur):
    """Get the last uploaded timestamp from audit_table"""
    cur.execute("SELECT MAX(uploaded_timestamp) FROM audit_table")
    result = cur.fetchone()
    return result[0] if result and result[0] else datetime(1970, 1, 1)


def insert_audit_records(cur, batch_id, files):
    """Insert new files into audit_table as 'processing'"""
    for key, ts in files:
        cur.execute("""
            INSERT INTO audit_table (
                filename, uploaded_timestamp, batch_id, status
            ) VALUES (%s, %s, %s, %s)
        """, (key, ts, batch_id, "processing"))


def update_status(cur, batch_id):
    """Update status of batch to 'success'"""
    cur.execute("""
        UPDATE audit_table
        SET status = 'success', processed_timestamp = CURRENT_TIMESTAMP
        WHERE batch_id = %s
    """, (batch_id,))
