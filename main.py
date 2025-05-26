# main.py
import config
from database import get_db_connection, get_last_uploaded_time, insert_audit_records, update_status
from s3_utils import get_new_files
from processor import process_files
from datetime import datetime


def generate_batch_id():
    """Generate a unique batch ID based on current time"""
    return f"batch_{datetime.now().strftime('%Y%m%d%H')}"


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Step 1: Get last uploaded time
        last_time = get_last_uploaded_time(cur)
        print(f"Last uploaded timestamp: {last_time}")

        # Step 2: Get new files from S3
        s3 = boto3.client("s3")
        new_files = get_new_files(s3, config.BUCKET, config.PREFIX, last_time)

        if not new_files:
            print("No new files found.")
            return

        # Step 3: Generate batch ID
        batch_id = generate_batch_id()
        print(f"Batch ID: {batch_id}")

        # Step 4: Insert records into audit table
        insert_audit_records(cur, batch_id, new_files)
        conn.commit()

        # Step 5: Process files (you write this part!)
        success = process_files(new_files)

        if success:
            # Step 6: Update status to success
            update_status(cur, batch_id)
            conn.commit()
            print("Status updated to success.")

    except Exception as e:
        print("Error:", str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
