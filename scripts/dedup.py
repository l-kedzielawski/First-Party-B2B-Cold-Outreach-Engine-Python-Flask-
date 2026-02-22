#!/usr/bin/env python3
import sqlite3
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(filename='dedup_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def deduplicate_leads(db_path):
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Log the start of the process
        logging.info(f"Starting deduplication process for {db_path}")

        # Count original records
        c.execute("SELECT COUNT(*) FROM leads")
        original_count = c.fetchone()[0]
        logging.info(f"Original leads count: {original_count}")

        # Create a temp table to hold unique emails (keeping the first rowid for each email)
        c.execute('''CREATE TABLE IF NOT EXISTS leads_temp
                     (email TEXT PRIMARY KEY, first_name TEXT, status TEXT, timestamp TEXT,
                      hash TEXT, opened_timestamp TEXT, email_sent INTEGER, interact_count INTEGER,
                      last_interact_timestamp TEXT, sent_template TEXT, notes TEXT)''')

        c.execute('''INSERT INTO leads_temp (email, first_name, status, timestamp, hash, opened_timestamp,
                      email_sent, interact_count, last_interact_timestamp, sent_template, notes)
                     SELECT email, first_name, status, timestamp, hash, opened_timestamp,
                            email_sent, interact_count, last_interact_timestamp, sent_template, notes
                     FROM leads
                     GROUP BY email''')

        # Count deduplicated records
        c.execute("SELECT COUNT(*) FROM leads_temp")
        temp_count = c.fetchone()[0]
        logging.info(f"Deduplicated leads count: {temp_count}")

        # Backup lead_details before processing
        c.execute('''CREATE TABLE IF NOT EXISTS lead_details_backup
                     AS SELECT * FROM lead_details''')
        logging.info("Created backup of lead_details as lead_details_backup")

        # Update lead_details to point to deduplicated emails
        c.execute('''DELETE FROM lead_details
                     WHERE email NOT IN (SELECT email FROM leads_temp)''')
        c.execute('''UPDATE lead_details
                     SET email = (SELECT email FROM leads_temp WHERE leads_temp.email = lead_details.email)''')
        logging.info("Updated lead_details to match deduplicated emails")

        # Replace the original leads table
        c.execute('DROP TABLE leads')
        c.execute('ALTER TABLE leads_temp RENAME TO leads')
        logging.info("Replaced leads table with deduplicated data")

        conn.commit()
        logging.info(f"Deduplication completed successfully for {db_path}")
        print(f"Deduplication completed. Check dedup_log.log for details. Removed {original_count - temp_count} duplicate records.")

    except sqlite3.IntegrityError as e:
        logging.error(f"IntegrityError during deduplication: {e}")
        conn.rollback()
        print(f"Error: {e}. Deduplication failed. Check dedup_log.log for details.")
    except Exception as e:
        logging.error(f"Unexpected error during deduplication: {e}")
        conn.rollback()
        print(f"Error: {e}. Deduplication failed. Check dedup_log.log for details.")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    db_path = 'polska.db'  # Matches your database path
    deduplicate_leads(db_path)