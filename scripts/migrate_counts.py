#!/usr/bin/env python3
import sqlite3
import os

# Path to your database (adjust as needed)
db_path = '/home/luke2100/domains/nma.themysticaroma.com/private_html/your_database.db'

conn = sqlite3.connect(db_path)
c = conn.cursor()

# Check if open_count exists and migrate to interact_count
c.execute("PRAGMA table_info(leads)")
columns = [col[1] for col in c.fetchall()]

if 'open_count' in columns:
    c.execute("ALTER TABLE leads ADD COLUMN interact_count INTEGER")
    c.execute("UPDATE leads SET interact_count = open_count WHERE open_count IS NOT NULL")
    # Workaround for SQLite (no DROP COLUMN)
    c.execute('''CREATE TABLE leads_new AS SELECT email, first_name, status, timestamp, hash, opened_timestamp,
                 email_sent, interact_count, sent_template, notes FROM leads''')
    c.execute("DROP TABLE leads")
    c.execute("ALTER TABLE leads_new RENAME TO leads")
    conn.commit()
    print("Migrated open_count to interact_count successfully.")
else:
    print("No open_count column found; migration not needed.")

conn.close()