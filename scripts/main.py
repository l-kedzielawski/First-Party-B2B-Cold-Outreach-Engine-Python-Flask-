import os
import smtplib
import sqlite3
import hashlib
import yaml
import argparse
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email_validator import validate_email, EmailNotValidError
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import csv
import urllib.parse

# Setup logging
logging.basicConfig(filename='email_system.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

smtp_server = config['smtp']['server']
smtp_port = config['smtp']['port']
smtp_username = config['smtp']['username']
smtp_password = os.getenv('SMTP_PASSWORD')
from_email = config['smtp']['from_email']
db_path = config['database']  # Default database path

if not smtp_password:
    raise ValueError("SMTP_PASSWORD environment variable not set")

# Setup Jinja2
env = Environment(loader=FileSystemLoader('templates'))

# Database setup
def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Create leads table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS leads
                 (email TEXT PRIMARY KEY, first_name TEXT, status TEXT, timestamp TEXT,
                  hash TEXT, opened_timestamp TEXT, email_sent INTEGER)''')
    
    # Create lead_details table for form submissions
    c.execute('''CREATE TABLE IF NOT EXISTS lead_details
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, name TEXT, position TEXT,
                  phone TEXT, message TEXT, submitted_at TEXT,
                  FOREIGN KEY(email) REFERENCES leads(email))''')
    
    # Check and add missing columns to leads table
    c.execute("PRAGMA table_info(leads)")
    columns = [col[1] for col in c.fetchall()]
    
    if 'hash' not in columns:
        c.execute("ALTER TABLE leads ADD COLUMN hash TEXT")
    if 'opened_timestamp' not in columns:
        c.execute("ALTER TABLE leads ADD COLUMN opened_timestamp TEXT")
    if 'email_sent' not in columns:
        c.execute("ALTER TABLE leads ADD COLUMN email_sent INTEGER DEFAULT 0")
    
    # Ensure existing rows have email_sent set to 0
    c.execute("UPDATE leads SET email_sent = 0 WHERE email_sent IS NULL")
    
    conn.commit()
    conn.close()

def hash_email(email):
    return hashlib.sha256(email.encode()).hexdigest()

def load_csv_to_db(csv_file, db_path):
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                validate_email(row['email'], check_deliverability=False)
                email_hash = hash_email(row['email'])
                c.execute('INSERT OR IGNORE INTO leads (email, first_name, status, timestamp, hash, email_sent) VALUES (?, ?, ?, ?, ?, ?)',
                          (row['email'], row.get('first_name', ''), 'gray', time.ctime(), email_hash, 0))
            except EmailNotValidError:
                logging.error(f"Invalid email: {row['email']}")
    conn.commit()
    conn.close()

def export_to_csv(status, output_file, db_path):
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('SELECT email, first_name FROM leads WHERE status = ?', (status,))
    rows = c.fetchall()
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email', 'first_name'])
        writer.writerows(rows)
    conn.close()

def send_email(email, first_name, db_path, attachment_path=None):
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('SELECT status FROM leads WHERE email = ?', (email,))
    result = c.fetchone()
    if result and result[0] in ['green', 'red']:
        logging.info(f"Skipping {email}: already in {result[0]} list")
        conn.close()
        return False

    # Render email template
    template = env.get_template('email.html')
    email_hash = hash_email(email)
    db_name = Path(db_path).name
    # Generate green and red URLs
    green_target_url = f"https://nma.themysticaroma.com/green?hash={email_hash}&db={db_name}"
    red_target_url = f"https://nma.themysticaroma.com/red?hash={email_hash}&db={db_name}"
    green_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(green_target_url)}&db={db_name}"
    red_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(red_target_url)}&db={db_name}"
    # Generate tracked links for other URLs in the email
    company_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://www.themysticaroma.com')}&db={db_name}"
    catalog_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://themysticaroma.com/3d-flip-book/de-produktkatalog/')}&db={db_name}"
    privacy_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://themysticaroma.com/privacy-policy/')}&db={db_name}"
    # Add timestamp for cache-busting
    timestamp = int(time.time())
    logging.info(f"Green URL: {green_url}")
    logging.info(f"Red URL: {red_url}")
    html_content = template.render(
        first_name=first_name or 'Valued Customer',
        email_hash=email_hash,
        db_name=db_name,
        green_url=green_url,
        red_url=red_url,
        company_link=company_link,
        catalog_link=catalog_link,
        privacy_link=privacy_link,
        timestamp=timestamp
    )

    # Create email
    msg = MIMEMultipart()
    msg['From'] = config['smtp']['from_email']
    msg['To'] = email
    msg['Subject'] = config['email']['subject']
    msg.attach(MIMEText(html_content, 'html'))

    # Attach file if provided
    if attachment_path and Path(attachment_path).is_file():
        if Path(attachment_path).stat().st_size > 5 * 1024 * 1024:  # 5MB limit
            logging.error(f"Attachment {attachment_path} too large")
            c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
            conn.commit()
            conn.close()
            return False
        with open(attachment_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=Path(attachment_path).name)
            part['Content-Disposition'] = f'attachment; filename="{Path(attachment_path).name}"'
            msg.attach(part)

    # Send email
    try:
        with smtplib.SMTP_SSL(config['smtp']['server'], config['smtp']['port']) as server:
            server.login(config['smtp']['username'], smtp_password)
            server.send_message(msg)
        logging.info(f"Sent email to {email}")
        c.execute("UPDATE leads SET email_sent = 1 WHERE email = ?", (email,))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {email}: {e}")
        c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
        conn.commit()
        return False
    finally:
        conn.close()

def preview_email(db_path, first_name='Test User'):
    init_db(db_path)
    template = env.get_template('email.html')
    email_hash = hash_email('test@example.com')
    db_name = Path(db_path).name
    # Generate green and red URLs
    green_target_url = f"https://nma.themysticaroma.com/green?hash={email_hash}&db={db_name}"
    red_target_url = f"https://nma.themysticaroma.com/red?hash={email_hash}&db={db_name}"
    green_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(green_target_url)}&db={db_name}"
    red_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(red_target_url)}&db={db_name}"
    # Generate tracked links for other URLs in the email
    company_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://www.themysticaroma.com')}&db={db_name}"
    catalog_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://themysticaroma.com/3d-flip-book/de-produktkatalog/')}&db={db_name}"
    privacy_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote('https://themysticaroma.com/privacy-policy/')}&db={db_name}"
    # Add timestamp for cache-busting
    timestamp = int(time.time())
    logging.info(f"Preview Green URL: {green_url}")
    logging.info(f"Preview Red URL: {red_url}")
    html_content = template.render(
        first_name=first_name,
        email_hash=email_hash,
        db_name=db_name,
        green_url=green_url,
        red_url=red_url,
        company_link=company_link,
        catalog_link=catalog_link,
        privacy_link=privacy_link,
        timestamp=timestamp
    )
    with open('email_preview.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info("Generated email preview at email_preview.html")

def main():
    parser = argparse.ArgumentParser(description='Cold Email Outreach System')
    parser.add_argument('action', choices=['send', 'load', 'export', 'preview'],
                        help='Action to perform')
    parser.add_argument('--list', help='CSV file with leads for load or send')
    parser.add_argument('--attachment', help='Path to attachment file')
    parser.add_argument('--status', help='Status for export (gray, green, red, yellow)')
    parser.add_argument('--output', help='Output CSV file for export')
    parser.add_argument('--db', help='Database file to use (overrides config)', default=config['database'])
    args = parser.parse_args()

    global db_path
    db_path = args.db
    init_db(db_path)

    if args.action == 'load':
        if not args.list:
            print("Error: --list required for load")
            return
        load_csv_to_db(args.list, db_path)
        print(f"Loaded leads from {args.list} into {db_path}")

    elif args.action == 'send':
        if not args.list:
            print("Error: --list required for send")
            return
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT email, first_name FROM leads WHERE status = 'gray' AND (email_sent IS NULL OR email_sent = 0)")
        leads = c.fetchall()
        conn.close()
        logging.info(f"Found {len(leads)} leads to send emails to")
        if not leads:
            print("No leads to send emails to (status: gray, email_sent: 0 or NULL)")
            return
        sent_count = 0
        for email, first_name in leads:
            if send_email(email, first_name, db_path, args.attachment):
                sent_count += 1
                time.sleep(config['email']['delay'])  # Rate limiting
            else:
                conn = sqlite3.connect(db_path)
                c = conn.cursor()
                c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
                conn.commit()
                conn.close()
        print(f"Email sending complete: {sent_count} emails sent")

    elif args.action == 'export':
        if not (args.status and args.output):
            print("Error: --status and --output required for export")
            return
        export_to_csv(args.status, args.output, db_path)
        print(f"Exported {args.status} list to {args.output}")

    elif args.action == 'preview':
        preview_email(db_path)
        print("Preview generated at email_preview.html")

if __name__ == '__main__':
    main()