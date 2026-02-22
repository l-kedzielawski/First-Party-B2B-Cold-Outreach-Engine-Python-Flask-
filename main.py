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

logging.basicConfig(filename='email_system.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
campaigns = config.get('campaigns', [])

class CampaignManager:
    def __init__(self, name, db_path, smtp_config, email_config, links):
        self.name = name
        self.db_path = db_path
        self.smtp_config = smtp_config
        self.email_config = email_config
        self.links = links or {}
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        if not self.smtp_password:
            raise ValueError("SMTP_PASSWORD environment variable not set")

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS leads
                     (email TEXT PRIMARY KEY, first_name TEXT, status TEXT, timestamp TEXT,
                      hash TEXT, opened_timestamp TEXT, email_sent INTEGER, interact_count INTEGER DEFAULT 0,
                      last_interact_timestamp TEXT, sent_template TEXT, notes TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS lead_details
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, name TEXT, position TEXT,
                      phone TEXT, message TEXT, submitted_at TEXT,
                      FOREIGN KEY(email) REFERENCES leads(email))''')
        c.execute("PRAGMA table_info(leads)")
        columns = [col[1] for col in c.fetchall()]
        if 'hash' not in columns: c.execute("ALTER TABLE leads ADD COLUMN hash TEXT")
        if 'opened_timestamp' not in columns: c.execute("ALTER TABLE leads ADD COLUMN opened_timestamp TEXT")
        if 'email_sent' not in columns: c.execute("ALTER TABLE leads ADD COLUMN email_sent INTEGER DEFAULT 0")
        if 'interact_count' not in columns: c.execute("ALTER TABLE leads ADD COLUMN interact_count INTEGER DEFAULT 0")
        if 'last_interact_timestamp' not in columns: c.execute("ALTER TABLE leads ADD COLUMN last_interact_timestamp TEXT")
        if 'sent_template' not in columns: c.execute("ALTER TABLE leads ADD COLUMN sent_template TEXT")
        if 'notes' not in columns: c.execute("ALTER TABLE leads ADD COLUMN notes TEXT")
        c.execute("UPDATE leads SET email_sent = 0 WHERE email_sent IS NULL")
        c.execute("UPDATE leads SET interact_count = 0 WHERE interact_count IS NULL")
        c.execute("UPDATE leads SET last_interact_timestamp = NULL WHERE last_interact_timestamp IS NULL")
        conn.commit()
        conn.close()

    def get_db_path(self):
        return self.db_path

campaign_managers = {camp['name']: CampaignManager(camp['name'], camp['database'], camp['smtp'], camp['email'], camp.get('links', {})) for camp in campaigns}
for manager in campaign_managers.values():
    manager.init_db()

env = Environment(loader=FileSystemLoader('templates'))

def hash_email(email):
    return hashlib.sha256(email.encode()).hexdigest()

def load_csv_to_db(csv_file, manager):
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute("SELECT email FROM leads")
    existing_emails = {row[0] for row in c.fetchall()}
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        new_leads = 0
        skipped_duplicates = 0
        for row in reader:
            try:
                validate_email(row['email'], check_deliverability=False)
                email = row['email']
                if email in existing_emails:
                    logging.info(f"Skipping duplicate email: {email} in {manager.name}")
                    skipped_duplicates += 1
                    continue
                email_hash = hash_email(email)
                c.execute('INSERT INTO leads (email, first_name, status, timestamp, hash, email_sent, interact_count, last_interact_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                          (email, row.get('first_name', ''), 'gray', time.ctime(), email_hash, 0, 0, None))
                existing_emails.add(email)
                new_leads += 1
            except EmailNotValidError:
                logging.error(f"Invalid email: {row['email']} in {manager.name}")
    conn.commit()
    conn.close()
    logging.info(f"Loaded {new_leads} new leads and skipped {skipped_duplicates} duplicates from {csv_file} into {manager.name}")
    print(f"Loaded {new_leads} new leads and skipped {skipped_duplicates} duplicates from {csv_file} into {manager.name}")

def export_to_csv(status, output_file, manager):
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute('SELECT email, first_name, interact_count FROM leads WHERE status = ?', (status,))
    rows = c.fetchall()
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email', 'first_name', 'interact_count'])
        writer.writerows(rows)
    conn.close()

def send_email(manager, email, first_name, attachment_path=None, template_path=None):
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute('SELECT status, interact_count FROM leads WHERE email = ?', (email,))
    result = c.fetchone()
    if result and result[0] in ['green', 'red', 'blue']:
        logging.info(f"Skipping {email} in {manager.name}: already in {result[0]} list")
        conn.close()
        return False
    if template_path and template_path.startswith('/templates/'):
        template_path = template_path[1:]
    template = env.get_template(template_path or 'email.html')
    email_hash = hash_email(email)
    db_name = manager.name  # Should be cukiernie_PL2
    logging.info(f"Rendering email for {email} with db_name: {db_name}")  # Debug log
    green_target_url = f"https://nma.themysticaroma.com/green?hash={email_hash}"
    red_target_url = f"https://nma.themysticaroma.com/red?hash={email_hash}"
    green_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(green_target_url)}"
    red_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(red_target_url)}"
    company_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(manager.links.get('company_link', 'https://www.themysticaroma.com'))}"
    shop_link = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(manager.links.get('shop_link', 'https://shop.themysticaroma.com'))}"
    buy_sample_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(manager.links.get('buy_sample', 'https://www.pastries.com/buy-sample'))}"
    privacy_policy_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(manager.links.get('privacy_policy', 'https://shop.themysticaroma.com/polityka-privatnosci/13'))}"
    landing_page_url = f"https://nma.themysticaroma.com/track_link?hash={email_hash}&url={urllib.parse.quote(manager.links.get('landing_page', 'https://themysticaroma.com/pl/cukiernie/'))}"
    timestamp = int(time.time())
    html_content = template.render(
        first_name=first_name,
        email_hash=email_hash,
        db_name=db_name,
        green_url=green_url,
        red_url=red_url,
        company_link=company_link,
        shop_link=shop_link,
        buy_sample_url=buy_sample_url,
        privacy_policy_url=privacy_policy_url,
        landing_page_url=landing_page_url,
        timestamp=timestamp
    )
    # Rest of the function...
    msg = MIMEMultipart()
    msg['From'] = manager.smtp_config['from_email']
    msg['To'] = email
    msg['Subject'] = manager.email_config['subject']
    msg.attach(MIMEText(html_content, 'html'))
    if attachment_path and Path(attachment_path).is_file():
        if Path(attachment_path).stat().st_size > 5 * 1024 * 1024:
            logging.error(f"Attachment {attachment_path} too large in {manager.name}")
            c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
            conn.commit()
            conn.close()
            return False
        with open(attachment_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=Path(attachment_path).name)
            part['Content-Disposition'] = f'attachment; filename="{Path(attachment_path).name}"'
            msg.attach(part)
    try:
        with smtplib.SMTP_SSL(manager.smtp_config['server'], manager.smtp_config['port']) as server:
            server.login(manager.smtp_config['username'], manager.smtp_password)
            server.send_message(msg)
        current_count = result[1] if result else 0
        c.execute("UPDATE leads SET email_sent = 1, interact_count = ?, timestamp = ?, sent_template = ?, last_interact_timestamp = ? WHERE email = ?",
                  (current_count + 1, time.ctime(), template_path or 'email.html', time.ctime(), email))
        conn.commit()
        logging.info(f"Sent email to {email} from {manager.name}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {email} from {manager.name}: {e}")
        c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
        conn.commit()
        return False
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Cold Email Outreach System')
    parser.add_argument('action', choices=['send', 'load', 'export', 'preview'],
                        help='Action to perform')
    parser.add_argument('--list', help='CSV file with leads for load or send')
    parser.add_argument('--attachment', help='Path to attachment file')
    parser.add_argument('--status', help='Status for export (gray, green, red, yellow, blue)')
    parser.add_argument('--output', help='Output CSV file for export')
    parser.add_argument('--campaign', choices=[c['name'] for c in config['campaigns']], default='poland',
                        help='Campaign to operate on')
    parser.add_argument('--email', help='Email template path (e.g., email-PL1.html)', default=None)
    args = parser.parse_args()

    manager = campaign_managers[args.campaign]

    if args.action == 'load':
        if not args.list:
            print("Error: --list required for load")
            return
        load_csv_to_db(args.list, manager)
        print(f"Loaded leads from {args.list} into {args.campaign}")

    elif args.action == 'send':
        if not args.list:
            print("Error: --list required for send")
            return
        conn = sqlite3.connect(manager.get_db_path())
        c = conn.cursor()
        c.execute("SELECT email, first_name FROM leads WHERE status = 'gray' AND (email_sent IS NULL OR email_sent = 0)")
        leads = c.fetchall()
        conn.close()
        logging.info(f"Found {len(leads)} leads to send emails to in {args.campaign}")
        if not leads:
            print(f"No leads to send emails to in {args.campaign} (status: gray, email_sent: 0 or NULL)")
            return
        sent_count = 0
        for email, first_name in leads:
            if send_email(manager, email, first_name, args.attachment, args.email):
                sent_count += 1
                time.sleep(manager.email_config['delay'])
            else:
                conn = sqlite3.connect(manager.get_db_path())
                c = conn.cursor()
                c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
                conn.commit()
                conn.close()
        print(f"Email sending complete for {args.campaign}: {sent_count} emails sent")

    elif args.action == 'export':
        if not (args.status and args.output):
            print("Error: --status and --output required for export")
            return
        export_to_csv(args.status, args.output, manager)
        print(f"Exported {args.status} list from {args.campaign} to {args.output}")

    elif args.action == 'preview':
        # Implement preview logic if needed
        print("Preview not implemented yet for multiple campaigns")

if __name__ == '__main__':
    main()