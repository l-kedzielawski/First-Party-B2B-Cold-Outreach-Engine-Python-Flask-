from flask import (
    Flask,
    request,
    render_template_string,
    redirect,
    send_from_directory,
    render_template,
)
import sqlite3
import hashlib
import logging
import yaml
import time
import os
import smtplib
from email.mime.text import MIMEText
import glob
from flask_httpauth import HTTPBasicAuth
import threading

app = Flask(__name__)
auth = HTTPBasicAuth()

logging.basicConfig(
    filename="email_system.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

with open("./config.yaml", "r") as f:
    config = yaml.safe_load(f)
campaigns = config.get("campaigns", [])


class CampaignManager:
    def __init__(
        self,
        name,
        db_path,
        smtp_config,
        email_config,
        privacy_url,
        landing_page,
        email_template,
    ):
        self.name = name
        self.db_path = db_path
        self.smtp_config = smtp_config
        self.email_config = email_config
        self.privacy_url = privacy_url
        self.landing_page = landing_page
        self.email_template = email_template
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        if not self.smtp_password:
            raise ValueError("SMTP_PASSWORD environment variable not set")

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS leads
                     (email TEXT PRIMARY KEY, first_name TEXT, status TEXT, timestamp TEXT,
                      hash TEXT, opened_timestamp TEXT, email_sent INTEGER, interact_count INTEGER DEFAULT 0,
                      last_interact_timestamp TEXT, sent_template TEXT, notes TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS lead_details
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, name TEXT, position TEXT,
                      phone TEXT, message TEXT, submitted_at TEXT,
                      FOREIGN KEY(email) REFERENCES leads(email))""")
        c.execute("PRAGMA table_info(leads)")
        columns = [col[1] for col in c.fetchall()]
        if "hash" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN hash TEXT")
        if "opened_timestamp" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN opened_timestamp TEXT")
        if "email_sent" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN email_sent INTEGER DEFAULT 0")
        if "interact_count" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN interact_count INTEGER DEFAULT 0")
        if "last_interact_timestamp" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN last_interact_timestamp TEXT")
        if "sent_template" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN sent_template TEXT")
        if "notes" not in columns:
            c.execute("ALTER TABLE leads ADD COLUMN notes TEXT")
        c.execute("UPDATE leads SET email_sent = 0 WHERE email_sent IS NULL")
        c.execute("UPDATE leads SET interact_count = 0 WHERE interact_count IS NULL")
        c.execute(
            "UPDATE leads SET last_interact_timestamp = ? WHERE last_interact_timestamp IS NULL",
            (time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),),
        )
        conn.commit()
        conn.close()

    def send_notification_email(self, to_email, subject, message):
        msg = MIMEText(message)
        msg["From"] = self.smtp_config["from_email"]
        msg["To"] = to_email
        msg["Subject"] = subject
        try:
            with smtplib.SMTP_SSL(
                self.smtp_config["server"], self.smtp_config["port"]
            ) as server:
                server.login(self.smtp_config["username"], self.smtp_password)
                server.send_message(msg)
            logging.info(f"Sent notification email to {to_email} from {self.name}")
        except Exception as e:
            logging.error(
                f"Failed to send notification email to {to_email} from {self.name}: {e}"
            )

    def get_db_path(self):
        return self.db_path


campaign_managers = {
    camp["name"]: CampaignManager(
        camp["name"],
        camp["database"],
        camp["smtp"],
        camp["email"],
        camp["privacy_url"],
        camp.get("landing_page", ""),
        camp.get("email_template", "email.html"),
    )
    for camp in campaigns
}
for manager in campaign_managers.values():
    manager.init_db()

_dashboard_password = os.getenv("DASHBOARD_PASSWORD", "")
if not _dashboard_password:
    raise ValueError("DASHBOARD_PASSWORD environment variable not set")
users = {"admin": _dashboard_password}


@auth.verify_password
def verify_password(username, password):
    if username in users and users[username] == password:
        return username
    return None


def hash_email(email):
    return hashlib.sha256(email.encode()).hexdigest()


def get_campaign_manager(campaign_name):
    if not campaign_name or campaign_name not in campaign_managers:
        logging.warning(
            f"No valid campaign specified: {campaign_name}. Redirecting to campaign selection."
        )
        return None
    return campaign_managers[campaign_name]


@app.route("/track_open")
def track_open():
    email_hash = request.args.get("hash")
    campaign_name = request.args.get("campaign")
    if not email_hash:
        logging.warning(f"Track open: Missing hash parameter")
        return "", 200  # Return 1x1 GIF to avoid breaking tracking pixel
    updated = False
    if campaign_name and campaign_name in campaign_managers:
        manager = campaign_managers[campaign_name]
        conn = sqlite3.connect(manager.get_db_path())
        c = conn.cursor()
        try:
            logging.info(
                f"Checking track open for hash: {email_hash} in {campaign_name}"
            )
            c.execute(
                "SELECT email, status, opened_timestamp, interact_count, last_interact_timestamp FROM leads WHERE hash = ?",
                (email_hash,),
            )
            row = c.fetchone()
            if row:
                (
                    email,
                    current_status,
                    opened_timestamp,
                    current_count,
                    last_interact_timestamp,
                ) = row
                new_count = current_count + 1
                current_time = time.ctime()
                if not opened_timestamp:
                    c.execute(
                        "UPDATE leads SET status = 'yellow', opened_timestamp = ?, interact_count = ?, last_interact_timestamp = ? WHERE email = ?",
                        (current_time, new_count, current_time, email),
                    )
                    logging.info(
                        f"Email opened by {email} in {campaign_name}, moved to yellow, interact_count now {new_count}"
                    )
                else:
                    c.execute(
                        "UPDATE leads SET interact_count = ?, last_interact_timestamp = ? WHERE email = ?",
                        (new_count, current_time, email),
                    )
                    logging.info(
                        f"Email reopened by {email} in {campaign_name}, interact_count now {new_count}"
                    )
                conn.commit()
                updated = True
        except Exception as e:
            logging.error(
                f"Track open error for hash {email_hash} in {campaign_name}: {e}"
            )
        finally:
            conn.close()
    else:
        logging.warning(
            f"Track open: Invalid or missing campaign {campaign_name}, skipping update"
        )
    if not updated:
        logging.warning(
            f"Track open: Lead not found for hash {email_hash} in {campaign_name if campaign_name else 'any campaign'}"
        )
    return (
        b"\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b",
        200,
        {"Content-Type": "image/gif"},
    )


@app.route("/track_link")
def track_link():
    email_hash = request.args.get("hash")
    destination = request.args.get("url")
    campaign_name = request.args.get("campaign", "poland")
    if not email_hash or not destination:
        return "Invalid request", 400
    manager = get_campaign_manager(campaign_name)
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    try:
        c.execute(
            "SELECT email, status, interact_count, last_interact_timestamp FROM leads WHERE hash = ?",
            (email_hash,),
        )
        row = c.fetchone()
        if row:
            email, current_status, current_count, last_interact_timestamp = row
            new_count = current_count + 1
            current_time = time.ctime()
            green_url = f"https://nma.themysticaroma.com/green?campaign={campaign_name}"
            if destination.startswith(green_url):
                c.execute(
                    "UPDATE leads SET status = 'green', interact_count = ?, last_interact_timestamp = ? WHERE email = ?",
                    (new_count, current_time, email),
                )
                logging.info(
                    f"Green link clicked by {email} in {campaign_name}, moved to green"
                )
            elif current_status != "red":
                c.execute(
                    "UPDATE leads SET status = 'yellow', interact_count = ?, last_interact_timestamp = ? WHERE email = ?",
                    (new_count, current_time, email),
                )
                logging.info(
                    f"Link clicked by {email} in {campaign_name} (URL: {destination}), moved to yellow"
                )
            else:
                c.execute(
                    "UPDATE leads SET interact_count = ?, last_interact_timestamp = ? WHERE email = ?",
                    (new_count, current_time, email),
                )
                logging.info(
                    f"Link clicked by {email} in {campaign_name} (URL: {destination}), remained red"
                )
            conn.commit()
        else:
            logging.warning(
                f"Track link: Lead not found for hash {email_hash} in {campaign_name}"
            )
    except Exception as e:
        logging.error(f"Track link error for hash {email_hash} in {campaign_name}: {e}")
    finally:
        conn.close()
    return redirect(destination)


@app.route("/green")
def green():
    email_hash = request.args.get("hash")
    campaign_name = request.args.get("campaign", "poland")
    manager = get_campaign_manager(campaign_name)
    if not email_hash:
        return "Invalid request", 400
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute("SELECT email, interact_count FROM leads")
    email = None
    for row in c.fetchall():
        computed_hash = hash_email(row[0])
        if computed_hash == email_hash:
            email = row[0]
            current_time = time.ctime()
            c.execute(
                "UPDATE leads SET status = 'green', timestamp = ? WHERE email = ?",
                (current_time, email),
            )
            conn.commit()
            logging.info(f"Moved {email} to green list in {campaign_name}")
            manager.send_notification_email(
                manager.smtp_config["from_email"],
                "New Interested Lead",
                f"Lead {email} clicked 'I’m Interested' at {current_time} (Campaign: {campaign_name})",
            )
            break
    conn.close()
    if not email:
        logging.warning(f"Lead not found for hash {email_hash} in {campaign_name}")
        return "Lead not found", 404
    return redirect(manager.landing_page)


@app.route("/red")
def red():
    email_hash = request.args.get("hash")
    campaign_name = request.args.get("campaign", "poland")
    manager = get_campaign_manager(campaign_name)
    if not email_hash:
        return "Invalid request", 400
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute("SELECT email, interact_count FROM leads")
    email = None
    for row in c.fetchall():
        computed_hash = hash_email(row[0])
        logging.info(
            f"Comparing hash for email {row[0]}: URL hash={email_hash}, computed hash={computed_hash}"
        )
        if computed_hash == email_hash:
            email = row[0]
            current_time = time.ctime()
            c.execute(
                "UPDATE leads SET status = 'red', timestamp = ? WHERE email = ?",
                (current_time, email),
            )
            conn.commit()
            logging.info(f"Moved {email} to red list in {campaign_name}")
            manager.send_notification_email(
                manager.smtp_config["from_email"],
                "Lead Unsubscribed",
                f"Lead {email} unsubscribed at {current_time} (Campaign: {campaign_name})",
            )
            break
    conn.close()
    if not email:
        logging.warning(f"Lead not found for hash {email_hash} in {campaign_name}")
        return "Lead not found", 404
    return render_template_string(
        """
        <html><head><title>Unsubscribed</title><style>body { font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f0f0f0; }
        .container { text-align: center; width: 80%; max-width: 600px; padding: 20px; background-color: white; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        .language-toggle { margin-top: 20px; }
        .language-toggle button { margin: 0 5px; padding: 5px 10px; cursor: pointer; }
        .language-content { display: none; }
        .language-content.active { display: block; }</style>
        <script>function toggleLanguage(lang) {document.querySelectorAll('.language-content').forEach(content => content.classList.remove('active'));document.getElementById(lang).classList.add('active');}</script></head>
        <body><div class="container"><div id="en" class="language-content active"><h1>Got it — We Won’t Reach Out Again</h1><p>Thanks for taking a moment to let us know.</p><p>We understand that our message wasn’t relevant for you, and we truly respect that.</p><p>We apologize for the interruption and appreciate your time.</p><p>If you ever have a need for premium vanilla, cacao, or spices — or just want to connect — here’s how to reach us:</p><p><strong>Lukasz Kedzielawski</strong><br>✉️ <a href="mailto:l.kedzielawski@themysticaroma.com">l.kedzielawski@themysticaroma.com</a> | 📱 <a href="https://wa.me/48665103994">WhatsApp: +48 665 103 994</a></p><p><strong>Karol Kucharski</strong><br>✉️ <a href="mailto:k.kucharski@themysticaroma.com">k.kucharski@themysticaroma.com</a> | 📱 <a href="https://wa.me/48509083445">WhatsApp: +48 509 083 445</a></p><p><strong><a href="https://nma.themysticaroma.com/track_link?hash={{ hash }}&url={{ 'https://www.themysticaroma.com'|urlencode }}&campaign={{ campaign }}">www.themysticaroma.com</a></strong></p><p><strong><a href="https://nma.themysticaroma.com/track_link?hash={{ hash }}&url={{ 'https://shop.themysticaroma.com'|urlencode }}&campaign={{ campaign }}">shop.themysticaroma.com</a></strong></p><p>No pressure. No follow-ups. Just gratitude.</p><p>Wishing you continued success.</p><p>– The Mystic Aroma Team</p></div><div id="de" class="language-content"><h1>Verstanden — Wir werden nicht mehr kontaktieren</h1><p>Vielen Dank, dass Sie uns Bescheid gegeben haben.</p><p>Wir verstehen, dass unsere Nachricht für Sie nicht relevant war, und respektieren das.</p><p>Wir entschuldigen uns für die Unterbrechung und schätzen Ihre Zeit.</p><p>Falls Sie jemals Bedarf an Premium-Vanille, Kakao oder Gewürzen haben oder einfach verbinden möchten, hier sind unsere Kontakte:</p><p><strong>Lukasz Kedzielawski</strong><br>✉️ <a href="mailto:l.kedzielawski@themysticaroma.com">l.kedzielawski@themysticaroma.com</a> | 📱 <a href="https://wa.me/48665103994">WhatsApp: +48 665 103 994</a></p><p><strong>Karol Kucharski</strong><br>✉️ <a href="mailto:k.kucharski@themysticaroma.com">k.kucharski@themysticaroma.com</a> | 📱 <a href="https://wa.me/48509083445">WhatsApp: +48 509 083 445</a></p><p><strong><a href="https://nma.themysticaroma.com/track_link?hash={{ hash }}&url={{ 'https://www.themysticaroma.com'|urlencode }}&campaign={{ campaign }}">www.themysticaroma.com</a></strong></p><p><strong><a href="https://nma.themysticaroma.com/track_link?hash={{ hash }}&url={{ 'https://shop.themysticaroma.com'|urlencode }}&campaign={{ campaign }}">shop.themysticaroma.com</a></strong></p><p>Kein Druck. Keine Nachverfolgung. Nur Dankbarkeit.</p><p>Wir wünschen Ihnen weiterhin Erfolg.</p><p>– Das Mystic Aroma Team</p></div><div class="language-toggle"><button onclick="toggleLanguage('en')">English</button><button onclick="toggleLanguage('de')">German</button></div></div></body></html>
    """,
        email=email,
        hash=email_hash,
        campaign=campaign_name,
    )


@app.route("/move_lead", methods=["POST"])
@auth.login_required
def move_lead():
    email = request.form.get("email")
    new_status = request.form.get("status")
    campaign_name = request.form.get("campaign", "poland")
    manager = get_campaign_manager(campaign_name)
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    try:
        c.execute(
            "UPDATE leads SET status = ?, timestamp = ? WHERE email = ?",
            (new_status, time.ctime(), email),
        )
        conn.commit()
        logging.info(f"Moved lead {email} to {new_status} in {campaign_name}")
    except Exception as e:
        logging.error(f"Error moving lead {email}: {e}")
        conn.rollback()
    finally:
        conn.close()
    return redirect(f"/dashboard?campaign={campaign_name}")


@app.route("/delete_lead", methods=["POST"])
@auth.login_required
def delete_lead():
    email = request.form.get("email")
    campaign_name = request.form.get("campaign", "poland")
    manager = get_campaign_manager(campaign_name)
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    try:
        c.execute("DELETE FROM leads WHERE email = ?", (email,))
        c.execute("DELETE FROM lead_details WHERE email = ?", (email,))
        conn.commit()
        logging.info(f"Deleted lead {email} from {campaign_name}")
    except Exception as e:
        logging.error(f"Error deleting lead {email}: {e}")
        conn.rollback()
    finally:
        conn.close()
    return redirect(f"/dashboard?campaign={campaign_name}")


@app.route("/update_notes", methods=["POST"])
@auth.login_required
def update_notes():
    email = request.form.get("email")
    notes = request.form.get("notes")
    campaign_name = request.form.get("campaign", "poland")
    manager = get_campaign_manager(campaign_name)
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    try:
        c.execute("UPDATE leads SET notes = ? WHERE email = ?", (notes, email))
        conn.commit()
        logging.info(f"Updated notes for {email} in {campaign_name}")
    except Exception as e:
        logging.error(f"Error updating notes for {email}: {e}")
        conn.rollback()
    finally:
        conn.close()
    return redirect(f"/dashboard?campaign={campaign_name}")


@app.route("/dashboard")
@auth.login_required
def dashboard():
    campaign_name = request.args.get("campaign")
    manager = get_campaign_manager(campaign_name)

    if not manager:
        # Show campaign selection page if no campaign is specified
        return render_template_string(
            """
            <html><head><title>Select Campaign</title></head><body>
            <h1>Select a Campaign</h1>
            <p>Please choose a campaign to view its dashboard:</p>
            {% for camp in campaign_managers.keys() %}
                <p><a href="/dashboard?campaign={{ camp }}">{{ camp }}</a> (Database: {{ campaign_managers[camp].get_db_path() }})</p>
            {% endfor %}
            </body></html>
        """,
            campaign_managers=campaign_managers,
        )

    db_path = manager.get_db_path()
    logging.info(f"Accessing dashboard for {campaign_name}")
    search_query = request.args.get("search", "").lower()
    sort_by = request.args.get("sort_by", "timestamp")
    sort_order = request.args.get("sort_order", "desc")

    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("PRAGMA table_info(leads)")
        logging.info(f"Table info for {db_path}: {c.fetchall()}")
        c.execute("SELECT status, COUNT(*) FROM leads GROUP BY status")
        stats = dict(c.fetchall())

        def fetch_leads(status):
            query = "SELECT email, first_name, timestamp, email_sent, sent_template, notes, opened_timestamp, last_interact_timestamp, interact_count FROM leads WHERE status = ?"
            params = [status]
            if search_query:
                query += " AND (email LIKE ? OR first_name LIKE ? OR notes LIKE ?)"
                params.extend(
                    [f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"]
                )
            order = "DESC" if sort_order == "desc" else "ASC"
            if sort_by == "interact_count":
                query += f" ORDER BY interact_count {order}, email {order}"
            elif sort_by == "last_interact_timestamp":
                query += f" ORDER BY COALESCE(last_interact_timestamp, '1970-01-01') {order}, email {order}"
            else:
                query += f" ORDER BY COALESCE(timestamp, '1970-01-01') {order}, email {order}"
            logging.info(f"Executing query: {query} with params {params}")
            c.execute(query, params)
            return c.fetchall()

        green_leads = fetch_leads("green")
        blue_leads = fetch_leads("blue")
        yellow_leads = fetch_leads("yellow")
        gray_leads = fetch_leads("gray")
        red_leads = fetch_leads("red")

        green_details = {}
        for lead in green_leads:
            email = lead[0]
            c.execute(
                "SELECT name, position, phone, message, submitted_at FROM lead_details WHERE email = ?",
                (email,),
            )
            details = c.fetchone()
            green_details[email] = (
                details if details else (None, None, None, None, None)
            )

        interact_counts = {
            row[0]: row[1]
            for row in c.execute("SELECT email, interact_count FROM leads").fetchall()
        }

        conn.close()
        logging.info(
            f"Stats: {stats}, Green leads: {len(green_leads)}, Blue: {len(blue_leads)}, Yellow: {len(yellow_leads)}, Gray: {len(gray_leads)}, Red: {len(red_leads)}"
        )
    except sqlite3.OperationalError as e:
        logging.error(f"Database error: {e} for {campaign_name}")
        stats = {"green": 0, "blue": 0, "yellow": 0, "gray": 0, "red": 0}
        green_leads, blue_leads, yellow_leads, gray_leads, red_leads = (
            [],
            [],
            [],
            [],
            [],
        )
        green_details = {}
        interact_counts = {}
    except Exception as e:
        logging.error(f"Unexpected error in dashboard: {e}")
        return "Internal Server Error: Check logs", 500

    def sort_link(column, label):
        new_order = "asc" if sort_order == "desc" else "desc"
        arrow = (
            " ↓"
            if (column == sort_by and sort_order == "desc")
            else " ↑"
            if (column == sort_by and sort_order == "asc")
            else ""
        )
        return (
            f"/dashboard?sort_by={column}&sort_order={new_order}&search={search_query}&campaign={campaign_name}",
            arrow,
        )

    try:
        dashboard_dir = os.getenv(
            "DATA_DIR", os.path.join(os.path.dirname(__file__), "data")
        )
        db_files = [
            os.path.basename(f) for f in glob.glob(os.path.join(dashboard_dir, "*.db"))
        ]
        db_links = []
        if db_files:
            path_to_campaign = {
                v.get_db_path(): k for k, v in campaign_managers.items()
            }
            logging.info(f"Campaign path mappings: {path_to_campaign}")
            for db_file in db_files:
                full_path = os.path.abspath(os.path.join(dashboard_dir, db_file))
                campaign = path_to_campaign.get(full_path, "unknown")
                if campaign != "unknown":
                    db_links.append(
                        f'<a href="/dashboard?campaign={campaign}">{db_file}</a>'
                    )
            db_links = (
                " | ".join(db_links)
                if db_links
                else "No valid campaign databases found"
            )
        else:
            db_links = "No databases available"

        return render_template_string(
            """
            <html><head><title>Analytics Dashboard</title><link rel="stylesheet" href="/static/dashboard.css"></head><body>
            <h1>Email Outreach Stats (Campaign: {{ campaign_name }})</h1>
            <div class="search-bar"><form method="get" action="/dashboard"><input type="text" name="search" placeholder="Search" value="{{ search_query }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit">Search</button></form></div>
            <p>Switch Campaign: {{ db_links | safe }}</p>
            <p>Green: {{ stats.get('green', 0) }}</p><p>Blue: {{ stats.get('blue', 0) }}</p><p>Yellow: {{ stats.get('yellow', 0) }}</p><p>Gray: {{ stats.get('gray', 0) }}</p><p>Red: {{ stats.get('red', 0) }}</p>
            <h2>Interested Leads (Green)</h2>{% if green_leads %}<table><tr><th><a href="{{ sort_link('timestamp', 'Timestamp')[0] }}">Timestamp{{ sort_link('timestamp', 'Timestamp')[1] }}</a></th><th>Email</th><th>First Name</th><th><a href="{{ sort_link('interact_count', 'Interact Count')[0] }}">Interact Count{{ sort_link('interact_count', 'Interact Count')[1] }}</a></th><th><a href="{{ sort_link('last_interact_timestamp', 'Last Interact')[0] }}">Last Interact{{ sort_link('last_interact_timestamp', 'Last Interact')[1] }}</a></th><th>Name</th><th>Position</th><th>Phone</th><th>Message</th><th>Submitted At</th><th>Sent Template</th><th>Notes</th><th>Actions</th></tr>{% for lead in green_leads %}<tr><td>{{ lead[2] }}</td><td>{{ lead[0] }}</td><td>{{ lead[1] or 'N/A' }}</td><td>{{ interact_counts.get(lead[0], 0) }}</td><td>{{ lead[7] or 'N/A' }}</td><td>{{ green_details.get(lead[0], (None,))[0] or 'N/A' }}</td><td>{{ green_details.get(lead[0], (None,))[1] or 'N/A' }}</td><td>{{ green_details.get(lead[0], (None,))[2] or 'N/A' }}</td><td>{{ green_details.get(lead[0], (None,))[3] or 'N/A' }}</td><td>{{ green_details.get(lead[0], (None,))[4] or 'N/A' }}</td><td><a href="/templates/{{ lead[4] }}" target="_blank">{{ lead[4] or 'N/A' }}</a></td><td><form method="post" action="/update_notes"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><input type="text" name="notes" value="{{ lead[5] or '' }}"><button type="submit">Save</button></form></td><td><form method="post" action="/move_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><select name="status"><option value="blue">Blue</option><option value="yellow">Yellow</option><option value="gray">Gray</option><option value="red">Red</option></select><button type="submit">Move</button></form><form method="post" action="/delete_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit" onclick="return confirm('Are you sure?')">Delete</button></form></td></tr>{% endfor %}</table>{% else %}<p>No interested leads yet.</p>{% endif %}
            <h2>Manually Contacted Leads (Blue)</h2>{% if blue_leads %}<table><tr><th><a href="{{ sort_link('timestamp', 'Timestamp')[0] }}">Timestamp{{ sort_link('timestamp', 'Timestamp')[1] }}</a></th><th>Email</th><th>First Name</th><th><a href="{{ sort_link('interact_count', 'Interact Count')[0] }}">Interact Count{{ sort_link('interact_count', 'Interact Count')[1] }}</a></th><th><a href="{{ sort_link('last_interact_timestamp', 'Last Interact')[0] }}">Last Interact{{ sort_link('last_interact_timestamp', 'Last Interact')[1] }}</a></th><th>Sent Template</th><th>Notes</th><th>Actions</th></tr>{% for lead in blue_leads %}<tr><td>{{ lead[2] }}</td><td>{{ lead[0] }}</td><td>{{ lead[1] or 'N/A' }}</td><td>{{ interact_counts.get(lead[0], 0) }}</td><td>{{ lead[7] or 'N/A' }}</td><td><a href="/templates/{{ lead[4] }}" target="_blank">{{ lead[4] or 'N/A' }}</a></td><td><form method="post" action="/update_notes"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><input type="text" name="notes" value="{{ lead[5] or '' }}"><button type="submit">Save</button></form></td><td><form method="post" action="/move_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><select name="status"><option value="green">Green</option><option value="yellow">Yellow</option><option value="gray">Gray</option><option value="red">Red</option></select><button type="submit">Move</button></form><form method="post" action="/delete_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit" onclick="return confirm('Are you sure?')">Delete</button></form></td></tr>{% endfor %}</table>{% else %}<p>No manually contacted leads yet.</p>{% endif %}
            <h2>Opened Emails (Yellow)</h2>{% if yellow_leads %}<table><tr><th><a href="{{ sort_link('timestamp', 'Timestamp')[0] }}">Timestamp{{ sort_link('timestamp', 'Timestamp')[1] }}</a></th><th>Email</th><th>First Name</th><th><a href="{{ sort_link('interact_count', 'Interact Count')[0] }}">Interact Count{{ sort_link('interact_count', 'Interact Count')[1] }}</a></th><th><a href="{{ sort_link('last_interact_timestamp', 'Last Interact')[0] }}">Last Interact{{ sort_link('last_interact_timestamp', 'Last Interact')[1] }}</a></th><th>Sent Template</th><th>Notes</th><th>Actions</th></tr>{% for lead in yellow_leads %}<tr><td>{{ lead[2] }}</td><td>{{ lead[0] }}</td><td>{{ lead[1] or 'N/A' }}</td><td>{{ interact_counts.get(lead[0], 0) }}</td><td>{{ lead[7] or 'N/A' }}</td><td><a href="/templates/{{ lead[4] }}" target="_blank">{{ lead[4] or 'N/A' }}</a></td><td><form method="post" action="/update_notes"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><input type="text" name="notes" value="{{ lead[5] or '' }}"><button type="submit">Save</button></form></td><td><form method="post" action="/move_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><select name="status"><option value="green">Green</option><option value="blue">Blue</option><option value="gray">Gray</option><option value="red">Red</option></select><button type="submit">Move</button></form><form method="post" action="/delete_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit" onclick="return confirm('Are you sure?')">Delete</button></form></td></tr>{% endfor %}</table>{% else %}<p>No emails opened yet.</p>{% endif %}
            <h2>Cold Leads (Gray)</h2>{% if gray_leads %}<table><tr><th><a href="{{ sort_link('timestamp', 'Timestamp')[0] }}">Timestamp{{ sort_link('timestamp', 'Timestamp')[1] }}</a></th><th>Email</th><th>First Name</th><th>Email Sent</th><th>Sent Template</th><th>Notes</th><th>Actions</th></tr>{% for lead in gray_leads %}<tr><td>{{ lead[2] }}</td><td>{{ lead[0] }}</td><td>{{ lead[1] or 'N/A' }}</td><td>{{ 'Yes' if lead[3] == 1 else 'No' }}</td><td><a href="/templates/{{ lead[4] }}" target="_blank">{{ lead[4] or 'N/A' }}</a></td><td><form method="post" action="/update_notes"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><input type="text" name="notes" value="{{ lead[5] or '' }}"><button type="submit">Save</button></form></td><td><form method="post" action="/move_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><select name="status"><option value="green">Green</option><option value="blue">Blue</option><option value="yellow">Yellow</option><option value="red">Red</option></select><button type="submit">Move</button></form><form method="post" action="/delete_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit" onclick="return confirm('Are you sure?')">Delete</button></form></td></tr>{% endfor %}</table>{% else %}<p>No cold leads yet.</p>{% endif %}
            <h2>Unsubscribed Leads (Red)</h2>{% if red_leads %}<table><tr><th><a href="{{ sort_link('timestamp', 'Timestamp')[0] }}">Timestamp{{ sort_link('timestamp', 'Timestamp')[1] }}</a></th><th>Email</th><th>First Name</th><th><a href="{{ sort_link('interact_count', 'Interact Count')[0] }}">Interact Count{{ sort_link('interact_count', 'Interact Count')[1] }}</a></th><th><a href="{{ sort_link('last_interact_timestamp', 'Last Interact')[0] }}">Last Interact{{ sort_link('last_interact_timestamp', 'Last Interact')[1] }}</a></th><th>Sent Template</th><th>Notes</th><th>Actions</th></tr>{% for lead in red_leads %}<tr><td>{{ lead[2] }}</td><td>{{ lead[0] }}</td><td>{{ lead[1] or 'N/A' }}</td><td>{{ interact_counts.get(lead[0], 0) }}</td><td>{{ lead[7] or 'N/A' }}</td><td><a href="/templates/{{ lead[4] }}" target="_blank">{{ lead[4] or 'N/A' }}</a></td><td><form method="post" action="/update_notes"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><input type="text" name="notes" value="{{ lead[5] or '' }}"><button type="submit">Save</button></form></td><td><form method="post" action="/move_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><select name="status"><option value="green">Green</option><option value="blue">Blue</option><option value="yellow">Yellow</option><option value="gray">Gray</option></select><button type="submit">Move</button></form><form method="post" action="/delete_lead"><input type="hidden" name="email" value="{{ lead[0] }}"><input type="hidden" name="campaign" value="{{ campaign_name }}"><button type="submit" onclick="return confirm('Are you sure?')">Delete</button></form></td></tr>{% endfor %}</table>{% else %}<p>No unsubscribed leads yet.</p>{% endif %}
            <p>Switch Campaign: {{ db_links | safe }}</p></body></html>
        """,
            stats=stats,
            green_leads=green_leads,
            blue_leads=blue_leads,
            yellow_leads=yellow_leads,
            gray_leads=gray_leads,
            red_leads=red_leads,
            green_details=green_details,
            campaign_name=campaign_name,
            db_links=db_links,
            search_query=search_query,
            interact_counts=interact_counts,
            sort_link=sort_link,
        )
    except Exception as e:
        logging.error(f"Template rendering error: {e}")
        return "Internal Server Error: Template rendering failed", 500


@app.route("/templates/<path:filename>")
def serve_template(filename):
    return send_from_directory("templates", filename)


def run_campaign(manager):
    while True:
        conn = sqlite3.connect(manager.get_db_path())
        c = conn.cursor()
        c.execute(
            "SELECT email, first_name FROM leads WHERE status = 'gray' AND (email_sent IS NULL OR email_sent = 0)"
        )
        leads = c.fetchall()
        if not leads:
            logging.info(f"No leads to process for {manager.name}")
            break
        for email, first_name in leads:
            if send_email(manager, email, first_name):
                time.sleep(manager.email_config["delay"])
        conn.close()


def send_email(manager, email, first_name):
    conn = sqlite3.connect(manager.get_db_path())
    c = conn.cursor()
    c.execute("SELECT status, interact_count FROM leads WHERE email = ?", (email,))
    result = c.fetchone()
    if result and result[0] in ["green", "red", "blue"]:
        logging.info(f"Skipping {email} in {manager.name}: already in {result[0]} list")
        conn.close()
        return False
    email_hash = hash_email(email)
    domain = manager.smtp_config.get("domain", "https://nma.themysticaroma.com/")
    msg = MIMEText(
        render_template(
            manager.email_template,
            first_name=first_name,
            hash=email_hash,
            domain=domain,
            landing_page=manager.landing_page,
            campaign=manager.name,
        )
    )
    msg["From"] = manager.smtp_config["from_email"]
    msg["To"] = email
    msg["Subject"] = manager.email_config["subject"]
    try:
        with smtplib.SMTP_SSL(
            manager.smtp_config["server"], manager.smtp_config["port"]
        ) as server:
            server.login(manager.smtp_config["username"], manager.smtp_password)
            server.send_message(msg)
        current_count = result[1] if result else 0
        c.execute(
            "UPDATE leads SET email_sent = 1, interact_count = ?, timestamp = ?, sent_template = ?, last_interact_timestamp = ? WHERE email = ?",
            (
                current_count + 1,
                time.ctime(),
                manager.email_template,
                time.ctime(),
                email,
            ),
        )
        conn.commit()
        logging.info(
            f"Sent email to {email} from {manager.name} using template {manager.email_template}"
        )
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {email} from {manager.name}: {e}")
        c.execute("UPDATE leads SET email_sent = 0 WHERE email = ?", (email,))
        conn.commit()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    threads = [
        threading.Thread(target=run_campaign, args=(manager,))
        for manager in campaign_managers.values()
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    app.run(host="0.0.0.0", port=5000)
