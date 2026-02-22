import imaplib
import email
import re
import os

# Mailbox config
IMAP_SERVER = os.getenv("IMAP_SERVER", "s2.cyber-folks.pl")
EMAIL_ACCOUNT = os.getenv("IMAP_EMAIL", "kedzielawski@themysticaroma.com")
PASSWORD = os.getenv("SMTP_PASSWORD")
if not PASSWORD:
    raise ValueError("SMTP_PASSWORD environment variable not set")

# Connect and login
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ACCOUNT, PASSWORD)

# Select INBOX (use readonly=True to avoid marking mails as read)
mail.select("INBOX", readonly=True)

# Search for all emails from Mail Delivery System
status, data = mail.search(None, "FROM", '"Mail Delivery System"')

bounced_emails = set()

for num in data[0].split():
    status, msg_data = mail.fetch(num, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])

    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ["text/plain", "message/delivery-status"]:
            body = part.get_payload(decode=True)
            if not body:
                continue
            text = body.decode(errors="ignore")

            # 1. Match Final-Recipient headers
            matches = re.findall(r"Final-Recipient:\s*rfc822;(.*)", text, re.IGNORECASE)
            bounced_emails.update([m.strip() for m in matches])

            # 2. Match plain email addresses (fallback)
            fallback_matches = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", text)
            bounced_emails.update(fallback_matches)

# Print results
print("\n📬 Bounced Emails:")
for addr in sorted(bounced_emails):
    print(f"- {addr}")
