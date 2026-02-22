#!/bin/bash

# Ask the user for inputs step-by-step with defaults
read -rp "Enter path to leads file [leads-pf6.csv]: " LEADS
LEADS=${LEADS:-leads-pf6.csv}

read -rp "Enter path to email template [email-PL2.html]: " EMAIL
EMAIL=${EMAIL:-email-PL2.html}

read -rp "Enter campaign name as defined in config.yaml [campaign_PL]: " CAMPAIGN
CAMPAIGN=${CAMPAIGN:-campaign_PL}

read -rp "Enter max campaign duration in seconds [14220]: " DURATION
DURATION=${DURATION:-14220}

# Timestamp for logs
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="campaign_$TIMESTAMP.log"

# Move into project folder
cd ~/domains/nma.themysticaroma.com || exit 1
source venv/bin/activate
cd private_html || exit 1

# Display summary
echo "Running campaign:"
echo "- Leads: $LEADS"
echo "- Email: $EMAIL"
echo "- Campaign: $CAMPAIGN"
echo "- Max duration: $DURATION seconds"
echo "- Log file: $LOGFILE"
echo ""

# Run the campaign
nohup timeout "$DURATION" python3 main.py send --list "$LEADS" --email "$EMAIL" --campaign "$CAMPAIGN" > "$LOGFILE" 2>&1 &
