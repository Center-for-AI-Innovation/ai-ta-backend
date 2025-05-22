# delete these top lines in real usage... it's just instructions.
# crontab -e
# 2 2 * * * /teton/uiuc-chatbot/qdrant-backups/qdrant-backup-job.sh

#!/bin/bash

# Configuration
OUTPUT_FOLDER="~" # Folder where backups will be stored.
COLLECTION_NAME="uiuc-chatbot-quantized"
BASE_URL="<QDRANT_URL>" # CHANGE ME
API_KEY="<QDRANT_API_KEY>" # CHANGE ME

# Create snapshot and extract the name - this is a BLOCKING request. Typically 7 minutes.
echo "Starting to create a snapshot..."
create_response=$(curl -s -H "API-Key: $API_KEY" -X POST "$BASE_URL/collections/$COLLECTION_NAME/snapshots")
if [ $? -ne 0 ]; then
    echo "Failed to create snapshot"
    exit 1
fi

latest_snapshot=$(echo "$create_response" | jq -r '.result.name')
if [ -z "$latest_snapshot" ]; then
    echo "Failed to retrieve snapshot name from creation response"
    exit 1
fi

# Wait 30 minutes
# sleep 1800

# Download the latest snapshot to local
echo "Starting to downloading snapshot to local..."
curl -s -H "API-Key: $API_KEY" -o "$OUTPUT_FOLDER/$latest_snapshot" "$BASE_URL/collections/$COLLECTION_NAME/snapshots/$latest_snapshot"
if [ $? -ne 0 ]; then
    echo "Failed to download the latest snapshot"
    exit 1
fi
echo "Snapshot downloaded successfully: $latest_snapshot"

# Delete the latest snapshot
echo "Starting to delete the just-created snapshot from AWS..."
curl -s -H "API-Key: $API_KEY" -X DELETE "$BASE_URL/collections/$COLLECTION_NAME/snapshots/$latest_snapshot"
if [ $? -ne 0 ]; then
    echo "Failed to delete the latest snapshot"
    exit 1
fi
echo "Deleted snapshot from AWS successfully: $latest_snapshot"

echo "Script completed successfully."
