# Copyright 2021 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

if [[ $# -ne 5 ]]
  then
    echo "Usage: sh start.sh [FEED PATH (WITHOUT TRAILING /)] [GCS FEED BUCKET] [GCS EOF BUCKET (WITHOUT TRAILING /)] [GCP PROJECT ID] [BIGQUERY DATASET NAME]"
    echo "***Please ensure that there are no non-feed files in the provided FEED PATH, otherwise unexpected behavior may occur!***"
    exit 1
fi

# Set the initial start timestamp and input params
eval FEEDPATH='$1'
FEEDBUCKET=$2
EOFBUCKET=$3
PROJECTNAME=$4
DATASETNAME=$5

RES="Failure"

REGEX="\{[0-9]+..[0-9]+\}"

# Since EOF files will be deleted after processing, if it exists then don't do anything yet.
check_eof="$(gsutil ls "$EOFBUCKET"/EOF 2>&1)"
if [[ $check_eof = *"One or more URLs matched no objects"* ]]; then
  echo "EOF not inside bucket at the moment, so proceeding with the automation"
else
  echo "An EOF file is already in the GCS bucket, which may mean processing is currently happening. This script will abort to prevent overlap."
  exit 1
fi

# Remove the items table to reset everything if it exists
check_items_exists="$(bq show "$PROJECTNAME:$DATASETNAME.items")"
if [[ $check_items_exists = *"Not found"* ]]; then
  echo "items table does not yet exist, will create on feed load..."
else
  echo "A pre-existing items table was found. Deleting the partition for today before proceeding..."
  partitionDate="$(date +%Y%m%d)"
  bq rm -f -t "$DATASETNAME.items\$$partitionDate"
fi

# If the streaming_items table doesn't exist, re-create it.
check_streaming_table="$(bq show "$PROJECTNAME:$DATASETNAME.streaming_items")"
if [[ $check_streaming_table = *"Not found"* ]]; then
  echo "streaming_items table does not currently exist. Creating it before proceeding..."
  bq mk -t --schema 'item_id:STRING,hashed_content:STRING,import_datetime:DATETIME' "$DATASETNAME.streaming_items"
fi

# If the items_to_delete table doesn't exist, re-create it.
check_delete_table="$(bq show "$PROJECTNAME:$DATASETNAME.items_to_delete")"
if [[ $check_delete_table = *"Not found"* ]]; then
  echo "items_to_delete table does not currently exist. Creating it before proceeding..."
  bq mk -t --schema 'item_id:STRING' "$DATASETNAME.items_to_delete"
fi

# If the items_to_upsert table doesn't exist, re-create it.
check_upsert_table="$(bq show "$PROJECTNAME:$DATASETNAME.items_to_upsert")"
if [[ $check_upsert_table = *"Not found"* ]]; then
  echo "items_to_upsert table does not currently exist. Creating it before proceeding..."
  bq mk -t --schema 'item_id:STRING' "$DATASETNAME.items_to_upsert"
fi

# Count the number of files you are going to upload
if [[ $FEEDPATH =~ $REGEX ]]; then
  NUMFILES="$(eval "ls $FEEDPATH" | wc -l)"
else
  NUMFILES="$(find "$FEEDPATH" -maxdepth 1 -type f -not -path '*/\.*' | wc -l | tr -d '[:space:]')"
fi

if [[ "$NUMFILES" -eq 0 ]]
then
  echo "No files detected in the provided path. Please try again."
  exit 1
fi

# Count the number of files you are going to upload
if [[ $FEEDPATH =~ $REGEX ]]; then
  NUMFILES="$(eval "ls $FEEDPATH" | wc -l)"
else
  NUMFILES="$(find "$FEEDPATH" -maxdepth 1 -type f -not -path '*/\.*' | wc -l | tr -d '[:space:]')"
fi

if [[ "$NUMFILES" = 0 ]]
then
  echo "No files detected in the provided path. Please try again."
  exit 1
fi

# Upload all the files
startDate="$(LC_TIME=en_US date -u)"
echo "Starting Feed Upload To GCS..."
if [[ $FEEDPATH =~ $REGEX ]]; then
  eval "gsutil cp -j csv $FEEDPATH $FEEDBUCKET"
else
  find "$FEEDPATH" -maxdepth 1 -type f -not -path '*/\.*' | sort | gsutil cp -I -j csv "$FEEDBUCKET"
fi

echo "$NUMFILES files uploaded. Comparing with number of success logs. This may take a while..."
sleep 30

# Check logs until number of success logs match the number of files uploaded
n=1
until [[ $n -ge 12 ]]
do
  echo "Checking Logs since $startDate - Attempt $n..."
  log="$(gcloud --quiet beta functions logs read importStorageFileIntoBigQuery --start-time "$startDate" --limit 1000 --filter "successfully")"
  numSuccesses="$(echo -n "$log" | grep -o "successfully" | wc -l | tr -d '[:space:]')"
  echo "$numSuccesses success logs found in Stackdriver Logs until now..."
  if [[ "$numSuccesses" = "$NUMFILES" && $numSuccesses -gt 0 ]]
  then
      echo "BigQuery loads all finished! Starting next automation process with EOF file..."
      touch ./EOF
      gsutil cp ./EOF "$EOFBUCKET"
      RES="Success"
      break
  fi
  n=$(($n+1))
  sleep 10
done

# Notify on success or failure, or check BigQuery import count if log check failed.
if [[ "$RES" = "Success" ]]
then
  echo "Feed Automation Kicked Off Successfully. Check For Further Logs and Alerts in Stackdriver."
else
  echo "Log count failed, trying one more verification with number of rows comparison. This may take a while..."
  if [[ $FEEDPATH =~ $REGEX ]]; then
    LINECOUNT="$(eval "wc -l $FEEDPATH" | tail -n1 | awk '{print $1;}')"
  else
    LINECOUNT="$(find "$FEEDPATH" -maxdepth 1 -type f -not -path '*/\.*' | xargs wc -l | tail -n1 | awk '{print $1;}')"
  fi

  NUMLINES=$((LINECOUNT - NUMFILES))
  COUNTROWSQUERY="SELECT COUNT(*) FROM \`$PROJECTNAME.$DATASETNAME.items\` WHERE _PARTITIONTIME = TIMESTAMP(CURRENT_DATE()) LIMIT 1"
  NUMROWS="$(bq query --nouse_legacy_sql "$COUNTROWSQUERY" | awk 'FNR == 5 {print $2;}')"
  echo "Looking for $NUMLINES in BigQuery..."
  echo "Found $NUMROWS in BigQuery"
  if [[ $NUMROWS = *"error"* ]]; then
    echo "Error occurred in item count attempt. Exiting..."
    exit 1
  fi
  if [[ $NUMLINES != "$NUMROWS" ]]
  then
    echo "Failure - One or more files did not upload to BigQuery Successfully. Aborting Processing..."
  else
    echo "BigQuery shows all rows were imported successfully. Starting next automation process with EOF file"
    touch ./EOF
    gsutil cp ./EOF "$EOFBUCKET"
    echo "Feed Automation Kicked Off Successfully. Check For Further Logs and Alerts in Stackdriver."
  fi
fi
