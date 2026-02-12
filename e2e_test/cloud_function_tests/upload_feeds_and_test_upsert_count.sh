# Copyright 2023 Google LLC.
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

# CAUTION: THIS SCRIPT WILL REMOVE GCS BUCKETS AND BIG QUERY TABLES IN THE
# TARGET GCP PROJECT. IT IS FOR TESTING PURPOSES ONLY.

# This script will upload a set of files pointed to by the given directory arg,
# start Feedloader by uploading an EOF file, create a change in one file only,
# and re-upload the same feeds to test an upsert happens via Feedloader's
# Cloud Functions. The modified file is reverted to its original state at the
# end of this script so that it can be run idempotently.

export CLOUDSDK_PYTHON=/usr/bin/python2.7
if echo "$OSTYPE" | grep -q darwin
then
  GREEN='\x1B[1;32m'
  NOCOLOR='\x1B[0m'
  HYPERLINK='\x1B]8;;'
else
  GREEN='\033[1;32m'
  NOCOLOR='\033[0m'
  HYPERLINK='\033]8;;'
fi

print_green() {
  echo -e "${GREEN}$1${NOCOLOR}"
}

GCP_PROJECT=$1
FEED_PATH=$2
FEED_EXTENSION=$3
EOF_PATH=$4
if [[ -z "$1" ]]; then
    print_green "Usage: \$GCP_PROJECT \$FEED_PATH \$FEED_EXTENSION \$EOF_PATH. No GCP project ID was supplied. Please re-run with a GCP project ID as the first argument."
    exit 1
fi
if [[ -z "$2" ]]; then
    print_green "Usage: \$GCP_PROJECT \$FEED_PATH \$FEED_EXTENSION \$EOF_PATH. Please provide an absolute folder path to your feed files as the second argument."
    exit 1
fi
if [[ -z "$3" ]]; then
    print_green "Usage: \$GCP_PROJECT \$FEED_PATH \$FEED_EXTENSION \$EOF_PATH. Please provide the extension for your feed files without the dot."
    exit 1
fi
if [[ -z "$4" ]]; then
    print_green "Usage: \$GCP_PROJECT \$FEED_PATH \$FEED_EXTENSION \$EOF_PATH. Please provide an absolute file path to your empty EOF file."
    exit 1
fi

print_green  "Starting automation of feed file uploads to Feedloader. First cleaning the state of the GCS buckets and BQ tables..."
gcloud storage rm gs://"${GCP_PROJECT}"-update/*
gcloud storage rm gs://"${GCP_PROJECT}"-lock/*
gcloud storage rm gs://"${GCP_PROJECT}"-feed/*
gcloud storage rm gs://"${GCP_PROJECT}"-retrigger/*
gcloud storage rm gs://"${GCP_PROJECT}"-completed/*
bq rm -f feed_data.items
bq rm -f feed_data.streaming_items
bq rm -f feed_data.items_expiration_tracking

print_green "Deleted files and existing tables. Recreating tables..."
bq mk -t \
  --schema 'item_id:STRING,google_merchant_id:STRING,hashed_content:STRING,import_datetime:DATETIME' \
  feed_data.streaming_items
bq mk -t \
  --schema 'item_id:STRING,last_touched_date:DATE' \
  feed_data.items_expiration_tracking
print_green "Finished recreating tables. Proceeding to upload feeds to GCS..."

gcloud storage cp --gzip-in-flight="csv" "${FEED_PATH%/}"/*."${FEED_EXTENSION#.}" gs://"${GCP_PROJECT}"-feed
print_green "Finished uploading feed files. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a. Pausing before uploading EOF..."
sleep 120

gcloud storage cp --gzip-in-flight="csv" "${EOF_PATH}" gs://"${GCP_PROJECT}"-update
print_green "Finished uploading EOF. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a"
sleep 30

# Simulate deleting the items table (which Composer should do, but for the purposes of this test, we don't wait).
bq rm -f feed_data.items

# shellcheck disable=SC2061
FEED_FILES=$(find "${FEED_PATH%/}" -name *."${FEED_EXTENSION#.}")
ONE_FILE=$(echo "${FEED_FILES}" | head -1)
print_green "ONE FILE: ${ONE_FILE}"

# Save the second line.
TEMP_LINE=$(sed '2q;d' "$ONE_FILE")
print_green "TEMP LINE:\n${TEMP_LINE}"

# Update the second line and overwrite to create a diff.
PRODUCT_ID=$(echo "$TEMP_LINE" | cut -d$'\t' -f1)
REST_OF_LINE=$(echo "$TEMP_LINE" | cut -d$'\t' -f3-)
print_green "PRODUCT_ID: ${PRODUCT_ID}"
print_green "REST_OF_LINE:\n${REST_OF_LINE}"

# Delete the second line and overwrite to create a diff.
sed -i '' '2d' "$ONE_FILE" &
wait $!

sed -i '' '2i\'$'\n'"$PRODUCT_ID""$(printf '\t')Test$(printf '\t')""${REST_OF_LINE}"$'\n' "$ONE_FILE" &
wait $!

gcloud storage cp --gzip-in-flight="csv" "${FEED_PATH%/}"/*."${FEED_EXTENSION#.}" gs://"${GCP_PROJECT}"-feed
print_green "Finished uploading all feed files with an update included. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a. Pausing before uploading EOF..."
sleep 120

gcloud storage cp --gzip-in-flight="csv" "${EOF_PATH}" gs://"${GCP_PROJECT}"-update
print_green "Finished uploading EOF. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a"

# Restore the line to the test file.
sed -i '' '2d' "$ONE_FILE" &
wait $!

sed -i '' -e '2i\'$'\n'"${TEMP_LINE}"$'\n' "$ONE_FILE" &
wait $!

print_green "Automated feed uploader completed."
