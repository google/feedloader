# Copyright 2022 Google LLC.
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

# This script cleans up buckets and tables, automates the upload of feed files
# to Cloud Storage, waits 200 seconds, and uploads an EOF file to initiate
# Feedloader processing of feed files. It is NOT intended for production use.

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
gsutil rm gs://"${GCP_PROJECT}"-update/*
gsutil rm gs://"${GCP_PROJECT}"-lock/*
gsutil rm gs://"${GCP_PROJECT}"-feed/*
gsutil rm gs://"${GCP_PROJECT}"-retrigger/*
gsutil rm gs://"${GCP_PROJECT}"-completed/*
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

gsutil -m cp -j csv "${FEED_PATH%/}"/*."${FEED_EXTENSION#.}" gs://"${GCP_PROJECT}"-feed
print_green "Finished uploading feed files. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/import_storage_file_into_big_query?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a. Pausing before uploading EOF..."
sleep 200

gsutil -m cp -j csv "${EOF_PATH}" gs://"${GCP_PROJECT}"-update
print_green "Finished uploading EOF. You can check the logs at ${HYPERLINK}https://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs\ahttps://console.cloud.google.com/functions/details/us-central1/calculate_product_changes?project=${GCP_PROJECT}&tab=logs${HYPERLINK}\a"
print_green "Automated feed uploader completed."
