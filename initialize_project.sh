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

#!/bin/bash -u

# Import the environment variables to be used in this installer
source ./env.sh

# Constants
APP_ENGINE_REGION=us-central
BQ_FEED_DATASET=feed_data
BQ_MONITOR_DATASET=monitor_data
CLOUD_COMPOSER_ENV_NAME=process-completion-monitor
CLOUD_COMPOSER_ZONE=us-central1-f
DAG_ID=completion_monitor
EXPIRATION_TRACKING_TABLE_ID=items_expiration_tracking
ITEM_RESULTS_TABLE_ID=item_results
ITEMS_TABLE=items
KEYRING=cf_secret_keyring
KEYNAME=cf_secret_key
PROCESS_RESULT_TABLE_ID=process_result
REGION=us-central1
MAILER_SUBSCRIPTION=push-to-gae-mailer
CLOUD_BUILD_SUBSCRIPTION=push-to-gae-build-reporter

if echo "$OSTYPE" | grep -q darwin
then
  PUBSUB_TOKEN=$(uuidgen)
  GREEN='\x1B[1;32m'
  NOCOLOR='\x1B[0m'
  HYPERLINK='\x1B]8;;'
else
  PUBSUB_TOKEN=$(cat /proc/sys/kernel/random/uuid)
  GREEN='\033[1;32m'
  NOCOLOR='\033[0m'
  HYPERLINK='\033]8;;'
fi

print_green() {
  echo -e "${GREEN}$1${NOCOLOR}"
}

# Authorize with a Google Account.
gcloud auth login

# This is required to initialize BigQuery credentials and avoid hanging prompt
print_green "Initialize BigQuery if BigQuery has never been run before. Otherwise, you can skip this step."
bq init

print_green "Initialization started."

# Set default project.
gcloud config set project "$GCP_PROJECT"

# Enable all necessary APIs
print_green "Enabling Cloud APIs if necessary..."
REQUIRED_SERVICES=(
  appengine.googleapis.com
  appengineflex.googleapis.com
  bigquerydatatransfer.googleapis.com
  bigquery-json.googleapis.com
  cloudbuild.googleapis.com
  composer.googleapis.com
  cloudfunctions.googleapis.com
  cloudkms.googleapis.com
  cloudtasks.googleapis.com
  composer.googleapis.com
  logging.googleapis.com
  monitoring.googleapis.com
  pubsub.googleapis.com
  run.googleapis.com
  serviceusage.googleapis.com
  shoppingcontent.googleapis.com
  sourcerepo.googleapis.com
  storage-api.googleapis.com
  storage-component.googleapis.com
)

ENABLED_SERVICES=$(gcloud services list)
for SERVICE in "${REQUIRED_SERVICES[@]}"
do
  if echo "$ENABLED_SERVICES" | grep -q "$SERVICE"
  then
    echo "$SERVICE is already enabled."
  else
    gcloud services enable "$SERVICE" \
      && echo "$SERVICE has been successfully enabled."
  fi
done

# Create Cloud Storage buckets
print_green "Creating Cloud Storage Buckets..."
STORAGE_BUCKETS=(
"$ARCHIVE_BUCKET"
"$COMPLETED_FILES_BUCKET"
"$FEED_BUCKET"
"$LOCK_BUCKET"
"$MONITOR_BUCKET"
"$RETRIGGER_BUCKET"
"$TRIGGER_COMPLETION_BUCKET"
"$UPDATE_BUCKET"
)

for BUCKET in "${STORAGE_BUCKETS[@]}"
do
  BUCKET_CREATE_RESULT=$(gsutil mb -l "$REGION" "$BUCKET" 2>&1)
  if [[ $BUCKET_CREATE_RESULT = *"Did you mean to use a gs:// URL"* ]]
  then
    gsutil mb -l "$REGION" "gs://$BUCKET"
  fi
done

# Use the .json file from here for the bucket lifecycle command below
# (Strip gs:// if it exists and re-add it to ensure valid bucket name)
ARCHIVE_BUCKET=${ARCHIVE_BUCKET/gs:\/\/}
ARCHIVE_BUCKET="gs://$ARCHIVE_BUCKET"
gsutil lifecycle set archive_bucket_lifecycle.json "$ARCHIVE_BUCKET"

# Create service accounts
print_green "Creating service accounts and clearing their keys..."
DeleteAllServiceAccountKeys() {
  SERVICE_ACCOUNT_EMAIL=$1
  for KEY in $(gcloud alpha iam service-accounts keys list --iam-account="$SERVICE_ACCOUNT_EMAIL" --managed-by="user" | awk '{if(NR>1)print}' | awk '{print $1;}')
  do
    gcloud alpha iam service-accounts keys -q delete "$KEY" --iam-account="$SERVICE_ACCOUNT_EMAIL"
  done
}
FEED_SERVICE_ACCOUNT=shopping-feed@"$GCP_PROJECT".iam.gserviceaccount.com
if gcloud iam service-accounts list | grep -q "$FEED_SERVICE_ACCOUNT"
then
  echo "$FEED_SERVICE_ACCOUNT already exists."
else
  gcloud iam service-accounts create shopping-feed --display-name "Shopping Feed"
  gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
    --member serviceAccount:"$FEED_SERVICE_ACCOUNT" \
    --role roles/editor \
    && echo "$FEED_SERVICE_ACCOUNT has been successfully created."
fi
DeleteAllServiceAccountKeys "$FEED_SERVICE_ACCOUNT"

MC_SERVICE_ACCOUNT=merchant-center@"$GCP_PROJECT".iam.gserviceaccount.com
if gcloud iam service-accounts list | grep -q "$MC_SERVICE_ACCOUNT"
then
  echo "$MC_SERVICE_ACCOUNT already exists."
else
  gcloud iam service-accounts create merchant-center --display-name "Merchant Center Service Account"
  gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
    --member serviceAccount:"$MC_SERVICE_ACCOUNT" \
    --role roles/editor \
    && echo "$MC_SERVICE_ACCOUNT has been successfully created."
fi
DeleteAllServiceAccountKeys "$MC_SERVICE_ACCOUNT"

# Create BigQuery datasets and tables
print_green "Creating BigQuery datasets and tables..."
if bq ls --all | grep -q -w "$BQ_FEED_DATASET"
then
  echo "$BQ_FEED_DATASET already exists."
else
  bq --location=US mk -d "$BQ_FEED_DATASET"
fi

bq rm -f "$BQ_FEED_DATASET".streaming_items
bq mk -t \
  --schema 'item_id:STRING,google_merchant_id:STRING,hashed_content:STRING,import_datetime:DATETIME' \
  "$BQ_FEED_DATASET".streaming_items

bq rm -f "$BQ_FEED_DATASET".items_to_delete
bq mk -t --schema 'item_id:STRING,google_merchant_id:STRING' "$BQ_FEED_DATASET".items_to_delete

bq rm -f "$BQ_FEED_DATASET".items_to_upsert
bq mk -t --schema 'item_id:STRING' "$BQ_FEED_DATASET".items_to_upsert

bq rm -f "$BQ_FEED_DATASET".items_expiration_tracking
bq mk -t \
  --schema 'item_id:STRING,last_touched_date:DATE' \
  "$BQ_FEED_DATASET".items_expiration_tracking

bq rm -f "$BQ_FEED_DATASET".items_to_prevent_expiring
bq mk -t \
  --schema 'item_id:STRING' \
  "$BQ_FEED_DATASET".items_to_prevent_expiring

if bq ls --all | grep -q -w "$BQ_MONITOR_DATASET"
then
  echo "$BQ_MONITOR_DATASET already exists."
else
  bq --location US mk -d "$BQ_MONITOR_DATASET"
fi

bq rm -f "$BQ_MONITOR_DATASET"."$PROCESS_RESULT_TABLE_ID"
bq mk -t "$BQ_MONITOR_DATASET"."$PROCESS_RESULT_TABLE_ID" \
  operation:STRING,timestamp:STRING,batch_id:INTEGER,success_count:INTEGER,failure_count:INTEGER,skipped_count:INTEGER

bq rm -f "$BQ_MONITOR_DATASET"."$ITEM_RESULTS_TABLE_ID"
bq mk -t "$BQ_MONITOR_DATASET"."$ITEM_RESULTS_TABLE_ID" \
  item_id:STRING,batch_id:INTEGER,operation:STRING,result:STRING,error:STRING,timestamp:STRING

print_green "Creating scheduled Queries..."
# Remove any pre-existing scheduled queries from the Cloud project
EXISTING_SCHEDULED_QUERIES=$(bq ls --transfer_config --transfer_location=us --project_id="$GCP_PROJECT" | grep projects | awk '{print $1}')

for SCHEDULED_QUERY in $(echo "$EXISTING_SCHEDULED_QUERIES")
do
  bq rm -f --transfer_config "$SCHEDULED_QUERY"
done

# Schedule a query to cleanup the streaming_items table periodically
bq query \
    --use_legacy_sql=false \
    --destination_table="$BQ_FEED_DATASET".streaming_items \
    --display_name='Cleanup streaming_items older than 30 days' \
    --schedule='every 730 hours' \
    --replace=true \
    "SELECT
      *
    FROM
      $BQ_FEED_DATASET.streaming_items
    WHERE import_datetime > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY)"

# Schedule a query to cleanup the item_results table periodically
bq query \
    --use_legacy_sql=false \
    --destination_table="$BQ_MONITOR_DATASET".item_results \
    --display_name='Cleanup item_results older than 30 days' \
    --schedule='every 730 hours' \
    --replace=true \
    "SELECT
      *
    FROM
      $BQ_MONITOR_DATASET.item_results
    WHERE timestamp > FORMAT_DATETIME(\"%Y%m%d\", DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY))"

# Create Cloud Pub/Sub topics
print_green "Creating PubSub topics and subscriptions..."
REQUIRED_TOPICS=(
  mailer-trigger
)
EXISTING_TOPICS=$(gcloud pubsub topics list)
for TOPIC in "${REQUIRED_TOPICS[@]}"
do
  if echo "$EXISTING_TOPICS" | grep -q "$TOPIC"
  then
    echo "Pub/Sub topic $TOPIC already exists."
  else
    gcloud pubsub topics create "$TOPIC" \
      && echo "Pub/Sub topic $TOPIC has been successfully created."
  fi
done

if gcloud beta pubsub subscriptions list | grep -q "$MAILER_SUBSCRIPTION"
then
  echo "Pub/Sub subscription $MAILER_SUBSCRIPTION already exists."
else
  gcloud beta pubsub subscriptions create \
      --topic mailer-trigger \
      --push-endpoint https://mailer-dot-"$GCP_PROJECT".appspot.com/pubsub/push?token="$PUBSUB_TOKEN" \
      --ack-deadline 600 \
      --expiration-period never \
      --message-retention-duration 10m \
      "$MAILER_SUBSCRIPTION" \
    && echo "Pub/Sub subscription $MAILER_SUBSCRIPTION has been successfully created."
fi

if gcloud beta pubsub subscriptions list | grep -q "$CLOUD_BUILD_SUBSCRIPTION"
then
  echo "Pub/Sub subscription $CLOUD_BUILD_SUBSCRIPTION already exists."
else
  gcloud beta pubsub subscriptions create \
      --topic cloud-builds \
      --push-endpoint https://build-reporter-dot-"$GCP_PROJECT".appspot.com/pubsub/push \
      --ack-deadline 600 \
      --expiration-period never \
      --message-retention-duration 10m \
      "$CLOUD_BUILD_SUBSCRIPTION" \
    && echo "Pub/Sub subscription $CLOUD_BUILD_SUBSCRIPTION has been successfully created."
fi

# Set up AppEngine
print_green "Setting up AppEngine..."
if ! gcloud app describe &> /dev/null; then
gcloud app create --region="$APP_ENGINE_REGION"
fi

# Set up AppEngine service account
print_green "Creating AppEngine service account keys..."
APP_ENGINE_SERVICE_ACCOUNT_KEYS=(
  appengine/uploader/config/gcp_service_account.json
  appengine/initiator/config/service_account.json
)
for SERVICE_ACCOUNT_KEY in "${APP_ENGINE_SERVICE_ACCOUNT_KEYS[@]}"
do
  if [[ -f "$SERVICE_ACCOUNT_KEY" ]]
  then
    echo "Service account key $SERVICE_ACCOUNT_KEY already exists. Deleting and recreating it..."
    rm "$SERVICE_ACCOUNT_KEY"
  fi
  gcloud iam service-accounts keys create \
      "$SERVICE_ACCOUNT_KEY" \
      --iam-account "$FEED_SERVICE_ACCOUNT" \
    && echo "Service account key $SERVICE_ACCOUNT_KEY has been successfully created."
done

MC_SERVICE_ACCOUNT_KEY=appengine/uploader/config/mc_service_account.json
if [[ -f "$MC_SERVICE_ACCOUNT_KEY" ]]
then
  echo "Service account key $MC_SERVICE_ACCOUNT_KEY already exists."
  rm "$MC_SERVICE_ACCOUNT_KEY"
fi
gcloud iam service-accounts keys create \
      "$MC_SERVICE_ACCOUNT_KEY" \
      --iam-account merchant-center@"$GCP_PROJECT".iam.gserviceaccount.com \
    && echo "Service account key $MC_SERVICE_ACCOUNT_KEY has been successfully created."

# Create a new Cloud Source Repository for Git
print_green "Creating the Git repository..."
EXISTING_REPOS="$(gcloud source repos list --filter="$SOURCE_REPO")"
if echo "$EXISTING_REPOS" | grep -q -w "$SOURCE_REPO"
then
  echo "Cloud Source Repository $SOURCE_REPO already exists."
else
  gcloud source repos create "$SOURCE_REPO"
fi

# Set up Cloud Composer
print_green "Setting up Cloud Composer. This could take a while..."
gcloud components install kubectl
if gcloud beta composer environments list --locations="$REGION" | grep -w "$CLOUD_COMPOSER_ENV_NAME" &> /dev/null
then
  echo "Cloud Composer Environment $CLOUD_COMPOSER_ENV_NAME already exists"
else
  gcloud composer environments create "$CLOUD_COMPOSER_ENV_NAME" \
    --airflow-configs=core-default_timezone=Asia/Tokyo \
    --location "$REGION" \
    --zone "$CLOUD_COMPOSER_ZONE" \
    --python-version 3 \
    --machine-type n1-standard-1 \
    --image-version composer-1.8.0-airflow-1.10.3 \
    --env-variables PROJECT_ID="$GCP_PROJECT",LOCATION="$REGION",PUBSUB_TOPIC="mailer-trigger"
fi

COMPOSER_UPDATE_RESULT=$(gcloud composer environments update "$CLOUD_COMPOSER_ENV_NAME" \
 --update-pypi-packages-from-file composer/completion_monitor/requirements.txt \
 --location "$REGION" 2>&1)

sed -e "s/<DAG_ID>/$DAG_ID/g" \
  -e "s/<GCP_PROJECT_ID>/$GCP_PROJECT/g" \
  -e "s/<QUEUE_LOCATION>/$REGION/g" \
  -e "s/<MONITOR_DATASET_ID>/$BQ_MONITOR_DATASET/g" \
  -e "s/<TIMEZONE_UTC_OFFSET>/$TIMEZONE_UTC_OFFSET/g" \
  -e "s/<FEED_DATASET_ID>/$BQ_FEED_DATASET/g" \
  -e "s/<ITEMS_TABLE_ID>/$ITEMS_TABLE/g" \
  -e "s/<EXPIRATION_TRACKING_TABLE_ID>/$EXPIRATION_TRACKING_TABLE_ID/g" \
  -e "s/<ITEM_RESULTS_TABLE_ID>/$ITEM_RESULTS_TABLE_ID/g" \
  -e "s%<LOCK_BUCKET>%$LOCK_BUCKET%g" \
  composer/completion_monitor/config/variables_template.json > composer/completion_monitor/config/variables.json

print_green "Creating service account keys for Cloud Composer..."
AIRFLOW_SERVICE_ACCOUNT_KEY=composer/completion_monitor/config/service_account.json
if [[ -f "$AIRFLOW_SERVICE_ACCOUNT_KEY" ]]
then
  echo "Service account key $AIRFLOW_SERVICE_ACCOUNT_KEY already exists."
  rm "$AIRFLOW_SERVICE_ACCOUNT_KEY"
fi
gcloud iam service-accounts keys create \
      "$AIRFLOW_SERVICE_ACCOUNT_KEY" \
      --iam-account "$FEED_SERVICE_ACCOUNT" \
    && echo "Service account key $AIRFLOW_SERVICE_ACCOUNT_KEY has been successfully created."

# Extract Cloud Composer Airflow Webserver ID
AIRFLOW_WEBSERVER_ID=$(gcloud composer environments describe "$CLOUD_COMPOSER_ENV_NAME" --location "$REGION" | grep -E "airflowUri" | awk '{print $NF}')
print_green "Returned Airflow Webserver ID was: $AIRFLOW_WEBSERVER_ID"

# Delete existing policies in case they are being re-created
print_green "Checking what alerting policies are already installed and deleting them first..."
EXISTING_POLICIES="$(gcloud alpha monitoring policies list | grep "name:" | grep -v "conditions" | awk '{printf("%s\n", $2)}')"
for POLICY_ID in $(echo "$EXISTING_POLICIES")
do
  gcloud alpha monitoring policies -q delete "$POLICY_ID"
done

# Create log metrics for alerts
print_green "Removing and re-creating log metrics..."
EXISTING_METRICS=$(gcloud logging metrics list)

IMPORT_STORAGE_FILE_ERRORS=ImportStorageFileErrors
if echo "$EXISTING_METRICS" | grep -q -w "$IMPORT_STORAGE_FILE_ERRORS"
then
  echo "Metric $IMPORT_STORAGE_FILE_ERRORS already exists. Deleting it and recreating..."
  gcloud logging metrics -q delete $IMPORT_STORAGE_FILE_ERRORS
fi
gcloud logging metrics create "$IMPORT_STORAGE_FILE_ERRORS" \
  --description="Count of the importStorageFileInfoBigQuery Cloud Function Errors" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileInfoBigQuery\" severity>=ERROR" \
  && echo "Metric $IMPORT_STORAGE_FILE_ERRORS has been successfully created."

BQ_LOAD_SUCCESSES=BQLoadSuccesses
if echo "$EXISTING_METRICS" | grep -q -w "$BQ_LOAD_SUCCESSES"
then
  echo "Metric $BQ_LOAD_SUCCESSES already exists."
  gcloud logging metrics -q delete $BQ_LOAD_SUCCESSES
fi
gcloud logging metrics create "$BQ_LOAD_SUCCESSES" \
  --description="Count of the importStorageFileInfoBigQuery Cloud Function Successes" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"successfully\"" \
  && echo "Metric $BQ_LOAD_SUCCESSES has been successfully created."

IMPORT_FUNCTION_TIMEOUT_ERRORS=ImportFunctionTimeouts
if echo "$EXISTING_METRICS" | grep -q -w "$IMPORT_FUNCTION_TIMEOUT_ERRORS"
then
  echo "Metric $IMPORT_FUNCTION_TIMEOUT_ERRORS already exists."
  gcloud logging metrics -q delete $IMPORT_FUNCTION_TIMEOUT_ERRORS
fi
gcloud logging metrics create "$IMPORT_FUNCTION_TIMEOUT_ERRORS" \
  --description="Count of the errors when importStorageFileInfoBigQuery function times out." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"Dropping event\"" \
  && echo "Metric $IMPORT_FUNCTION_TIMEOUT_ERRORS has been successfully created."

INVALID_FILE_RANGE_ERRORS=InvalidFileRangeErrors
if echo "$EXISTING_METRICS" | grep -q -w "$INVALID_FILE_RANGE_ERRORS"
then
  echo "Metric $INVALID_FILE_RANGE_ERRORS already exists."
  gcloud logging metrics -q delete $INVALID_FILE_RANGE_ERRORS
fi
gcloud logging metrics create "$INVALID_FILE_RANGE_ERRORS" \
  --description="Count of the errors when the FILE_RANGE environment variable in importStorageFileInfoBigQuery has an invalid format." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"FILE_RANGE environment variable is incorrectly formatted\"" \
  && echo "Metric $INVALID_FILE_RANGE_ERRORS has been successfully created."

INVALID_FILE_NAME_ERRORS=InvalidFileNameErrors
if echo "$EXISTING_METRICS" | grep -q -w "$INVALID_FILE_NAME_ERRORS"
then
  echo "Metric $INVALID_FILE_NAME_ERRORS already exists."
  gcloud logging metrics -q delete $INVALID_FILE_NAME_ERRORS
fi
gcloud logging metrics create "$INVALID_FILE_NAME_ERRORS" \
  --description="Count of the errors when the file name processed by importStorageFileInfoBigQuery has an invalid format." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"Filename was not properly numbered\"" \
  && echo "Metric $INVALID_FILE_NAME_ERRORS has been successfully created."

BQ_LOAD_FAILURES=BQLoadFailures
if echo "$EXISTING_METRICS" | grep -q -w "$BQ_LOAD_FAILURES"
then
  echo "Metric $BQ_LOAD_FAILURES already exists."
  gcloud logging metrics -q delete $BQ_LOAD_FAILURES
fi
gcloud logging metrics create "$BQ_LOAD_FAILURES" \
  --description="Count of the importStorageFileInfoBigQuery Cloud Function BigQuery load failures" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" (\"BigQuery load job failed\" OR \"One or more errors occurred while loading the feed\")" \
  && echo "Metric $BQ_LOAD_FAILURES has been successfully created."

GCS_IMPORTED_FILENAME_SAVE_FAILURES=GCSImportedFilenameSaveFailures
if echo "$EXISTING_METRICS" | grep -q -w "$GCS_IMPORTED_FILENAME_SAVE_FAILURES"
then
  echo "Metric $GCS_IMPORTED_FILENAME_SAVE_FAILURES already exists."
  gcloud logging metrics -q delete $GCS_IMPORTED_FILENAME_SAVE_FAILURES
fi
gcloud logging metrics create "$GCS_IMPORTED_FILENAME_SAVE_FAILURES" \
  --description="Count of the importStorageFileInfoBigQuery Cloud Function errors of failed GCS inserts of the completed filename" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"An error occurred when saving the completed filename to GCS\"" \
  && echo "Metric $GCS_IMPORTED_FILENAME_SAVE_FAILURES has been successfully created."

EOF_EXISTS_DURING_IMPORT_ERRORS=EOFExistsErrors
if echo "$EXISTING_METRICS" | grep -q -w "$EOF_EXISTS_DURING_IMPORT_ERRORS"
then
  echo "Metric $EOF_EXISTS_DURING_IMPORT_ERRORS already exists."
  gcloud logging metrics -q delete $EOF_EXISTS_DURING_IMPORT_ERRORS
fi
gcloud logging metrics create "$EOF_EXISTS_DURING_IMPORT_ERRORS" \
  --description="Count of the importStorageFileInfoBigQuery Cloud Function errors due to existing EOF preventing concurrent runs from starting" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" resource.labels.region=\"us-central1\" \"The bucket must be empty before loading feed data. Exiting\""	 \
  && echo "Metric $EOF_EXISTS_DURING_IMPORT_ERRORS has been successfully created."

CALCULATE_PRODUCT_CHANGES_ERRORS=CalculateProductChangesErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CALCULATE_PRODUCT_CHANGES_ERRORS"
then
  echo "Metric $CALCULATE_PRODUCT_CHANGES_ERRORS already exists."
  gcloud logging metrics -q delete $CALCULATE_PRODUCT_CHANGES_ERRORS
fi
gcloud logging metrics create "$CALCULATE_PRODUCT_CHANGES_ERRORS" \
  --description="Count of the calculateProductChanges Cloud Function Errors" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" severity>=ERROR" \
  && echo "Metric $CALCULATE_PRODUCT_CHANGES_ERRORS has been successfully created."

SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD=SchemaConfigParseFailureForLoad
if echo "$EXISTING_METRICS" | grep "$SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD"
then
  echo "Metric $SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD already exists."
  gcloud logging metrics -q delete $SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD
fi
gcloud logging metrics create "$SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD" \
  --description="Logs indicating that the schema config file failed to be parsed for creating the items table" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"importStorageFileIntoBigQuery\" \"Unable to map any columns from the schema config\"" \
  && echo "Metric $SCHEMA_CONFIG_PARSE_FAILURE_FOR_LOAD has been successfully created."

SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION=SchemaConfigParseFailureForCalculation
if echo "$EXISTING_METRICS" | grep "$SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION"
then
  echo "Metric $SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION already exists."
  gcloud logging metrics -q delete $SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION
fi
gcloud logging metrics create "$SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION" \
  --description="Logs indicating that the schema config file failed to be injected into the SQL queries" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Unable to map any columns from the schema config\"" \
  && echo "Metric $SCHEMA_CONFIG_PARSE_FAILURE_FOR_CALCULATION has been successfully created."

DELETES_THRESHOLD_CROSSED=DeletesThresholdCrossed
if echo "$EXISTING_METRICS" | grep -q -w "$DELETES_THRESHOLD_CROSSED"
then
  echo "Metric $DELETES_THRESHOLD_CROSSED already exists."
  gcloud logging metrics -q delete $DELETES_THRESHOLD_CROSSED
fi
gcloud logging metrics create "$DELETES_THRESHOLD_CROSSED" \
  --description="Logs indicating that the delete count crossed the allowed delete threshold amount" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"crossed deletes threshold\"" \
  && echo "Metric $DELETES_THRESHOLD_CROSSED has been successfully created."

UPSERTS_THRESHOLD_CROSSED=UpsertsThresholdCrossed
if echo "$EXISTING_METRICS" | grep "$UPSERTS_THRESHOLD_CROSSED"
then
  echo "Metric $UPSERTS_THRESHOLD_CROSSED already exists."
  gcloud logging metrics -q delete $UPSERTS_THRESHOLD_CROSSED
fi
gcloud logging metrics create "$UPSERTS_THRESHOLD_CROSSED" \
  --description="Logs indicating that the upsert count crossed the allowed upsert threshold amount" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"crossed upserts threshold\"" \
  && echo "Metric $UPSERTS_THRESHOLD_CROSSED has been successfully created."

COUNT_DELETES_QUERY_FAILURES=CountDeletesQueryFailures
if echo "$EXISTING_METRICS" | grep -q -w "$COUNT_DELETES_QUERY_FAILURES"
then
  echo "Metric $COUNT_DELETES_QUERY_FAILURES already exists."
  gcloud logging metrics -q delete $COUNT_DELETES_QUERY_FAILURES
fi
gcloud logging metrics create "$COUNT_DELETES_QUERY_FAILURES" \
  --description="Count of the COUNT_DELETES_QUERY job errors in calculateProductChanges" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Delete count and publish job failed\"" \
  && echo "Metric $COUNT_DELETES_QUERY_FAILURES has been successfully created."

COUNT_UPSERTS_QUERY_FAILURES=CountUpsertsQueryFailures
if echo "$EXISTING_METRICS" | grep -q -w "$COUNT_UPSERTS_QUERY_FAILURES"
then
  echo "Metric $COUNT_UPSERTS_QUERY_FAILURES already exists."
  gcloud logging metrics -q delete $COUNT_UPSERTS_QUERY_FAILURES
fi
gcloud logging metrics create "$COUNT_UPSERTS_QUERY_FAILURES" \
  --description="Count of the COUNT_UPSERTS_QUERY job errors in calculateProductChanges" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Upsert count and publish job failed\"" \
  && echo "Metric $COUNT_UPSERTS_QUERY_FAILURES has been successfully created."

INVALID_EOF_ERRORS=InvalidEofErrors
if echo "$EXISTING_METRICS" | grep -q -w "$INVALID_EOF_ERRORS"
then
  echo "Metric $INVALID_EOF_ERRORS already exists."
  gcloud logging metrics -q delete $INVALID_EOF_ERRORS
fi
gcloud logging metrics create "$INVALID_EOF_ERRORS" \
  --description="Count of the errors in calculateProductChanges for invalid EOF upload" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"File was not an empty EOF\"" \
  && echo "Metric $INVALID_EOF_ERRORS has been successfully created."

EOF_LOCK_CHECK_ERRORS=EofLockCheckErrors
if echo "$EXISTING_METRICS" | grep -q -w "$EOF_LOCK_CHECK_ERRORS"
then
  echo "Metric $EOF_LOCK_CHECK_ERRORS already exists."
  gcloud logging metrics -q delete $EOF_LOCK_CHECK_ERRORS
fi
gcloud logging metrics create "$EOF_LOCK_CHECK_ERRORS" \
  --description="Count of the errors in calculateProductChanges for EOF.lock check failure" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"EOF.lock check failed\"" \
  && echo "Metric $EOF_LOCK_CHECK_ERRORS has been successfully created."

EOF_LOCKED_ERRORS=EofLockedErrors
if echo "$EXISTING_METRICS" | grep -q -w "$EOF_LOCKED_ERRORS"
then
  echo "Metric $EOF_LOCKED_ERRORS already exists."
  gcloud logging metrics -q delete $EOF_LOCKED_ERRORS
fi
gcloud logging metrics create "$EOF_LOCKED_ERRORS" \
  --description="Count of the errors in calculateProductChanges for locked EOF errors" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"An EOF.lock file was found, which indicates that this function is still running\"" \
  && echo "Metric $EOF_LOCKED_ERRORS has been successfully created."

EOF_LOCK_FAILED_ERRORS=EofLockFailedErrors
if echo "$EXISTING_METRICS" | grep -q -w "$EOF_LOCK_FAILED_ERRORS"
then
  echo "Metric $EOF_LOCK_FAILED_ERRORS already exists."
  gcloud logging metrics -q delete $EOF_LOCK_FAILED_ERRORS
fi
gcloud logging metrics create "$EOF_LOCK_FAILED_ERRORS" \
  --description="Count of the errors in calculateProductChanges for EOF lock failed errors" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"EOF could not be locked! Exiting Function\"" \
  && echo "Metric $EOF_LOCK_FAILED_ERRORS has been successfully created."

EOF_UNLOCK_FAILED_ERRORS=EofUnlockFailedErrors
if echo "$EXISTING_METRICS" | grep -q -w "$EOF_UNLOCK_FAILED_ERRORS"
then
  echo "Metric $EOF_UNLOCK_FAILED_ERRORS already exists."
  gcloud logging metrics -q delete $EOF_UNLOCK_FAILED_ERRORS
fi
gcloud logging metrics create "$EOF_UNLOCK_FAILED_ERRORS" \
  --description="Count of the errors in calculateProductChanges for EOF unlock failed errors" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Subsequent runs will be blocked unless the lock file is removed\"" \
  && echo "Metric $EOF_UNLOCK_FAILED_ERRORS has been successfully created."

NONEXISTENT_TABLE_ERRORS=NonexistentTableErrors
if echo "$EXISTING_METRICS" | grep -q -w "$NONEXISTENT_TABLE_ERRORS"
then
  echo "Metric $NONEXISTENT_TABLE_ERRORS already exists."
  gcloud logging metrics -q delete $NONEXISTENT_TABLE_ERRORS
fi
gcloud logging metrics create "$NONEXISTENT_TABLE_ERRORS" \
  --description="Count of the errors in calculateProductChanges for a nonexistent table" \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"table must exist before\"" \
  && echo "Metric $NONEXISTENT_TABLE_ERRORS has been successfully created."

CALCULATE_DELETES_ERRORS=CalculateDeletesErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CALCULATE_DELETES_ERRORS"
then
  echo "Metric $CALCULATE_DELETES_ERRORS already exists."
  gcloud logging metrics -q delete $CALCULATE_DELETES_ERRORS
fi
gcloud logging metrics create "$CALCULATE_DELETES_ERRORS" \
  --description="Count of the runQueryJob errors in calculateProductChanges for CALCULATE_ITEMS_FOR_DELETION_QUERY." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"CALCULATE_ITEMS_FOR_DELETION_QUERY job failed\"" \
  && echo "Metric $CALCULATE_DELETES_ERRORS has been successfully created."

CALCULATE_UPDATES_ERRORS=CalculateUpdatesErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CALCULATE_UPDATES_ERRORS"
then
  echo "Metric $CALCULATE_UPDATES_ERRORS already exists."
  gcloud logging metrics -q delete $CALCULATE_UPDATES_ERRORS
fi
gcloud logging metrics create "$CALCULATE_UPDATES_ERRORS" \
  --description="Count of the runQueryJob errors in calculateProductChanges for CALCULATE_ITEMS_FOR_UPDATE_QUERY." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"CALCULATE_ITEMS_FOR_UPDATE_QUERY job failed\"" \
  && echo "Metric $CALCULATE_UPDATES_ERRORS has been successfully created."

CALCULATE_INSERTS_ERRORS=CalculateInsertsErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CALCULATE_INSERTS_ERRORS"
then
  echo "Metric $CALCULATE_INSERTS_ERRORS already exists."
  gcloud logging metrics -q delete $CALCULATE_INSERTS_ERRORS
fi
gcloud logging metrics create "$CALCULATE_INSERTS_ERRORS" \
  --description="Count of the runQueryJob errors in calculateProductChanges for CALCULATE_ITEMS_FOR_INSERTION_QUERY." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"CALCULATE_ITEMS_FOR_INSERTION_QUERY job failed.\"" \
  && echo "Metric $CALCULATE_INSERTS_ERRORS has been successfully created."

CHANGES_COUNT_QUERY_ERRORS=ChangesCountQueryErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CHANGES_COUNT_QUERY_ERRORS"
then
  echo "Metric $CHANGES_COUNT_QUERY_ERRORS already exists."
  gcloud logging metrics -q delete $CHANGES_COUNT_QUERY_ERRORS
fi
gcloud logging metrics create "$CHANGES_COUNT_QUERY_ERRORS" \
  --description="Count of the errors in calculateProductChanges when extracting the count of changes." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"could not be extracted from the query result\"" \
  && echo "Metric $CHANGES_COUNT_QUERY_ERRORS has been successfully created."

CREATE_TASK_EMPTY_RESPONSE_ERROR=CreateTaskEmptyResponseErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CREATE_TASK_EMPTY_RESPONSE_ERROR"
then
  echo "Metric $CREATE_TASK_EMPTY_RESPONSE_ERROR already exists."
  gcloud logging metrics -q delete $CREATE_TASK_EMPTY_RESPONSE_ERROR
fi
gcloud logging metrics create "$CREATE_TASK_EMPTY_RESPONSE_ERROR" \
  --description="Count of the Task creation empty responses in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Task creation returned unexpected response\"" \
  && echo "Metric $CREATE_TASK_EMPTY_RESPONSE_ERROR has been successfully created."

CREATE_TASK_EXCEPTION_ERRORS=CreateTaskExceptionErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CREATE_TASK_EXCEPTION_ERRORS"
then
  echo "Metric $CREATE_TASK_EXCEPTION_ERRORS already exists."
  gcloud logging metrics -q delete $CREATE_TASK_EXCEPTION_ERRORS
fi
gcloud logging metrics create "$CREATE_TASK_EXCEPTION_ERRORS" \
  --description="Count of the Task creation exceptions in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Error occurred when creating a Task to start GAE\"" \
  && echo "Metric $CREATE_TASK_EXCEPTION_ERRORS has been successfully created."

ATTEMPTED_FILES_RETRIEVAL_ERRORS=AttemptedFilesRetrievalErrors
if echo "$EXISTING_METRICS" | grep -q -w "$ATTEMPTED_FILES_RETRIEVAL_ERRORS"
then
  echo "Metric $ATTEMPTED_FILES_RETRIEVAL_ERRORS already exists."
  gcloud logging metrics -q delete $ATTEMPTED_FILES_RETRIEVAL_ERRORS
fi
gcloud logging metrics create "$ATTEMPTED_FILES_RETRIEVAL_ERRORS" \
  --description="Count of the attemptedFiles retrieval errors in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"attemptedFiles retrieval failed\"" \
  && echo "Metric $ATTEMPTED_FILES_RETRIEVAL_ERRORS has been successfully created."

IMPORTED_FILES_RETRIEVAL_ERRORS=ImportedFilesRetrievalErrors
if echo "$EXISTING_METRICS" | grep -q -w "$IMPORTED_FILES_RETRIEVAL_ERRORS"
then
  echo "Metric $IMPORTED_FILES_RETRIEVAL_ERRORS already exists."
  gcloud logging metrics -q delete $IMPORTED_FILES_RETRIEVAL_ERRORS
fi
gcloud logging metrics create "$IMPORTED_FILES_RETRIEVAL_ERRORS" \
  --description="Count of the importedFiles retrieval errors in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"importedFiles retrieval failed\"" \
  && echo "Metric $IMPORTED_FILES_RETRIEVAL_ERRORS has been successfully created."

DELETE_IMPORTED_FILENAMES_ERRORS=DeleteImportedFilenamesErrors
if echo "$EXISTING_METRICS" | grep -q -w "$DELETE_IMPORTED_FILENAMES_ERRORS"
then
  echo "Metric $DELETE_IMPORTED_FILENAMES_ERRORS already exists."
  gcloud logging metrics -q delete $DELETE_IMPORTED_FILENAMES_ERRORS
fi
gcloud logging metrics create "$DELETE_IMPORTED_FILENAMES_ERRORS" \
  --description="Count of the failed imported filenames deletes errors in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"One or more errors occurred when deleting from the imported filenames bucket\"" \
  && echo "Metric $DELETE_IMPORTED_FILENAMES_ERRORS has been successfully created."

RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS=RetrieveFeedsToArchiveErrors
if echo "$EXISTING_METRICS" | grep -q -w "$RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS"
then
  echo "Metric $RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS already exists."
  gcloud logging metrics -q delete $RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS
fi
gcloud logging metrics create "$RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS" \
  --description="Count of the errors of retrieving feeds to archive in calculateProductChanges." \
  --log-filter="resource.type=\"cloud_function\" resource.labels.function_name=\"calculateProductChanges\" \"Retrieval of feeds to archive failed\"" \
  && echo "Metric $RETRIEVE_FEEDS_TO_ARCHIVE_ERRORS has been successfully created."

CONTENT_API_ITEM_INSERT_ERRORS=ContentApiItemInsertErrors
if echo "$EXISTING_METRICS" | grep -q -w "$CONTENT_API_ITEM_INSERT_ERRORS"
then
  echo "Metric $CONTENT_API_ITEM_INSERT_ERRORS already exists."
  gcloud logging metrics -q delete $CONTENT_API_ITEM_INSERT_ERRORS
fi
gcloud logging metrics create "$CONTENT_API_ITEM_INSERT_ERRORS" \
--description="Count of the Content API errors for inserting items" \
--log-filter="resource.type=\"gae_app\" \"The item could not be inserted\"" \
  && echo "Metric $CONTENT_API_ITEM_INSERT_ERRORS has been successfully created."

APP_ENGINE_UPLOAD_LOG_ERRORS=AppEngineUploadLogErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_UPLOAD_LOG_ERRORS"
then
  echo "Metric $APP_ENGINE_UPLOAD_LOG_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_UPLOAD_LOG_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_UPLOAD_LOG_ERRORS" \
--description="Count of the App Engine errors when writing to the log table fails" \
--log-filter="resource.type=\"gae_app\" \"The result of Content API for Shopping call was not recorded\"" \
  && echo "Metric $APP_ENGINE_UPLOAD_LOG_ERRORS has been successfully created."

APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS=AppEngineMerchantInfoNotFoundErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS"
then
  echo "Metric $APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS" \
--description="Count of the App Engine errors when merchant-info.json could not be loaded" \
--log-filter="resource.type=\"gae_app\" \"Merchant Info JSON file does not exist\"" \
  && echo "Metric $APP_ENGINE_MERCHANT_INFO_NOT_FOUND_ERRORS has been successfully created."

APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS=AppEngineMerchantInfoInvalidErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS"
then
  echo "Metric $APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS" \
--description="Count of the App Engine errors when merchant-info.json could not be parsed" \
--log-filter="resource.type=\"gae_app\" \"Could not parse Merchant center configuration file\"" \
  && echo "Metric $APP_ENGINE_MERCHANT_INFO_INVALID_ERRORS has been successfully created."

APP_ENGINE_TASK_PARSE_ERRORS=AppEngineTaskParseErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_TASK_PARSE_ERRORS"
then
  echo "Metric $APP_ENGINE_TASK_PARSE_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_TASK_PARSE_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_TASK_PARSE_ERRORS" \
--description="Count of the App Engine errors when incoming task could not be parsed" \
--log-filter="resource.type=\"gae_app\" \"Error parsing the task JSON\"" \
  && echo "Metric $APP_ENGINE_TASK_PARSE_ERRORS has been successfully created."

APP_ENGINE_RECORD_RESULT_ERRORS=AppEngineRecordResultErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_RECORD_RESULT_ERRORS"
then
  echo "Metric $APP_ENGINE_RECORD_RESULT_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_RECORD_RESULT_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_RECORD_RESULT_ERRORS" \
--description="Count of the App Engine errors when API results could not be written to BigQuery" \
--log-filter="resource.type=\"gae_app\" \"The result of Content API for Shopping call was not recorded to BigQuery\"" \
  && echo "Metric $APP_ENGINE_RECORD_RESULT_ERRORS has been successfully created."

APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS=AppEngineRecordPerItemResultsErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS"
then
  echo "Metric $APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS" \
--description="Count of the App Engine errors when per item results of the Content API call could not be written to BigQuery" \
--log-filter="resource.type=\"gae_app\" \"The per item results of the Content API for Shopping call were not recorded to BigQuery\"" \
  && echo "Metric $APP_ENGINE_RECORD_PER_ITEM_RESULTS_ERRORS has been successfully created."

APP_ENGINE_TASK_HOSTNAME_ERRORS=AppEngineTaskHostnameErrors
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_TASK_HOSTNAME_ERRORS"
then
  echo "Metric $APP_ENGINE_TASK_HOSTNAME_ERRORS already exists."
  gcloud logging metrics -q delete $APP_ENGINE_TASK_HOSTNAME_ERRORS
fi
gcloud logging metrics create "$APP_ENGINE_TASK_HOSTNAME_ERRORS" \
--description="Count of the App Engine errors when a Task is created and hostname could not be found" \
--log-filter="resource.type=\"gae_app\" \"Failed to create a task: Failed to resolve hostname\"" \
  && echo "Metric $APP_ENGINE_TASK_HOSTNAME_ERRORS has been successfully created."

APP_ENGINE_PROCESS_BATCH_FAILED=AppEngineProcessBatchFailed
if echo "$EXISTING_METRICS" | grep -q -w "$APP_ENGINE_PROCESS_BATCH_FAILED"
then
  echo "Metric $APP_ENGINE_PROCESS_BATCH_FAILED already exists."
  gcloud logging metrics -q delete $APP_ENGINE_PROCESS_BATCH_FAILED
fi
gcloud logging metrics create "$APP_ENGINE_PROCESS_BATCH_FAILED" \
--description="Count of the App Engine errors when a batch of items failed to be upserted/deleted and will not be retried" \
--log-filter="resource.type=\"gae_app\" \"failed and will not be retried\"" \
  && echo "Metric $APP_ENGINE_PROCESS_BATCH_FAILED has been successfully created."

# Create Notification Alerts
print_green "Recreating Stackdriver alerts..."
ALERT_POLICIES=(
  app-engine-merchant-info-invalid-errors-policy.yaml
  app-engine-merchant-info-not-found-errors-policy.yaml
  app-engine-process-batch-errors-policy.yaml
  app-engine-record-per-item-results-errors-policy.yaml
  app-engine-record-result-errors-policy.yaml
  app-engine-task-hostname-errors-policy.yaml
  app-engine-task-parse-errors-policy.yaml
  attempted-files-retrieval-errors-policy.yaml
  bq-load-errors-policy.yaml
  calculate-changes-function-errors-policy.yaml
  calculate-deletes-query-errors-policy.yaml
  calculate-inserts-query-errors-policy.yaml
  calculate-updates-query-errors-policy.yaml
  content-api-call-log-errors-policy.yaml
  content-api-item-insert-errors-policy.yaml
  count-deletes-query-errors-policy.yaml
  count-upserts-query-errors-policy.yaml
  create-task-empty-response-errors-policy.yaml
  create-task-exception-errors-policy.yaml
  delete-imported-filenames-errors-policy.yaml
  deletes-threshold-errors-policy.yaml
  eof-exists-during-import-errors-policy.yaml
  eof-lock-check-errors-policy.yaml
  eof-lock-failed-errors-policy.yaml
  eof-locked-errors-policy.yaml
  eof-unlock-failed-errors-policy.yaml
  files-to-archive-retrieval-errors-policy.yaml
  get-changes-count-query-errors-policy.yaml
  import-function-timeout-errors-policy.yaml
  import-storage-file-function-errors-policy.yaml
  imported-files-retrieval-errors-policy.yaml
  imported-gcs-save-errors-policy.yaml
  invalid-eof-errors-policy.yaml
  invalid-filename-errors-policy.yaml
  invalid-filerange-errors-policy.yaml
  nonexistent-table-errors-policy.yaml
  schema-config-errors-calc-policy.yaml
  schema-config-errors-load-policy.yaml
  upserts-threshold-errors-policy.yaml
)
print_green "Recreating alerting policies..."
for POLICY in "${ALERT_POLICIES[@]}"
do
  POLICY_RESULT="$(gcloud alpha monitoring policies create --policy-from-file="stackdriver_alerts/$POLICY" 2>&1)"
  POLICY_ID=$(echo "$POLICY_RESULT" | sed "s/.*\[\(.*\)\].*/\1/")
  for EMAIL in $(echo "$ALERT_EMAILS" | sed "s/,/ /g")
  do
    NOTIFICATION_RESULT="$(gcloud alpha monitoring channels create --display-name="$EMAIL" --type=email --channel-labels="email_address=$EMAIL" --description="Shopping Alert Notification Channel" 2>&1)"
    CHANNEL=$(echo "$NOTIFICATION_RESULT" | sed "s/.*\[\(.*\)\].*/\1/")
    gcloud alpha monitoring policies update "$POLICY_ID" --add-notification-channels="$CHANNEL"
  done
done

# Setup Service Accounts and grant permissions
print_green "Setting up service account permissions..."
PROJECT_NUMBER=$(gcloud projects list --filter="PROJECT_ID=$GCP_PROJECT" --format="value(PROJECT_NUMBER)")
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$GCP_PROJECT"@appspot.gserviceaccount.com \
  --role roles/editor
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$GCP_PROJECT"@appspot.gserviceaccount.com \
  --role roles/bigquery.admin
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$GCP_PROJECT"@appspot.gserviceaccount.com \
  --role roles/bigquery.jobUser
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$GCP_PROJECT"@appspot.gserviceaccount.com \
  --role roles/iam.serviceAccountTokenCreator
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$GCP_PROJECT"@appspot.gserviceaccount.com \
  --role roles/iam.serviceAccountUser
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/cloudfunctions.developer
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/iam.serviceAccountUser
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/cloudfunctions.developer
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/appengine.appAdmin
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/composer.admin
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/container.admin
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/editor
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role roles/run.admin
gcloud iam service-accounts add-iam-policy-binding \
  "$GCP_PROJECT"@appspot.gserviceaccount.com \
  --member=serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
  --role=roles/iam.serviceAccountUser \
  --project="$GCP_PROJECT"
gcloud projects add-iam-policy-binding "$GCP_PROJECT" \
  --member serviceAccount:"$FEED_SERVICE_ACCOUNT" \
  --role roles/cloudtasks.admin

# Setup CI/CD
print_green "Installing KMS keyring and key for Cloud Build..."
EXISTING_KEYRINGS=$(gcloud kms keyrings list --location=global --filter=$KEYRING)
if echo "$EXISTING_KEYRINGS" | grep -q "$KEYRING"
then
  echo "Keyring $KEYRING already exists."
else
  gcloud kms keyrings create "$KEYRING" \
    --location=global
fi

EXISTING_KEYS=$(gcloud kms keys list --location=global --keyring=$KEYRING)
if echo "$EXISTING_KEYS" | grep -q "$KEYNAME"
then
  echo "Keyring $KEYNAME already exists."
else
  gcloud kms keys create "$KEYNAME" \
    --location=global \
    --keyring="$KEYRING" \
    --purpose=encryption
  gcloud kms keys add-iam-policy-binding \
      "$KEYNAME" --location=global --keyring="$KEYRING" \
      --member=serviceAccount:"$PROJECT_NUMBER"@cloudbuild.gserviceaccount.com \
      --role=roles/cloudkms.cryptoKeyDecrypter
fi

print_green "Deleting and re-creating Cloud Build triggers..."
CreateTrigger() {
  TARGET_TRIGGER=$1
  DESCRIPTION=$2
  ENV_VARIABLES=$3
  gcloud alpha builds triggers create cloud-source-repositories \
  --build-config=cicd/"$TARGET_TRIGGER" \
  --repo="$SOURCE_REPO" \
  --branch-pattern=master \
  --description="$DESCRIPTION" \
  --substitutions ^::^"$ENV_VARIABLES"
}

# Recreate the Cloud Build triggers by deleting them all first.
EXISTING_TRIGGERS=$(gcloud alpha builds triggers list | grep "id:" | awk '{printf("%s\n", $2)}')
for TRIGGER in $(echo "$EXISTING_TRIGGERS")
do
  gcloud alpha builds triggers -q delete "$TRIGGER"
done
CreateTrigger deploy_functions_calculate_product_changes.yaml \
  "Deploy Calculate Product Change Function" \
  _TIMEZONE_UTC_OFFSET="$TIMEZONE_UTC_OFFSET"::_EXPIRATION_THRESHOLD="$EXPIRATION_THRESHOLD"::_BQ_DATASET="$BQ_FEED_DATASET"::_DELETES_THRESHOLD="$DELETES_THRESHOLD"::_UPSERTS_THRESHOLD="$UPSERTS_THRESHOLD"::_UPDATE_BUCKET="$UPDATE_BUCKET"::_RETRIGGER_BUCKET="$RETRIGGER_BUCKET"::_FEED_BUCKET="$FEED_BUCKET"::_ARCHIVE_BUCKET="$ARCHIVE_BUCKET"::_COMPLETED_FILES_BUCKET="$COMPLETED_FILES_BUCKET"::_LOCK_BUCKET="$LOCK_BUCKET"
CreateTrigger deploy_functions_import_file_into_bq.yaml \
  "Deploy Feed Import Function" \
  _BQ_DATASET="$BQ_FEED_DATASET"::_FILE_RANGE="$FILE_RANGE"::_COMPLETED_FILES_BUCKET="$COMPLETED_FILES_BUCKET"::_FEED_BUCKET="$FEED_BUCKET"::_UPDATE_BUCKET="$UPDATE_BUCKET"::_LOCK_BUCKET="$LOCK_BUCKET"
CreateTrigger deploy_functions_retry_feed_import.yaml \
  "Deploy Feed File Retry Function" \
  _BQ_DATASET="$BQ_FEED_DATASET"::_FEED_BUCKET="$FEED_BUCKET"::_UPDATE_BUCKET="$UPDATE_BUCKET"::_COMPLETED_FILES_BUCKET="$COMPLETED_FILES_BUCKET"::_RETRIGGER_BUCKET="$RETRIGGER_BUCKET"
CreateTrigger deploy_functions_trigger_dag.yaml \
  "Deploy Trigger DAG Function" \
  _GCP_PROJECT="$GCP_PROJECT"::_AIRFLOW_WEBSERVER_ID="$AIRFLOW_WEBSERVER_ID"::_DAG_ID="$DAG_ID"::_TRIGGER_COMPLETION_BUCKET="$TRIGGER_COMPLETION_BUCKET"::_REGION="$REGION"::_CLOUD_COMPOSER_ENV_NAME="$CLOUD_COMPOSER_ENV_NAME"
CreateTrigger deploy_gae_initiator.yaml \
  "Deploy initiator AppEngine" \
  _KEYRING="$KEYRING"::_KEYNAME="$KEYNAME"::_GCP_PROJECT="$GCP_PROJECT"::_REGION="$REGION"::_TRIGGER_COMPLETION_BUCKET="$TRIGGER_COMPLETION_BUCKET"::_LOCK_BUCKET="$LOCK_BUCKET"
CreateTrigger deploy_monitor.yaml \
  "Deploy Monitor Composer & Function" \
  _KEYRING="$KEYRING"::_KEYNAME="$KEYNAME"::_MAILER_SUBSCRIPTION="$MAILER_SUBSCRIPTION"::_GCP_PROJECT="$GCP_PROJECT"::_PUBSUB_TOKEN="$PUBSUB_TOKEN"::_CLOUD_COMPOSER_ENV_NAME="$CLOUD_COMPOSER_ENV_NAME"::_REGION="$REGION"::_FINISH_EMAILS="$FINISH_EMAILS"::_UPDATE_BUCKET="$UPDATE_BUCKET"
CreateTrigger deploy_gae_build_reporter.yaml \
  "Deploy build reporter AppEngine" \
  _KEYRING="$KEYRING"::_KEYNAME="$KEYNAME"::_CLOUD_BUILD_SUBSCRIPTION="$CLOUD_BUILD_SUBSCRIPTION"::_GCP_PROJECT="$GCP_PROJECT"::_SHOPTIMIZER_BUILD_NOTIFICATION_EMAILS="$SHOPTIMIZER_BUILD_NOTIFICATION_EMAILS"
CreateTrigger deploy_shoptimizer_api_and_gae_uploader.yaml \
  "Deploy Shoptimizer API and uploader AppEngine" \
  _KEYRING="$KEYRING"::_KEYNAME="$KEYNAME"::_GCP_PROJECT="$GCP_PROJECT"::_DRY_RUN="$DRY_RUN"::_MERCHANT_ID="$MERCHANT_ID"::_IS_MCA="$IS_MCA"
# Encrypt Keys
print_green "Generating encrypted versions of the service accounts so that Cloud Build can deploy stuff securely..."
SECRET_KEYS=(
  appengine/uploader/config/gcp_service_account.json
  appengine/initiator/config/service_account.json
  appengine/uploader/config/mc_service_account.json
  composer/completion_monitor/config/service_account.json
)
for SECRET_KEY in "${SECRET_KEYS[@]}"
do
  ENCRYPTED_KEY="$SECRET_KEY".enc
  gcloud kms encrypt \
      --location=global \
      --keyring="$KEYRING" \
      --key="$KEYNAME" \
      --plaintext-file="$SECRET_KEY" \
      --ciphertext-file="$ENCRYPTED_KEY" \
    && echo "Secret key $SECRET_KEY has been successfully encrypted to $ENCRYPTED_KEY."
done

print_green "Installation and setup finished. Please deploy via Cloud Build either manually or by pushing to your source repository at ${HYPERLINK}https://source.cloud.google.com/{$GCP_PROJECT}/{$SOURCE_REPO}\ahttps://source.cloud.google.com/{$GCP_PROJECT}/{$SOURCE_REPO}${HYPERLINK}\a"
