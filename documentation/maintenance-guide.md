# Feedloader Dev Ops Guide

- [Overview](#overview)
- [Manually Verifying The Deployment](#manually-verifying-the-deployment)
  - [Check Cloud Buckets](#check-cloud-buckets)
  - [Check Cloud Build](#check-cloud-build)
  - [Check App Engine Services](#check-app-engine-services)
  - [Check Cloud Functions](#check-cloud-functions)
  - [Check BigQuery Tables](#check-bigquery-tables)
  - [Check Service Account Keys](#check-service-account-keys)
  - [Check Cloud Composer](#check-cloud-composer)
- [Checking the State of the Solution Before/During/After a Run](#checking-the-state-of-the-solution-beforeduringafter-a-run)
  - [Before EOF uploaded (Only Feed Files were uploaded to Cloud Storage):](#before-eof-uploaded-only-feed-files-were-uploaded-to-cloud-storage)
  - [After EOF uploaded (Both Feed Files and the EOF trigger have been uploaded):](#after-eof-uploaded-both-feed-files-and-the-eof-trigger-have-been-uploaded)
- [Checking Merchant Center Data Using Content API](#checking-merchant-center-data-using-content-api)
- [Sending a fake POST to AppEngine to check its behavior](#sending-a-fake-post-to-appengine-to-check-its-behavior)
- [Manually Handle Expiration](#manually-handle-expiration)
- [Manually Send PubSub](#manually-send-pubsub)
- [FAQ](#faq)
  - [Q: Where do I find the logs for [insert component here]?](#q-where-do-i-find-the-logs-for-insert-component-here)
  - [Q: How do I test a feed upload?](#q-how-do-i-test-a-feed-upload)
  - [Q: How do I check that the feeds were uploaded successfully?`](#q-how-do-i-check-that-the-feeds-were-uploaded-successfully)
  - [Q: Why did no items get flagged to be updated/deleted in Merchant Center?](#q-why-did-no-items-get-flagged-to-be-updateddeleted-in-merchant-center)
  - [Q: How do I know if a Cloud Function failed?](#q-how-do-i-know-if-a-cloud-function-failed)
  - [Q: How do I know if a Task in the Task Queue failed?](#q-how-do-i-know-if-a-task-in-the-task-queue-failed)
  - [Q: How do I test that the Cloud Function retry is working?](#q-how-do-i-test-that-the-cloud-function-retry-is-working)
  - [Q: How do I test that the delete prevention threshold feature is working?](#q-how-do-i-test-that-the-delete-prevention-threshold-feature-is-working)
  - [Q: How do I check the configuration of log alerts?](#q-how-do-i-check-the-configuration-of-log-alerts)
  - [Q: How do I roll back a Cloud Function?](#q-how-do-i-roll-back-a-cloud-function)
  - [Q: How do I roll back App Engine?](#q-how-do-i-roll-back-app-engine)
  - [Q: How do I start/stop the Task Queue?](#q-how-do-i-startstop-the-task-queue)
  - [Q: Why is the Task Queue task count higher than what it should be?](#q-why-is-the-task-queue-task-count-higher-than-what-it-should-be)
  - [Q: How do I abort processing the items?](#q-how-do-i-abort-processing-the-items)
  - [Q: How do I reset the BigQuery tables/Cloud Storage if I want to start clean?](#q-how-do-i-reset-the-bigquery-tablescloud-storage-if-i-want-to-start-clean)
  - [Q: How do I configure Cloud Build?](#q-how-do-i-configure-cloud-build)
  - [Q: Why is there an items table AND a streaming_items table?](#q-why-is-there-an-items-table-and-a-streaming_items-table)
  - [Q: Why are a bunch of temporary BigQuery tables getting created during the Task Queue run?](#q-why-are-a-bunch-of-temporary-bigquery-tables-getting-created-during-the-task-queue-run)
  - [Q: How do I download more than 300 lines of logs from Cloud Logging?](#q-how-do-i-download-more-than-300-lines-of-logs-from-cloud-logging)
  - [Q: How long does it take to upload items?](#q-how-long-does-it-take-to-upload-items)

## Overview

This document provides a guide to various Dev Ops maintenance tasks and FAQs in order to ensure that Feedloader runs correctly and explains how to investigate/resolve various common issues. While the Feedloader solution should be automated and includes features like failure detection, automatic retry, and error logging, there may be some situations where manual intervention becomes necessary, which this document aims to cover.

## Manually Verifying The Deployment

After deploying to GCP via git, you can check the solution deployed correctly as follows:

### Check Cloud Storage Buckets

  1. Navigate to GCP admin console > Cloud Storage.

  2. Check all buckets exist. See the table below for a list of the expected buckets:

        |Environment Variable Name |Example Bucket Name               |
        |--------------------------|----------------------------------|
        |$ARCHIVE_BUCKET           |myproject-archive                 |
        |$COMPLETED_FILES_BUCKET   |myproject-completed               |
        |$FEED_BUCKET              |myproject-feed                    |
        |$LOCK_BUCKET              |myproject-lock                    |
        |$MONITOR_BUCKET           |myproject-monitor                 |
        |$RETRIGGER_BUCKET         |myproject-retrigger               |
        |$TRIGGER_COMPLETION_BUCKET|myproject-trigger-completion-check|
        |$UPDATE_BUCKET            |myproject-update                  |

        If buckets are missing, check the bucket names you set in install_to_gcp.sh are valid (globally unique, start with gs://, etc.), and rerun the script.

### Check Cloud Build

  1. Navigate to GCP admin console > Cloud Build > History. All checks should be green.
  2. If you do not have new builds (check the timestamp) listed in Cloud Build:
      i. Navigate to GCP admin console > Cloud Build > Triggers.
      ii. Check Cloud Build triggers exist and that they are all enabled.
      iii. If the triggers are not enabled, enable them.
      iv. If the triggers do not exist, run install_to_gcp.sh again and make note of any errors.
  3. It is possible that Cloud Build shows the green check even though a build failed. So it’s worth checking that the Cloud Functions and App Engine services deployed correctly, too. Proceed to the next section to check these.

### Check App Engine Services
  1. Navigate to GCP admin console > App Engine > Services.
  2. All services should have a similar last deployed timestamp. If a service has an old timestamp, it is likely the build for that service failed.
  3. In this case, navigate to GCP admin console > Cloud Build > History.
  4. Click on the build for the failing App Engine service and look for errors. Existence of a Pipfile in an App Engine service will cause the build to fail. Delete Pipfiles locally and do a git push.

### Check Cloud Functions
  1. Navigate to GCP admin console > Cloud Functions.
  2. Ensure all functions are listed. The list of expected Cloud Functions is shown in the table below:

      |Cloud Function name|
      |:----|
      |calculate_product_changes|
      |import_storage_file_into_big_query|
      |reprocess_feed_file|
      |trigger_dag|
  3. All functions should have a similar last deployed timestamp. If a function has an old timestamp, it is likely the build for that function failed. In this case, navigate to GCP admin console > Cloud Build > History.
  4. Click on the build for the failing Cloud Function and look for errors.
  5. Check Cloud Function Environment Variables: Navigate to GCP admin console > Cloud Functions. Click on each Cloud Function and ensure the environment variables match those set in your install_to_gcp.sh script.
  6. For the trigger_dag function, the webserver_id environment variable should match the URL in Cloud Composer. To get this URL, navigate to GCP admin console > Cloud Composer > Click process-completion-monitor. The URL is listed under “Airflow Web UI”.
  7. To check the client_id variable, you can run the following commands in your local project directory:

      `gcloud auth login`

      `python -W ignore get_cc_client_id.py "PROJECT-NAME" "us-central1" "process-completion-monitor"`
  8. If client_id is not set, it is likely your local Python environment is missing a required library, which caused get_cc_client_id.py to fail. Run “pip install google-auth request”. Then run install_to_gcp.sh again.
  9. In the case that an environment variable was not set correctly, run install_to_gcp.sh and take note of any errors.


### Check BigQuery Tables
  1. You should see the following datasets and their respective tables in BigQuery as shown in the table below (Note: some tables are only generated when the solution has been run at least once):

      |Dataset: feed_data|Dataset: monitor_data|Dataset: processing_feed_data|
      |------------------|---------------------|-----------------------------|
      |items_expiration_tracking|item_results|process_items_to_[action]_[timestamp]|
      |items_to_delete|process_result|…|
      |items_to_prevent_expiring| |…|
      |items_to_upsert| |…|
      |streaming_items| |…|

### Check Service Account Keys

  1. Naviage to GCP admin console > IAM & admin > Service accounts.
  2. Check that keys exist for:

      `cloud-composer@`

      `merchant-center@`

      `shopping-feed@`

  3. The keys should have a creation date similar to the build date.
  4. If keys do not exist, re-run install_to_gcp.sh and take note of any errors.
  5. Next, check that the keys exist in the deployed code:
      1. Navigate to GCP admin console > App Engine > Services.
      2. To the right of Default Service, under “Diagnose”, select “Source” from the dropdown.
      3. Check the config/service_account.json file exists.
      4. Go back to the list of App Engine services.
      5. To the right of Uploader, under “Diagnose”, select “Source” from the dropdown.
      6. Check that config/gcp_service_account.json and config/mc_service_account.json exist.
          i. If these keys do not exist, check if they exist in your local project.
          ii. If they do exist, be sure to `git add` them and `git push` to the GCP Source Repository.
          iii. If they do not exist, re-run install_to_gcp.sh.

### Check Cloud Composer

  1. Navigate to GCP admin console > Composer.
  2. Check process-completion-monitor is deployed.
  3. Click on the link to Airflow under "Airflow webserver".
  4. Ensure completion monitor does not have spinning “loading circles”. If it does, you probably have to delete and redeploy Airflow (by running install_to_gcp.sh). This takes around 90 minutes total.




After checking all of the above, Feedloader should be ready to run. As explained in the usage guide, to start using Feedloader, upload feed files to the feed Storage bucket, then upload an empty EOF file to the update storage bucket after all files have finished uploading.


## Checking the State of the Solution Before/During/After a Run

### Before EOF uploaded (Only Feed Files were uploaded to Cloud Storage):

  1. Navigate to GCP admin console > Logging > Logs Viewer.
  2. In the Logs Viewer dropdown: navigate to Cloud Functions > import_storage_file_into_big_query.
  3. Check there are no errors in the logs.
  4. Navigate to GCP admin console > BigQuery > feed_data > items.
  5. Check that the `items` table was successfully created and populated.
  6. If it was not, ensure your feed_schema_config.json file and feed files have matching/correct formats.

### After EOF uploaded (Both Feed Files and the EOF trigger have been uploaded):

  1. Navigate to GCP admin console > Logging > Logs Viewer.
  2. In the Logs Viewer dropdown: navigate to Cloud Function > calculate_product_changes
  3. Check there are no errors in the logs.
  4. Navigate to GCP admin console > BigQuery > feed_data.
  5. Check the following tables: `streaming_items`, `items_to_delete`, and `items_to_upsert`
  6. Check these tables are populated as expected: `items_to_delete`/`items_to_upsert` represent the diff between the `items` table and the last data that was uploaded to streaming_items.
  7. If no diff was found when you expected one, it is possible there was a failure after calculate_product_changes inserted the previous data into the `streaming_items` table. This data will need to be cleaned up before running Feedloader again. See the section [How do I reset the BigQuery tables](#q-how-do-i-reset-the-bigquery-tablescloud-storage-if-i-want-to-start-clean) in the FAQ for details.
  8. Navigate to GCP admin console > Logging > Logs Viewer.
  9. In the Logs Viewer dropdown, navigate to Cloud Function > trigger_dag.
  10. Check there are no errors in the logs.
  11. If there are errors, check the environment variables in your trigger_dag function, and check that Cloud Composer is running.
  12. Navigate to GAE application > Default Service.
  13. Check there are no errors in the logs.
  13. Navigate to GCP admin console > BigQuery > processing_feed_data.
  14. Check `process_items_to_insert` and `process_items_to_insert` tables are created.
  15. Navigate to GCP admin console > Logging > Logs Viewer.
  16. In the Logs Viewer dropdown: GAE application > uploader.
  17. Check that the batches were inserted successfully. You should see logs like this:
      ```
      A 20XX-09-17T10:09:13.104189Z Batch #1: custombatch insert API successfully submitted 3 of 3 items
      A 20XX-09-17T10:09:13.105415Z Batch #1: Processing 0.006s, API call 1.379s, Total 1.385s
      A 20XX-09-17T10:09:13.105568Z Batch #1 with method insert and initiation timestamp 20190917100858 successfully processed 3 items, failed to processed 0 items and skipped 0 items.
      ```
  18. Then, you should see a log like this:
      ```
      The result of Content API for Shopping call was successfully recorded to BigQuery. Success inserting into table process_result.
      ```
  19. This second log is to record the history of Content API calls. If you see failures for this, it means the Feedloader logging failed. However, the Content API calls may still have succeeded.
  20. (Back in log viewer) Navigate to GAE application > mailer
  21. Check there are no errors in the logs.
  22. If the mailer service did not receive any logs, check that Cloud Composer started Airflow: Navigate to GCP admin console > Composer. Click the link under “Airflow webserver”.
  23. Check the last run date in Airflow and ensure it ran.
  24. If the completion_monitor did not run, check the Cloud Build logs to make sure it deployed correctly, and then check your service account keys are set up correctly.

You should now receive a successful completion email!

## Checking Merchant Center Data Using Content API
  1. You can use the Google API samples to check the data on Merchant Center.
  2. Create a new directory and run.

      `git clone https://github.com/googleads/googleads-shopping-samples`
  3. Run:
`mkdir ~/shopping-samples/content`.
  4. Go to GCP admin console > IAM & admin > Service Accounts.
  5. Download the key for your merchant center account (merchant-center@[project-name].iam.gserviceaccount.com) by clicking on … > create key (**warning: each account has a limited number of keys**).
  6. Copy the key to ~/shopping-samples/content, and rename it "service-account.json".
  7. Still in the ~/shopping-samples/content directory, create a new file called merchant-info.json with the following format:
      ```
      {
        "merchantId": 12345,
        "accountSampleUser": "",
        "accountSampleAdWordsCID": 0
      }
      ```

  8. Replace "merchantId" with your merchant ID. You can find this in install_to_gcp.sh, or in the merchant center URL. Warning: be sure not to accidentally use a production merchant center ID.
  9. Optional: Create a Python virtual environment using your tool of choice (the code should work with Python 2 or 3).
  10. `cd` to your googleads-shopping-samples/python directory, run: `pip install -r requirements.txt`
  11. Edit and run the scripts as you like. For example, to list all products in Merchant Center: `python -m shopping.content.products.list > all_products.txt`
  12. Alternatively, open the googleads-shopping-samples/python in PyCharm and run the scripts from there.

## Sending a fake POST to AppEngine to check its behavior
You can send POST requests to AppEngine directly without triggering Cloud Pub/Sub. Use test MC accounts.

  1. Upload feeds normally to create an `items` table.
  2. Copy `items` table to the same dataset naming it `items_backup`.
  3. If `items_to_upsert` or `items_to_delete` table is not existing in the dataset: Upload an EOF to the update bucket to create the `items_to_upsert` and/or `items_to_delete` tables. This will issue a POST request to AppEngine and delete the `items` table.
  4. Restore `items` from `items_backup` by copying it.
  5. Send a fake POST request to AppEngine by using your choice of tools: curl command, REST client (VS Code plugin) or Postman.

      REST client example:

      ```
      POST https://default-dot-sfo-rihito-dev.appspot.com/start
      content-type: application/json`

      {"message": {"attributes": {"action": "delete", "jobId": "delete job ID: aa305095-4635-48aa-983b-14f41e43243e", "numRows": "1", "upsertCount": "2000", "deleteCount": "2000"}, "data": "ZGVsZXRlIFJFQURZ", "messageId": "618477215923628", "message_id": "618477215923628", "publishTime": "2019-07-17T03:19:26.848Z", "publish_time": "2019-07-17T03:19:26.848Z"}, "subscription": "projects/project-id/subscriptions/initiator-start"}
      ```
  6. Repeat steps 4 to 5 to check AppEngine’s behavior.

## Manually Handle Expiration
This section documents the steps required to manually prevent items from expiring. It must be done once per month in the absence of an automated job.

1. Ensure the last run’s feed files have been archived in GCS.
2. If there is no archive, download the last batch of files directly from the bucket using: `gcloud storage cp --recursive “gs://[feed bucket]/*” ./`
3. Take a BigQuery backup of the table `streaming_items` either with the backup feature or by copying the table with the name `streaming_items_backup_[DATE]`
4. Change the environment variable in the calculate_product_changes Cloud Function for UPSERT_THRESHOLD to 100000000
5. Clear out the `streaming_items` table by running this query in BigQuery: `DELETE FROM [project + schema].streaming_items WHERE true`.
6. Wait for the next batch of items to process everything (after EOF has been uploaded to the update bucket).
7. Verify in Merchant Center that the item expirations have been resolved: the Yellow warning in the graph will fall back down.
8. Change the calculate_product_changes UPSERT_THRESHOLD environment variable back to the original value (default 1000000).


## Manually Send PubSub

In the case that PubSub messages do not get sent, follow the below steps.

  1. Navigate to GCP Pub/Sub -> and find the “updates-ready-trigger” topic.
  2. Click Publish Message at the top.
  3. Click “Add an attribute”.
  4. Key: deleteCount. Value: [Find it in the calculate_product_changes Cloud Function Logs].
  5. Click “Add an attribute”.
  6. Key: expiringCount. Value: [Find it in the calculate_product_changes Cloud Function Logs].
  7. Click “Add an attribute”.
  8. Key: upsertCount. Value: [Find it in the calculate_product_changes Cloud Function Logs]
  9. Click “Publish”.

## FAQ

##### Q: Where do I find the logs for [insert component here]?
A: Logs can be found from the Cloud Console’s Logging tool. Click on “Logs Viewer” and then filter the logs for the specific thing you want to see.

##### Q: How do I test a feed upload?
A: Prepare a properly-formatted feed file (TSV-format, matching the schema in `feed_schema_config.json`. Upload it to the feed bucket in Cloud Storage (as defined by your `FEED_BUCKET` environment variable at install time. Upload it to Cloud Storage using the following command: `gcloud storage cp --gzip-in-flight csv ../Sample_Feed/MultipleTest/* gs://pla-feed`. You can remove `-m` if you want to upload files sequentially, since `-m` uploads them in parallel.

##### Q: How do I check that the feeds were uploaded successfully?`
A: The feeds can be found in Cloud Storage, they go into the feed bucket named by your environment variable `FEED_BUCKET`. However, in order to check that the files were converted into a BigQuery table, you can check the `items` table was created and was populated in BigQuery. Feedloader also does checks that file content conversions into BigQuery were actually completed by populating a GCS bucket with the name denoted by your `COMPLETED_FILES_BUCKET` environment variable. Check that all the filenames show up in this bucket that you actually attempted to upload to further ensure that feeds uploaded successfully. Finally, you may do a count comparison of the total number of rows in your feeds with the number of rows in the `items` BigQuery table.

##### Q: Why did no items get flagged to be updated/deleted in Merchant Center?
A: The likely scenario is that two runs were exactly the same (no diff). However there could have been other problems such as BigQuery failing or a function not triggering. Logs must be checked in this case.

##### Q: How do I know if a Cloud Function failed?
A: Two ways: one is to go to Cloud Functions in the GCP admin UI, click on the function name to check, the monitoring graph will show if there was a failure on the timeline. Otherwise, click on “Logs” at the top for that Cloud Functions and filter for error logs (either by severity or just filter text “error”).

##### Q: How do I know if a Task in the Task Queue failed?
A: There is a log tab in Cloud Tasks to see logs of the Task Queue. Also, since Tasks execute App Engine services, Cloud Logging can be checked for those respective App Engine services. You may need to enable Logging for Cloud Tasks as described here: https://cloud.google.com/tasks/docs/logging#console

##### Q: How do I test that the Cloud Function retry is working?
A: There is a specific Cloud Function called "reprocess_feed_file". Check the logs for this Cloud Function, and if it was run, there will be logs that it executed, showing what was reprocessed.

##### Q: How do I test that the delete prevention threshold feature is working?
A: In the logs for calculate_product_changes, you should see a log if the number of detected product item changes exceeded the thresholds set in your environment variables (`UPSERTS_THRESHOLD ` and `DELETES_THRESHOLD `). The log should read as: "`[action] count X crossed [action] threshold of [threshold] items. Skipping [action] processing...`"

##### Q: How do I check the configuration of log alerts?
A: The first thing to check is the install_to_gcp.sh script which contains the full list of all configurations in one place. One thing to note is that these use external .yaml files to define the alert rules which are in the alerts folder. Otherwise, these can be checked (after they are deployed) in the Cloud Logging (“Monitoring” in GCP admin console) Policies screen.

##### Q: How do I roll back a Cloud Function?
A: It can’t be done directly, it requires a redeploy. Leverage Cloud Build for this: check the build history, find the last successful git commit that deployed successfully, and re-run that commit.

##### Q: How do I roll back App Engine?
A: It can’t be done directly, it requires a redeploy. Leverage Cloud Build for this: check the build history, find the last successful git commit that deployed successfully, and re-run that commit. However, there is a way to set up App Engine traffic routing to use old deploys. See [this article](https://cloud.google.com/appengine/docs/standard/python3/migrating-traffic) for details.

##### Q: How do I start/stop the Task Queue?
A: Go to the GCP admin console > Cloud Tasks, and then pause the Task Queue and purge it to clear it out. It can then be unpaused.

##### Q: Why is the Task Queue task count higher than what it should be?
A: Typically the number of tasks in the task queue should be (upsertCount + deleteCount + expirationCount) / 1000. (The reason for this is that API calls are batched into chunks of 1,000). If the observed number of tasks is higher than this (particularly when the Task Queue is paused), this is a known bug. For some reason when tasks are being added to the queue, the displayed task count may grow beyond the actual total. Solution: after waiting a while (5-10 min), the actual task count should be displayed correctly.

##### Q: How do I abort processing the items?
A: It depends on what stage Feedloader processing has reached. If the items are currently being processed by AppEngine, you must go to the GCP admin console, go to Cloud Tasks, and then pause the Task Queue and purge it.

##### Q: How do I reset the BigQuery tables/Cloud Storage if I want to start clean?
A: Run a `DELETE FROM streaming_items WHERE true` query in BigQuery (you need to prefix the table with the dataset). Then simply delete the `items` table (“drop”). Delete the lock file in the GCS lock bucket (denoted by your `LOCK_BUCKET` environment variable), and ensure files in the feed bucket (`FEED_BUCKET`) and completed files bucket (`COMPLETED_FILES_BUCKET`) are cleared out from GCS. Then feed files can be reuploaded.

##### Q: How do I configure Cloud Build?
A: These are deployed from the gcloud SDK CLI. They work on the concept of “Build Triggers”. A trigger comprises of a repository source (in our case Cloud Source Repositories, also created in the installation script), and a set of steps which are defined in a .yaml file. The trigger also contains environment variables which can be edited from the GCP admin console under Cloud Build, however the aforementioned build steps CANNOT be changed from the UI. A CLI redeploy would need to happen to read from the .yaml file.

##### Q: Why is there an `items` table AND a `streaming_items` table?
A: Several reasons. Partially due to the way Cloud Functions/BigQuery APIs work: they import a file from Cloud Storage directly. This means that no special columns can be added at import-time such as timestamps or additional metadata fields. The resulting BigQuery table on GCS import and trigger will be a 1:1 mapping of the feed file. This makes it difficult to know what a “batch” of files is. A timestamp would be needed to know when a full set of files was intended by a Feedloader user. Due to this, an EOF strategy is employed which uses another table called `streaming_items` to only store part of the feed data: a hash value of all columns, an item id, and a timestamp. This makes it possible to do a diff calculation on previous runs.

##### Q: Why are a bunch of temporary BigQuery tables getting created during the Task Queue run?
A: Due to limitations with the BigQuery API and AppEngine and the way we setup our data (a join is required to get all the columns for an item), it is necessary to materialize a table for each batch of 1000 items. These temp tables are queried from and the result is used in the actual Content API request.

##### Q: How do I download more than 300 lines of logs from Cloud Logging?
A: Sample command line
gcloud logging read "resource.type=gae_app AND logName=projects/my-gcp-project/logs/stderr AND textPayload:invalid MPN" --limit 1000 --format json

##### Q: How long does it take to upload items?
A: It depends on the number of items. It is possible to roughly calculate the total processing time by the formula below:

    Processing time = (# of items) / 60,000 mins + 5 mins

    For example, when a user uploads 7M items, the processing time is

    7,000,000 / 60,000 mins + 5 mins  = 116 mins + 5 mins = 121 mins = 2 hours
