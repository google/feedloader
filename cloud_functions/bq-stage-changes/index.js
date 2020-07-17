/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @fileoverview Cloud Function that copies imported items table IDs
 *     into a timestamped table and calculates which items to delete/upsert.
 */
const config = require('./config.json');

let deletesThreshold;
let upsertsThreshold;
let upsertSet = false;

const LOCK_FILE_NAME = 'EOF.lock';

// Supported actions interpreted by GAE.
const GAE_ACTIONS = Object.freeze({
  UPSERT: 'upsert',
  DELETE: 'delete',
  PREVENT_EXPIRING: 'prevent_expiring',
});

// Set the default expiration of the items table to 12 hours.
const ITEMS_TABLE_EXPIRATION_DURATION = 43200000;

/**
 * Writes items table to a streaming table with timestamps for each load,
 *     then calculates which items are missing since the last load and which
 *     items need to be upserted (changed or new items), then finally reports
 *     those counts to GAE via Task Queue.
 *
 * @param {{
 *     name: string,
 *     size: string
 * }} fileReference The event payload.
 * @param {{
 *     eventId: string,
 *     timestamp: string,
 *     eventType: string,
 *     resource: string
 * }} eventMetadata The event metadata.
 */
exports.calculateProductChanges = async (fileReference, eventMetadata) => {
  deletesThreshold = Number(process.env.DELETES_THRESHOLD);
  upsertsThreshold = Number(process.env.UPSERTS_THRESHOLD);

  const {BigQuery} = require('@google-cloud/bigquery');
  const {Storage} = require('@google-cloud/storage');
  const bigQuery = new BigQuery();
  const storage = new Storage();
  const dataset = bigQuery.dataset(process.env.BQ_DATASET);

  // The GCS file must be zero bytes and named "EOF" in order to proceed.
  if ((fileReference.name !== 'EOF' && fileReference.name !== 'EOF.retry') ||
      Number(fileReference.size) !== 0) {
    console.error(
        `File ${fileReference.name} was not an empty EOF! Exiting Function...`);
    return;
  }

  // If the file that triggered this function did not originate from the retry
  // function, check for lock existence and create lock if necessary.
  if (fileReference.name === 'EOF') {
    let lockExists;
    try {
      lockExists = await checkLockExists(storage);
    } catch (error) {
      console.error('EOF.lock check failed.', error);
      return;
    }
    if (lockExists) {
      console.error(
          'An EOF.lock file was found, which indicates that this ' +
          'function is still running. Exiting Function...');
      return;
    }

    // Lock the EOF file at this point to prevent concurrent runs.
    const bucketObj = storage.bucket(fileReference.bucket);
    const eofObj = bucketObj.file(fileReference.name);
    const eofLockResult = await lockEof(eofObj);
    if (!eofLockResult) {
      console.error('EOF could not be locked! Exiting Function...');
      return;
    }
  }

  if (!deletesThreshold || isNaN(deletesThreshold) || deletesThreshold <= 0) {
    // Set a default tolerance threshold for number of deletes.
    deletesThreshold = 100000;
  }
  if (upsertsThreshold && !isNaN(upsertsThreshold) && upsertsThreshold > 0) {
    upsertSet = true;
  }

  // BigQuery Table names to read/write.
  const Tables = Object.freeze({
    DELETES: 'items_to_delete',
    EXPIRING: 'items_to_prevent_expiring',
    ITEMS: 'items',
    STREAMING: 'streaming_items',
    UPSERTS: 'items_to_upsert',
  });

  console.log(
      'Empty EOF file detected. Starting diff calculation in BigQuery...');

  // Stop execution and retry if any imports to BigQuery were missed/dropped.
  const importSuccess = await ensureAllFilesWereImported(dataset, storage);
  if (!importSuccess) {
    return;
  }

  console.log(
      'All the feeds are loaded. Continuing calculateProductUpdates...');

  // Set an expiration time limit on the items table since at this point
  // the items table should be fully loaded for the run.
  setTableExpirationDate(dataset, 'items', ITEMS_TABLE_EXPIRATION_DURATION);

  // Archive the feed files that were already loaded by moving them to a
  // timestamped subfolder in the Cloud Storage bucket.
  if (!await archiveFolder(storage)) {
    await cleanup(dataset, storage);
    return;
  }

  const streamingTable = dataset.table(Tables.STREAMING);
  const expirationTable = dataset.table(Tables.EXPIRING);
  const upsertsTable = dataset.table(Tables.UPSERTS);
  const deletesTable = dataset.table(Tables.DELETES);
  const itemsTable = dataset.table(Tables.ITEMS);

  const tableExistenceResults = await Promise.all([
    checkTableExists(streamingTable),
    checkTableExists(upsertsTable),
    checkTableExists(deletesTable),
    checkTableExists(itemsTable),
    checkTableExists(expirationTable),
  ]);

  if (tableExistenceResults.includes(false)) {
    console.error('One or more required tables do not exist. Aborting...');
    await cleanup(dataset, storage);
    return;
  }

  const queries = require('./queries');

  // Validate, parse, and generate SQL from the BigQuery schema config file.
  const configExists = (config && config.mapping) || false;
  if (!configExists || !Array.isArray(config.mapping)) {
    console.error(
        'Unable to map any columns from the schema config. Aborting...');
    await cleanup(dataset, storage);
    return;
  }
  const filteredConfigEntries = config.mapping.filter((x) => !!x.bqColumn);
  const columnsToHash =
      filteredConfigEntries
          .map((x) => `IFNULL(CAST(Items.${x.bqColumn} AS STRING), 'NULL')`)
          .join(', ');

  if (!columnsToHash) {
    console.error(
        'Unable to map any columns from the schema config. Aborting...');
    await cleanup(dataset, storage);
    return;
  }
  let merchantIdColumn = '';
  if (filteredConfigEntries.map((x) => x.bqColumn)
          .includes('google_merchant_id')) {
    merchantIdColumn = 'google_merchant_id,';
  }

  // Copy over the imported items to another table
  // in order to add timestamps and hashes of the item data.
  await runMaterializeJob(
      bigQuery, streamingTable,
      queries.COPY_ITEM_BATCH_QUERY
          .replace('{{COLUMNS_TO_HASH}}', columnsToHash)
          .replace('{{MC_COLUMN}}', merchantIdColumn),
      'WRITE_APPEND');

  // Find out how many items need to be deleted, if any.
  const deletesInsertJobId = await runMaterializeJob(
      bigQuery, deletesTable, queries.CALCULATE_ITEMS_FOR_DELETION_QUERY,
      'WRITE_TRUNCATE');

  // Start the "upserts" calculation. This is done with two
  // separate queries, one for updates, one for inserts.
  const updatesInsertJobId = await runMaterializeJob(
      bigQuery, upsertsTable,
      queries.CALCULATE_ITEMS_FOR_UPDATE_QUERY.replace(
          '{{COLUMNS_TO_HASH}}', columnsToHash),
      'WRITE_TRUNCATE');

  // Newly inserted items cannot rely on hashes used by the above query
  // to detect them, so append these results to the upserts table too.
  const insertsInsertJobId = await runMaterializeJob(
      bigQuery, upsertsTable, queries.CALCULATE_ITEMS_FOR_INSERTION_QUERY,
      'WRITE_APPEND');

  // Populate the items to prevent expiring table with items that have
  // not been touched in EXPIRATION_THRESHOLD days.
  const expirationInsertJobId = await runMaterializeJob(
      bigQuery, expirationTable, queries.GET_EXPIRING_ITEMS_QUERY,
      'WRITE_TRUNCATE');

  // Get the count of how many items need to be deleted,
  let deleteCount = await countChanges(
      bigQuery, queries.COUNT_DELETES_QUERY, GAE_ACTIONS.DELETE,
      deletesInsertJobId);
  if (deleteCount < 0) {
    console.error('Delete count and publish job failed. ' +
                  'Skipping deletion processing.');

    // Zero-out the delete count so that processing can continue without doing
    // any delete operations for expiration prevention purposes.
    deleteCount = 0;
  }

  // Get the count of the number of upserts
  let upsertCount = await countChanges(
      bigQuery, queries.COUNT_UPSERTS_QUERY, GAE_ACTIONS.UPSERT,
      `${updatesInsertJobId} + ${insertsInsertJobId}`);
  if (upsertCount < 0) {
    console.error('Upsert count and publish job failed. ' +
                  'Skipping upsert processing.');

    // Zero-out the upsert count so that processing can continue without doing
    // any upsert operations.
    upsertCount = 0;

    // Clean up the streaming_items table if upserts threshold was crossed
    // so that this run's items can be upserted in the future.
    await runDmlJob(bigQuery, queries.DELETE_LATEST_STREAMING_ITEMS);
  }

  // Get the count of the number of expiring items
  let expiringCount = await countChanges(
      bigQuery, queries.COUNT_EXPIRING_QUERY, GAE_ACTIONS.PREVENT_EXPIRING,
      expirationInsertJobId);
  if (expiringCount < 0) {
    console.error('Expiring count and publish job failed. Exiting...');

    // Zero-out the expiring count so that processing can continue without doing
    // any expiration operations for expiration prevention purposes.
    expiringCount = 0;
  }

  if (deleteCount > deletesThreshold) {
    console.error(
        `Deletes count ${deleteCount} crossed deletes threshold ` +
        `of ${deletesThreshold} items. Skipping delete feed processing...`);

    // Zero-out the delete count so that processing can continue without doing
    // any delete operations.
    deleteCount = 0;
  }

  if (upsertSet && upsertCount > upsertsThreshold) {
    console.error(
        `Upsert count ${upsertCount} crossed upserts threshold ` +
        `of ${upsertsThreshold} items. Skipping upsert feed processing...`);

    // Zero-out the upsert count so that processing can continue without doing
    // any upsert operations.
    upsertCount = 0;

    // Clean up the streaming_items table
    // so that this run's items can be upserted in the future.
    await runDmlJob(bigQuery, queries.DELETE_LATEST_STREAMING_ITEMS);
  }

  // Trigger AppEngine with a Task Queue task containing the query results
  const taskPayload = JSON.stringify({
    deleteCount: deleteCount,
    expiringCount: expiringCount,
    upsertCount: upsertCount,
  });

  const createTaskResult = await createTask(
      process.env.GCP_PROJECT, 'trigger-initiator', 'us-central1', taskPayload);
  if (!createTaskResult) {
    await cleanup(dataset, storage);
    await runDmlJob(bigQuery, queries.DELETE_LATEST_STREAMING_ITEMS);
    return;
  }
};

/**
 * Checks if BigQuery table exists or not.
 *
 * @param {!Table} table The BigQuery table.
 * @return {boolean} True if the table was found in BigQuery, otherwise false.
 */
async function checkTableExists(table) {
  const tableExistsResult = await table.exists();
  if (!tableExistsResult || !tableExistsResult[0]) {
    console.error(
        `${table.id} table must exist before ` +
        'running the product calculation function.');
    return false;
  }
  return tableExistsResult[0];
}

/**
 * Writes query results to a table.
 *
 * @param {!BigQuery} bigQuery The BigQuery class instance.
 * @param {!Table} destinationTable The BigQuery table to write to.
 * @param {string} query The BigQuery SQL to run.
 * @param {string} writeDisposition Specifies the action that occurs
 *     if the destination table already exists. See
 *     https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs for
 *     details.
 * @return {number} The ID of the BigQuery job that was run.
 */
async function runMaterializeJob(
    bigQuery, destinationTable, query, writeDisposition) {
  let bigQueryJob = {id: 0};
  await bigQuery
      .createQueryJob({
        destination: destinationTable,
        query: query,
        writeDisposition: writeDisposition,
      })
      .then((bigQueryJobResults) => {
        // bigQueryJobResults contains the started Job object.
        bigQueryJob = bigQueryJobResults[0];
        console.log(`Job ${bigQueryJob.id} started.`);
        return bigQueryJob.promise();
      })
      .then(() => {
        return bigQueryJob.getMetadata();
      })
      .then((metadata) => {
        // The job metadata contains the job's status and any errors.
        const errors = metadata[0].status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .then(() => {
        console.log(
            `createQueryJob into ${destinationTable.id} table job ` +
            `${bigQueryJob.id} completed. Query was: ${query}`);
      })
      .catch((error) => {
        console.error(
            `createQueryJob Error for Job ID ${bigQueryJob.id}: ` +
                `Query was: ${query}`,
            error);
      });

  return bigQueryJob.id;
}

/**
 * Runs a DML statement on a BigQuery table.
 *
 * @param {!BigQuery} bigQuery The BigQuery class instance.
 * @param {string} query The BigQuery SQL to run.
 * @return {number} The ID of the BigQuery job that was run.
 */
async function runDmlJob(bigQuery, query) {
  let bigQueryJob = {id: 0};
  await bigQuery
      .createQueryJob({
        query: query,
      })
      .then((bigQueryJobResults) => {
        // bigQueryJobResults contains the started Job object.
        bigQueryJob = bigQueryJobResults[0];
        console.log(`Job ${bigQueryJob.id} started.`);
        return bigQueryJob.promise();
      })
      .then(() => {
        return bigQueryJob.getMetadata();
      })
      .then((metadata) => {
        // The job metadata contains the job's status and any errors.
        const errors = metadata[0].status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .then(() => {
        console.log(
            'DML query with job ID ' +
            `${bigQueryJob.id} completed. Query was: ${query}`);
      })
      .catch((error) => {
        console.error(
            `createQueryJob Error for Job ID ${bigQueryJob.id}. ` +
                `Query was: ${query}`,
            error);
      });

  return bigQueryJob.id;
}

/**
 * Runs the given query to count the number of changes and returns the result.
 *
 * @param {!BigQuery} bigQuery The BigQuery class instance.
 * @param {string} query The BigQuery SQL to run.
 * @param {string} action The type of API action to perform.
 * @param {string} jobId The BigQuery job ID to log.
 * @return {number} The query result count of items.
 */
async function countChanges(bigQuery, query, action, jobId) {
  const options = {
    query: query,
    location: 'US',
  };

  let changesCount = NaN;

  try {
    const [job] = await bigQuery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);
    const [rows] = await job.getQueryResults();

    // This condition checks for null and empty string because
    // Number(null) or Number('') will return 0 rather than NaN.
    // Number(undefined) will return NaN, so this condition will be
    // caught by the if (isNaN)... block below.
    if (!rows || rows.length === 0 || !(rows[0] instanceof Object) ||
        rows[0]['f0_'] === null || rows[0]['f0_'] === '') {
      console.error(`Query rows result for ${action} was null or empty.`);
      return -1;
    }

    changesCount = Number(rows[0]['f0_']);
  } catch (error) {
    console.error('Exception occurred in running query: ' + query, error);
    return -1;
  }

  if (isNaN(changesCount)) {
    console.error(
        `Count ${action}s could not be extracted ` +
        'from the query result.');
    return -1;
  }

  console.log(`Number of rows to ${action} in this run: ` + changesCount);
  return changesCount;
}

/**
 * Checks if a file named "EOF.lock" exists in the bucket.
 *
 * @param {!Storage} storage The GCP Storage API object.
 * @return {!Promise<boolean>} True if the EOF.lock exists in the bucket,
 *     otherwise false.
 */
async function checkLockExists(storage) {
  const eofLockBucket = storage.bucket(process.env.LOCK_BUCKET);
  const eofLockFile = eofLockBucket.file(LOCK_FILE_NAME);
  const [eofLockExists] = await eofLockFile.exists();
  return eofLockExists;
}

/**
 * Renames the EOF file to EOF.lock in order to prevent concurrent runs.
 *
 * @param {!File} eofObj A GCP Storage API File object.
 * @return {!Promise<boolean>} True if the EOF was renamed successfully,
 *     otherwise false.
 */
async function lockEof(eofObj) {
  try {
    // Because .move() requires the gs:// prefix for Storage buckets,
    // handle both possibilities of user-generated bucket names by stripping
    // out the gs:// prefix if it exists and explicitly prepending it.
    const lockDestination = 'gs://' +
        process.env.LOCK_BUCKET.replace('gs://', '') + '/' + LOCK_FILE_NAME;
    await eofObj.move(lockDestination);
  } catch (error) {
    console.error(`Could not rename file: ${eofObj.name}. Error:`, error);
    return false;
  }
  return true;
}

/**
 * Validates that all files imported to BQ match files uploaded to GCS.
 *
 * @param {!Dataset} dataset The BigQuery Dataset class instance.
 * @param {!Storage} storage The GCP Storage API object.
 * @return {boolean} True if all expected files were imported, otherwise false.
 */
async function ensureAllFilesWereImported(dataset, storage) {
  const options = {
    delimiter: 'archives',
  };
  let [attemptedFiles] =
      await storage.bucket(process.env.FEED_BUCKET).getFiles(options);

  if (!attemptedFiles || attemptedFiles.length === 0) {
    console.error('attemptedFiles retrieval failed.');
    await cleanup(dataset, storage);
    return false;
  }

  let [importedFiles] =
      await storage.bucket(process.env.COMPLETED_FILES_BUCKET).getFiles();

  if (!importedFiles) {
    console.error('importedFiles retrieval failed.');
    await cleanup(dataset, storage);
    return false;
  }

  attemptedFiles = attemptedFiles.map((file) => file.name);
  console.log('attemptedFiles', attemptedFiles);
  importedFiles = importedFiles.map((file) => file.name);
  console.log('importedFiles', importedFiles);

  const missingFiles =
      attemptedFiles.filter((file) => importedFiles.indexOf(file) === -1);

  const missingFilesStr = missingFiles.join('\n');

  console.log('missingFiles', missingFiles);
  console.log('missingFilesStr', missingFilesStr);

  if (missingFiles.length !== 0) {
    await storage.bucket(process.env.RETRIGGER_BUCKET)
        .file('REPROCESS_TRIGGER_FILE')
        .save(missingFilesStr)
        .then((data) => {
          console.log(
              `${missingFiles.length} files are missing. ` +
              'Inserted Retry Trigger.');
        });
    return false;
  }

  return await cleanupCompletedFilenames(storage);
}

/**
 * Removes all files from the completed filenames GCS bucket
 *
 * @param {!Storage} storage The GCP Storage API object.
 * @return {boolean} True if files were removed successfully, otherwise false.
 */
async function cleanupCompletedFilenames(storage) {
  // All data was verified to have been imported into BigQuery, so the
  // completed filenames should be cleaned from the bucket before the next run.
  console.log('Removing temp feed filenames from GCS...');
  let cleanupCompletedBucketSuccess = true;
  storage.bucket(process.env.COMPLETED_FILES_BUCKET)
      .deleteFiles({force: true}, (errors) => {
        if (errors && errors.length > 0) {
          console.error(
              'One or more errors occurred when deleting from the ' +
                  'completed filenames bucket.',
              errors);
          cleanupCompletedBucketSuccess = false;
        }
      });
  console.log(
      'Finished deleting completed filenames from ' +
      `${process.env.COMPLETED_FILES_BUCKET}.`);
  return cleanupCompletedBucketSuccess;
}
/**
 * Renames current feeds to subfolder with timestamp for archival purposes.
 *
 * @param {!Storage} storage The GCP Storage API object.
 * @return {!Promise<boolean>} True if files cleaned up successfully, otherwise
 *     false.
 */
async function archiveFolder(storage) {
  console.log('Attempting to archive feeds in GCS...');
  const fileReadOptions = {
    delimiter: 'archives',
  };

  const feedBucketName = process.env.FEED_BUCKET.replace('gs://', '');
  const [feeds] =
      await storage.bucket(feedBucketName).getFiles(fileReadOptions);
  if (!feeds || feeds.length === 0) {
    console.error(
        'Could not find any feeds to archive. ' +
        'Did you upload an EOF without uploading feeds?');
    return false;
  }
  const archiveDate = new Date().toISOString();

  // Move each feed file into the archive GCS bucket.
  // Because .move() requires the gs:// prefix for Storage buckets,
  // handle both possibilities of user-generated bucket names by stripping
  // out the gs:// prefix if it exists and explicitly prepending it.
  for (feed of feeds) {
    await feed
        .move(
            'gs://' +
            `${process.env.ARCHIVE_BUCKET.replace('gs://', '')}/archives/` +
            `${archiveDate}/${feed.name}`)
        .catch((error) => {
          console.error(`Could not move file ${feed.name}. Error:`, error);
          return false;
        });
  }

  console.log('Finished archiving.');
  return true;
}

/**
 * Creates a Task on the trigger-initiator Task Queue to start App Engine.
 *
 * @param {string} project The GCP Project id.
 * @param {string} queueName The name of your Queue.
 * @param {string} location The GCP region of your queue.
 * @param {string} payload The task HTTP request body.
 * @return {!Promise<boolean>} True if task created successfully, otherwise
 *     false.
 */
async function createTask(project, queueName, location, payload) {
  const {CloudTasksClient} = require('@google-cloud/tasks');
  const tasks = new CloudTasksClient();
  const parent = tasks.queuePath(project, location, queueName);

  const task = {
    appEngineHttpRequest: {
      httpMethod: 'POST',
      relativeUri: '/start',
    },
  };

  if (payload) {
    task.appEngineHttpRequest.body = Buffer.from(payload).toString('base64');
  }

  console.log('Sending task: ', task);
  const request = {parent, task};
  try {
    const [response] = await tasks.createTask(request);
    if (!response || !response.name) {
      console.error('Task creation returned unexpected response: ', response);
      return false;
    }

    console.log(
        `Created task with name: ${response.name}. Full response was: `,
        response);
    return true;
  } catch (error) {
    console.error('Error occurred when creating a Task to start GAE', error);
    return false;
  }
}

/**
 * Cleans up the state of the run (items table and EOF) upon error cases.
 * @param {!Dataset} dataset The BigQuery Dataset class instance.
 * @param {!Storage} storage The Cloud Storage class instance.
 */
async function cleanup(dataset, storage) {
  const eofLockBucket = storage.bucket(process.env.LOCK_BUCKET);
  const eofLockFile = eofLockBucket.file(LOCK_FILE_NAME);
  const [eofLockExists] = await eofLockFile.exists();

  if (eofLockExists) {
    try {
      await eofLockFile.delete();
    } catch (error) {
      console.error(
          `${LOCK_FILE_NAME} deletion failed. Subsequent runs will ` +
              'be blocked unless the lock file is removed.',
          error);
      return;
    }
    console.log(`${LOCK_FILE_NAME} was deleted successfully.`);
  }

  dataset.table('items')
      .delete()
      .then((apiResponse) => {
        console.log(
            'Items table was deleted successfully. Response: ', apiResponse);
      })
      .catch((error) => {
        console.error('Items table could not be deleted.', error);
      });
}

/**
 * Sets the time that the given table expires so that in the worst case
 *     if the table isn't cleaned up, it will automatically delete itself.
 *
 * @param {!Dataset} dataset The BigQuery Dataset class instance.
 * @param {string} tableName The name of the table to set the expiration on.
 * @param {!integer} durationInMs The length of time in milliseconds before the
 * table expires.
 */
function setTableExpirationDate(dataset, tableName, durationInMs) {
  const targetTable = dataset.table(tableName);
  const expirationTimeInMs = Date.now() + durationInMs;
  const expirationMetadata = {'expirationTime': expirationTimeInMs.toString()};
  targetTable.setMetadata(expirationMetadata);
}
