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
 * @fileoverview Cloud Function that creates a partitioned BigQuery table from a
 * TSV-format Cloud Storage file whose upload triggers this function to run.
 */
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const config = require('./config.json');

// The default expiration of the items table is 12 hours.
const ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000;

// Set the default expiration of this Cloud Function to 9 minutes
const eventMaxAge = 540000;

/**
 * Imports a TSV into BigQuery triggered by Cloud Storage.
 *
 * @param {{
 *     name: string,
 *     bucket: string
 * }} fileReference The event payload.
 * @param {{
 *     eventId: string,
 *     timestamp: string,
 *     eventType: string,
 *     resource: string
 * }} context The event metadata.
 *
 */
exports.importStorageFileIntoBigQuery = async (fileReference, context) => {
  // Prevent long-running retries
  const eventAge = Date.now() - Date.parse(context.timestamp);
  if (eventAge > eventMaxAge) {
    console.error(`Dropping event ${context.eventId} with age ${eventAge} ms.`);
    return;
  }

  console.log('Starting importStorageFileIntoBigQuery Cloud Function...');
  console.log(`File detected with name: ${fileReference.name}`);

  // Allow only a subset of the files to be processed,
  // specified by the FILE_RANGE environment variable
  if (process.env.FILE_RANGE && process.env.FILE_RANGE.length > 0) {
    console.log(`Filtering files outside range: ${process.env.FILE_RANGE}`);
    const fileRange = process.env.FILE_RANGE.split('-');
    if (fileRange.length != 2 || isNaN(fileRange[0]) || isNaN(fileRange[1])) {
      console.error(
          'FILE_RANGE environment variable is incorrectly ' +
          'formatted. Format must be {(int)-(int)}.');
      return;
    }

    const fileNumMatches = fileReference.name.match(/\d+/g);
    if (fileNumMatches && fileNumMatches.length > 0) {
      const fileNumber = parseInt(fileNumMatches[0]);
      if (isNaN(fileNumber) || fileNumber < fileRange[0] ||
          fileNumber > fileRange[1]) {
        console.log(
            `Skipping BigQuery import: file with number ${fileNumber} ` +
            'is not in range, but file was processed successfully.');
        return;
      }

      console.log('File is in range.');
    } else {
      console.error(
          'Filename was not properly numbered ' +
          'in the format \"[filename]_12345.txt\".');
      return;
    }
  }

  // Validate and parse the BigQuery schema config file
  // for creating the items table.
  if (!Array.isArray(config.mapping)) {
    console.error(
        'Unable to map any columns from the schema config. Aborting...');
    return;
  }

  const schema = config.mapping.filter((x) => !!x.bqColumn)
                     .filter((x) => !!x.columnType)
                     .map((x) => ({name: x.bqColumn, type: x.columnType}));

  if (!schema || schema.length < 1) {
    console.error(
        'Unable to map any columns from the schema config. Aborting...');
    return;
  }

  // https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#resource-representations
  const bigQueryMetadata = {
    encoding: 'UTF-8',
    quote: '',
    schema: {
      // Explicitly declare the schema because autodetect is inconsistent.
      fields: schema,
    },
    sourceFormat: 'CSV',
    location: 'US',
    writeDisposition: 'WRITE_APPEND',
    skipLeadingRows: 1,
    maxBadRecords: 0,
    fieldDelimiter: '\t',
    allowJaggedRows: true,
    timePartitioning: {
      type: 'DAY',
      expirationMs: ITEMS_TABLE_EXPIRATION_DURATION_MS,
    },
  };

  console.log('Attempting import into BigQuery...');
  const storage = new Storage();

  // Use EOF.lock as a lock to prevent feed load if another run is happening.
  // EOF needs to also be checked for existence because of an edge case where
  // the calculateProductChanges function may not rename the file fast enough.
  const eofBucket = storage.bucket(process.env.UPDATE_BUCKET);
  const eofFile = eofBucket.file('EOF');
  const [eofExists] = await eofFile.exists();
  const eofLockBucket = storage.bucket(process.env.LOCK_BUCKET);
  const eofLockFile = eofLockBucket.file('EOF.lock');
  const [eofLockExists] = await eofLockFile.exists();
  if (eofExists) {
    console.warn(
        `An EOF file was found in ${process.env.UPDATE_BUCKET}. ` +
        'The bucket must be empty before loading feed data. Exiting...');
    return;
  }
  if (eofLockExists) {
    console.warn(
        `An EOF.lock file was found in ${process.env.LOCK_BUCKET}. ` +
        'The bucket must be empty before loading feed data. Exiting...');
    return;
  }

  const bucketObj = storage.bucket(fileReference.bucket);
  const fileObj = bucketObj.file(fileReference.name);
  const bigQuery = new BigQuery();
  const bqDataset = bigQuery.dataset(process.env.BQ_DATASET);
  const itemsTable = bqDataset.table('items');
  try {
    ([bqLoadJob] = await itemsTable.load(fileObj, bigQueryMetadata));
  } catch (error) {
    console.error(
        `BigQuery load job failed. Failed file: ${fileObj.name}`, error);
    return;
  }
  const errors = bqLoadJob.status.errors;
  if (errors && errors.length > 0) {
    console.error(
        'One or more errors occurred while loading the feed ' +
            'into BigQuery.',
        errors);
    return;
  }
  console.log(
      `File:${fileObj.name} was loaded into BigQuery successfully. ` +
      `Job ID: ${bqLoadJob.id}`);

  // Write the successfully imported filename to GCS to record history
  // which will be used for verification of success.
  console.log('Starting insert of import history record...');

  const completedFilesBucket =
      process.env.COMPLETED_FILES_BUCKET.replace('gs://', '');

  storage.bucket(completedFilesBucket)
      .file(fileReference.name)
      .save('')
      .then((data) => {
        console.log(
            `Saved completed file ${fileReference.name} to bucket.`, data);
      })
      .catch((error) => {
        console.error(
            'An error occurred when saving the completed filename to GCS.',
            error);
      });
  console.log(`Function complete for GCS file ${fileReference.name}`);
};
