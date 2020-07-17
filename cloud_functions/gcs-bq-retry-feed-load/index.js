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
 * @fileoverview Cloud Function that re-loads a file into BigQuery
 * specified by the filename received from PubSub.
 */
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const config = require('./config.json');

/**
 * The default expiration of the items table is 12 hours.
 * @const {number}
 */
const ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000;

/**
 * Set the default expiration of this Cloud Function to 9 minutes in ms.
 * @const {number}
 */
const EVENT_MAX_AGE_MS = 540000;

const /** !Array<!Object<string, *>> */ schema =
    config.mapping.map((x) => ({name: x.bqColumn, type: x.columnType}));

// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#resource-representations
const /** !Object<string, *> */ bqMetadata = {
  'encoding': 'UTF-8',
  'quote': '',
  'schema': {
    // Need to explicitly declare the schema because autodetect is inconsistent.
    'fields': schema,
  },
  'sourceFormat': 'CSV',
  'location': 'US',
  'writeDisposition': 'WRITE_APPEND',
  'skipLeadingRows': 1,
  'maxBadRecords': 0,
  'fieldDelimiter': '\t',
  'allowJaggedRows': true,
  'timePartitioning': {
    'type': 'DAY',
    'expirationMs': ITEMS_TABLE_EXPIRATION_DURATION_MS,
  },
};

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
exports.reprocessFeedfile = async (fileReference, context) => {
  // Prevent long-running retries
  const eventAge = Date.now() - Date.parse(context.timestamp);
  if (eventAge > EVENT_MAX_AGE_MS) {
    console.error(`Dropping event ${context.eventId} with age ${eventAge} ms.`);
    return;
  }
  console.log('Starting reprocessFeedfile Cloud Function...');
  const storage = new Storage();
  const retriggerBucketObj = storage.bucket(process.env.RETRIGGER_BUCKET);
  const missingFilesObj = retriggerBucketObj.file(fileReference.name);
  let fileListBuffer = new Buffer('');
  let fileListContent = '';
  try {
    await missingFilesObj.createReadStream()
        .on('error',
            (error) => {
              console.log(error);
            })
        .on('data',
            (chunk) => {
              console.log('chunk', chunk);
              fileListBuffer = Buffer.concat([fileListBuffer, chunk]);
              console.log('fileListContent', fileListBuffer);
            })
        .on('end', async () => {
          fileListContent = fileListBuffer.toString('utf8');
          console.log('fileListContentToUtf8', fileListContent);
          if (fileListContent.length === 0) {
            // No files to reprocess, re-trigger calculateProductChanges
            retriggerCalculationFunction(storage);
          } else {
            console.log(fileListContent);
            let fileList = fileListContent.split('\n');
            const fileToReprocess = fileList.shift();
            const reprocessSuccess =
                await reprocessFile(storage, fileToReprocess);
            if (!reprocessSuccess) {
              console.error('Reprocessing failed. Exiting Cloud Function...');
              return;
            }
            await reUploadFileList(storage, fileList, fileReference.name);
          }
        });
  } catch (error) {
    console.error(
        'Error encountered while parsing and reprocessing the ' +
        'retry file. Exiting the Cloud Function.');
    return;
  }
};

/**
 * Reloads specified filename from Cloud Storage into BigQuery.
 *
 * @param {!Storage} storage The GCS object instance.
 * @param {string} filename The filename.
 * @return {boolean} True if reprocess succeeded, or false if there was an
 *     error.
 */
async function reprocessFile(storage, filename) {
  console.log(`Attempting reprocess of file ${filename} into BigQuery...`);

  const feedBucketObj = storage.bucket(process.env.FEED_BUCKET);
  const fileObj = feedBucketObj.file(filename);
  const bigQuery = new BigQuery();
  const bqDataset = bigQuery.dataset(process.env.BQ_DATASET);
  const bqTable = bqDataset.table('items');
  let bqLoadJob;
  try {
    [bqLoadJob] = await bqTable.load(fileObj, bqMetadata);
  } catch (error) {
    console.error('Attempt to call BigQuery table load failed.', error);
    return false;
  }
  const errors = bqLoadJob.status.errors;
  if (errors && errors.length > 0) {
    console.error('Load into BigQuery returned, but with errors.', errors);
    return false;
  }
  console.log(
      `File:${filename} was loaded into BigQuery successfully. ` +
      `Job ID: ${bqLoadJob.id}`);

  // Write the successfully reprocessed filename to GCS.
  console.log('Starting insert of import history record...');
  const completedFilesBucket =
      process.env.COMPLETED_FILES_BUCKET.replace('gs://', '');

  let completedFilenameSaveSuccess = true;
  await storage.bucket(completedFilesBucket)
      .file(filename)
      .save('')
      .then((data) => {
        console.log(`Saved reprocessed file ${filename} to bucket.`, data);
      })
      .catch((error) => {
        console.error(
            'An error occurred when saving the reprocessed filename to GCS.',
            error);
        completedFilenameSaveSuccess = false;
      });
  return completedFilenameSaveSuccess;
}

/**
 * Reuploads updated list of missing files to Storage.
 *
 * @param {!Storage} storage The GCS object instance.
 * @param {!Array<string>} fileList Array containing each filename.
 * @param {string} filename The filename to re-upload to the bucket.
 * @return {boolean} True if uploading the file list succeeded,
 *     or false if there was an error.
 */
async function reUploadFileList(storage, fileList, filename) {
  // Re-notify the calculatePlaUpdates function to check import results
  const fileListContent = fileList.join('\n');
  const retriggerBucketName = process.env.RETRIGGER_BUCKET.replace('gs://', '');
  reuploadFileListSaveSuccess = true;
  await storage.bucket(retriggerBucketName)
      .file(filename)
      .save(fileListContent)
      .then((data) => {
        console.log(
            'Updated missing file list and re-uploaded it to bucket.', data);
      })
      .catch((error) => {
        console.error(
            'An error occurred when re-uploading the file list to GCS.', error);
        reuploadFileListSaveSuccess = false;
      });
  return reuploadFileListSaveSuccess;
}

/**
 * Uploads EOF.retry to re-trigger calculateProductChanges Cloud Function.
 *
 * @param {!Storage} storage The GCS object instance.
 */
function retriggerCalculationFunction(storage) {
  console.log('No more files to reprocess. Uploading a retry EOF.');
  const eofBucketName = process.env.UPDATE_BUCKET.replace('gs://', '');
  storage.bucket(eofBucketName).file('EOF.retry').save('').then((data) => {
    console.log('Retriggered calculateProductChanges function.', data);
  });
}
