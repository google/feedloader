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
 * @fileoverview Unit tests for failing cases of the calculateProductChanges
 * Cloud Function.
 */
const ava = require('ava');
const {BigQuery} = require('@google-cloud/bigquery');
const proxyquire = require('proxyquire');
const queries = require('./queries');
const sinon = require('sinon');
const sinonHelpers = require('sinon-helpers');
const {Storage} = require('@google-cloud/storage');
const {Tasks} = require('@google-cloud/tasks');

let bqTableDeleteStub_Success;
let bqTableStub;
let clock;
let consoleError;
let consoleLog;
let deleteMetadata_Error;
let deleteMetadata_Success;
let deletesInsertJobResult;
let eofDeleteStub_Exception;
let eofDeleteStub_Success;
let eofLockStub_CleanupSuccess;
let eofLockStub_FileExistsException;
let eofLockStub_FileUnlockedSuccess;
let eofLockStub_UnlockedDeleteException;
let eofMoveStub_Exception;
let expiringInsertJobResult;
let expiringMetadata_Success;
let feedFileMoveStub_Error;
let feedFileMoveStub_Success;
let getAttemptedFilesStub_Failure;
let getAttemptedFilesStub_Success;
let getFeedFiles_Empty;
let getFeedFiles_Error;
let getFeedFiles_Success;
let getMetadata_Error;
let getMetadata_Success;
let getMetadataStub_Default;
let getMetadataStub_Delete;
let insertsInsertJobResult;
let jobPromiseStub_Success;
let streamingInsertJobResult;
let truncateStreamingJobResult;
let updateBucketStub_Success;
let updatesInsertJobResult;

ava.before((t) => {
  process.env.ARCHIVE_BUCKET = 'testarchivebucket';
  process.env.BQ_DATASET = 'testdataset';
  process.env.COMPLETED_FILES_BUCKET = 'testcompletedbucket';
  process.env.FEED_BUCKET = 'testfeedbucket';
  process.env.GCP_PROJECT = 'testgcpproject';
  process.env.LOCK_BUCKET = 'testlockbucket';
  process.env.RETRIGGER_BUCKET = 'testretriggerbucket';
  process.env.UPDATE_BUCKET = 'testupdatebucket';
  jobPromiseStub_Success = sinon.stub().resolves();
  bqTableStub = sinon.stub();
  bqTableDeleteStub_Success = sinon.stub().resolves(['response']);
  eofDeleteStub_Success = sinon.stub().resolves({});
  eofDeleteStub_Exception =
      sinon.stub().throws(new Error('EOF Delete failed.'));
  feedFileMoveStub_Success = sinon.stub().resolves();
  feedFileMoveStub_Error = sinon.stub().rejects('Feed move failed.');
  updateBucketStub_Success = sinon.stub();
  updateBucketStub_Success.withArgs(sinon.match('EOF')).returns({
    move: sinon.stub().resolves({}),
    name: 'EOF',
  });
  eofLockStub_FileUnlockedSuccess = sinon.stub();
  eofLockStub_FileUnlockedSuccess.withArgs(sinon.match('EOF.lock')).returns({
    delete: sinon.stub().resolves({}),
    exists: sinon.stub().resolves([false]),
  });
  eofLockStub_CleanupSuccess = sinon.stub();
  eofLockStub_CleanupSuccess.withArgs(sinon.match('EOF.lock')).returns({
    delete: eofDeleteStub_Success,
    exists: sinon.stub().onCall(0).resolves([false]).onCall(1).resolves([true]),
  });
  eofLockStub_UnlockedDeleteException =
      sinon.stub().withArgs(sinon.match('EOF.lock')).returns({
        delete: eofDeleteStub_Exception,
        exists:
            sinon.stub().onCall(0).resolves([false]).onCall(1).resolves([true]),
      });
  eofLockStub_FileExistsException =
      sinon.stub().withArgs(sinon.match('EOF.lock')).returns({
        exists: sinon.stub().throws(new Error('Exists failed')),
                                   move: sinon.stub().resolves({}),
      });
  eofMoveStub_Exception = sinon.stub().withArgs(sinon.match('EOF')).returns({
    move: sinon.stub().rejects(new Error('Move failed')),
    name: 'EOF',
  });
  consoleLog = sinon.stub(console, 'log');
  consoleError = sinon.stub(console, 'error');
});

ava.beforeEach((t) => {
  consoleLog.resetHistory();
  consoleError.resetHistory();
  bqTableStub.resetHistory();
  feedFileMoveStub_Success.resetHistory();
  bqTableStub.withArgs('streaming_items')
      .returns({
        exists: sinon.stub().resolves([true]),
        id: 'streaming_items',
      })
      .withArgs('items_to_upsert')
      .returns({
        exists: sinon.stub().resolves([true]),
        id: 'items_to_upsert',
      })
      .withArgs('items_to_delete')
      .returns({
        exists: sinon.stub().resolves([true]),
        id: 'items_to_delete',
      })
      .withArgs('items_to_prevent_expiring')
      .returns({
        exists: sinon.stub().resolves([true]),
        id: 'items_to_prevent_expiring',
      })
      .withArgs('items')
      .returns({
        delete: bqTableDeleteStub_Success,
        exists: sinon.stub().resolves([true]),
        id: 'items',
        setMetadata: sinon.stub().withArgs(sinon.match.object),
      })
      .withArgs('items_to_prevent_expiring')
      .returns({
        exists: sinon.stub().resolves([true]),
        id: 'items_to_prevent_expiring',
      });
  clock = sinon.useFakeTimers({
    now: new Date('2019-01-01T12:00:00'),
    shouldAdvanceTime: false,
  });
  getMetadata_Success = [{
    status: {
      errors: null,
    },
  }];
  getMetadata_Error = [{
    status: {
      errors: [new Error('An Error')],
    },
  }];
  getMetadataStub_Default = sinon.stub().resolves(getMetadata_Success);
  streamingInsertJobResult = [
    {
      id: 1,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  deletesInsertJobResult = [
    {
      id: 2,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  updatesInsertJobResult = [
    {
      id: 3,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  insertsInsertJobResult = [
    {
      id: 4,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  expiringInsertJobResult = [
    {
      id: 5,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  truncateStreamingJobResult = [
    {
      id: 6,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Default
    },
  ];
  getFeedFiles_Success = [[
    {
      name: 'file1.txt',
      move: feedFileMoveStub_Success,
    },
    {
      name: 'file2.txt',
      move: feedFileMoveStub_Success,
    },
    {
      name: 'file3.txt',
      move: feedFileMoveStub_Success,
    },
  ]];
  getFeedFiles_Empty = [[]];
  getFeedFiles_Error = [[
    {
      name: 'file1.txt',
      move: feedFileMoveStub_Success,
    },
    {
      name: 'file2.txt',
      move: feedFileMoveStub_Error,
    },
  ]];
  getAttemptedFilesStub_Success =
      sinon.stub().withArgs(process.env.FEED_BUCKET);
  getAttemptedFilesStub_Success.onCall(0).resolves(getFeedFiles_Success);
  getAttemptedFilesStub_Success.onCall(1).resolves(
      [[{move: sinon.stub().resolves()}]]);
  getAttemptedFilesStub_Failure =
      sinon.stub().withArgs(process.env.FEED_BUCKET);
  getAttemptedFilesStub_Failure.onCall(0).resolves(getFeedFiles_Success);
  getAttemptedFilesStub_Failure.onCall(1).resolves(getFeedFiles_Error);
  t.context = {
    attemptedFilesBucketSuccessStub: {
      getFiles: getAttemptedFilesStub_Success,
    },
    attemptedFilesBucketFailureStub: {
      getFiles: getAttemptedFilesStub_Failure,
    },
    completedFilesBucketSuccessStub: {
      getFiles: sinon.stub().resolves(getFeedFiles_Success),
      deleteFiles: sinon.stub().yields(null),
    },
    completedFilesBucketFailureStub: {
      getFiles: sinon.stub().resolves(getFeedFiles_Success),
      deleteFiles: sinon.stub().yields([new Error('Delete Error!')]),
    },
    context: {},
    countDeletesJobResult: [
      {
        id: 5,
        getQueryResults: sinon.stub().resolves([[{'f0_': 1}]]),
      },
    ],
    countDeletesQueryJobObject: {
      query: queries.COUNT_DELETES_QUERY,
      location: 'US',
    },
    countExpiringJobResult: [
      {
        id: 7,
        getQueryResults: sinon.stub().resolves([[{'f0_': 3}]]),
      },
    ],
    countExpiringQueryJobObject: {
      query: queries.COUNT_EXPIRING_QUERY,
      location: 'US',
    },
    countUpsertsJobResult: [
      {
        id: 6,
        getQueryResults: sinon.stub().resolves([[{'f0_': 2}]]),
      },
    ],
    countUpsertsQueryJobObject: {
      query: queries.COUNT_UPSERTS_QUERY,
      location: 'US',
    },
    createTaskDefaultStub:
        sinon.stub().withArgs(sinon.match.object).resolves([{name: 'success'}]),
    createTaskFailedStub: sinon.stub()
                              .withArgs(sinon.match.object)
                              .rejects(new Error('Task Creation Failed')),
    data: {
      name: 'EOF',
      bucket: 'testupdatebucket',
      size: 0,
    },
    deletesQueryJobObject: {
      destination: sinon.match.any,
      query: queries.CALCULATE_ITEMS_FOR_DELETION_QUERY,
      writeDisposition: 'WRITE_TRUNCATE',
    },
    emptyConfig: {
      'mapping': [],
    },
    expiringQueryJobObject: {
      destination: sinon.match.any,
      query: queries.GET_EXPIRING_ITEMS_QUERY,
      writeDisposition: 'WRITE_TRUNCATE',
    },
    feedFilesBucketStubEmpty: {
      getFiles: sinon.stub().resolves(getFeedFiles_Empty),
    },
    importedFilesBucketStubEmpty: {
      getFiles: sinon.stub().resolves([undefined]),
    },
    insertsQueryJobObject: {
      destination: sinon.match.any,
      query: queries.CALCULATE_ITEMS_FOR_INSERTION_QUERY,
      writeDisposition: 'WRITE_APPEND',
    },
    invalidConfigString: {
      'mapping': 'invalidValue',
    },
    invalidConfigColumnKey: {
      'mapping': [{'incorrectColumn': 'id'}],
    },
    validConfig: {
      'mapping': [
        {'csvHeader': 'item_id', 'bqColumn': 'item_id', 'columnType': 'STRING'},
        {'csvHeader': 'title', 'bqColumn': 'title', 'columnType': 'STRING'},
      ],
    },
    streamingQueryJobObject: {
      destination: sinon.match.any,
      query: sinon.match(
          'TO_BASE64(SHA1(CONCAT(IFNULL(CAST(Items.item_id AS STRING), ' +
          '\'NULL\'), IFNULL(CAST(Items.title AS STRING), \'NULL\'))))'),
      writeDisposition: 'WRITE_APPEND',
    },
    taskQueuePathDefaultStub:
        sinon.stub()
            .withArgs(
                sinon.match.string, sinon.match.string, sinon.match.string)
            .returns({}),
    truncateStreamingQueryJobObject: {
      query: sinon.match('DELETE FROM'),
    },
    updatesQueryJobObject: {
      destination: sinon.match.any,
      query: sinon.match(
          'TO_BASE64(SHA1(CONCAT(IFNULL(CAST(Items.item_id AS STRING), ' +
          '\'NULL\'), IFNULL(CAST(Items.title AS STRING), \'NULL\'))))'),
      writeDisposition: 'WRITE_TRUNCATE',
    },
  };
});

ava.after((t) => {
  clock.restore();
});

/**
 * Sets stub behavior on a sinon-helpers instance with
 * the specified test context objects.
 *
 * @param {!Object} feedStub The stub for Storage with args passed in for the
 *     Feed Bucket.
 * @param {!Object} lockStub The stub for Storage with args passed in for the
 *     Lock Bucket.
 * @param {!Object} updateStub The stub for Storage with args passed in for the
 *     Update Bucket.
 * @param {!Object} completedStub The stub for Storage with args passed in for
 *     the Completed Bucket.
 *
 * @return {!Object} The initialized Storage stub.
 */
function setupStorageInstanceStub(
    feedStub, lockStub, updateStub, completedStub) {
  return sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
    instance.bucket.withArgs(process.env.FEED_BUCKET)
        .returns(feedStub)
        .withArgs(process.env.LOCK_BUCKET)
        .returns({
          file: lockStub,
        })
        .withArgs(process.env.UPDATE_BUCKET)
        .returns({
          file: updateStub,
        })
        .withArgs(process.env.COMPLETED_FILES_BUCKET)
        .returns(completedStub);
  });
}

ava('calculateProductChanges: should log error on failed ' +
        'EOF.lock existence check',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileExistsException, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith('EOF.lock check failed.'));
    });

ava('calculateProductChanges: should log error on failed EOF move',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, eofMoveStub_Exception,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Could not rename file: EOF. Error:')));
    });

ava('calculateProductChanges: archiveFolder logs error if ' +
        'feed move fails.',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketFailureStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Could not move file file2.txt.')));
    });

ava('calculateProductChanges: ensureAllFilesWereImported logs error if ' +
        'getFiles returns no files in the imported feeds bucket',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.feedFilesBucketStubEmpty,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('attemptedFiles retrieval failed')));
    });

ava('calculateProductChanges: logs error if no imported files could ' +
        'be retrieved from storage',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.importedFilesBucketStubEmpty)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });
      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('importedFiles retrieval failed.')));
    });

ava('calculateProductChanges: cleanupCompletedFilenames logs error if ' +
        'deleteFiles had any errors',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketFailureStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });
      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'One or more errors occurred ' +
          'when deleting from the completed filenames bucket.'));
    });

ava('calculateProductChanges: logs error if config has invalid column',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      streamingInsertJobResult = [
        {
          id: 1,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.invalidConfigColumnKey,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });

ava('calculateProductChanges: logs error if config has invalid mapping value',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      streamingInsertJobResult = [
        {
          id: 1,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.invalidConfigString,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });

ava('calculateProductChanges: logs error if config has empty schema',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      streamingInsertJobResult = [
        {
          id: 1,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.emptyConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });

ava('calculateProductChanges: logs error and exits if streaming_items table ' +
        'is missing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      bqTableStub.withArgs('streaming_items').returns({
        exists: sinon.stub().resolves([false]),
        id: 'streaming_items',
      });
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'streaming_items table must exist before ' +
          'running the product calculation function.'));
      t.true(consoleError.calledWith(
          'One or more required tables do not exist. Aborting...'));
    });

ava('calculateProductChanges: logs error and exits if items_to_upsert table ' +
        'is missing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      bqTableStub.withArgs('items_to_upsert').returns({
        exists: sinon.stub().resolves([false]),
        id: 'items_to_upsert',
      });
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'items_to_upsert table must exist before ' +
          'running the product calculation function.'));
      t.true(consoleError.calledWith(
          'One or more required tables do not exist. Aborting...'));
    });

ava('calculateProductChanges: logs error and exits if items_to_delete table ' +
        'is missing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      bqTableStub.withArgs('items_to_delete').returns({
        exists: sinon.stub().resolves([false]),
        id: 'items_to_delete',
      });
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'items_to_delete table must exist before ' +
          'running the product calculation function.'));
      t.true(consoleError.calledWith(
          'One or more required tables do not exist. Aborting...'));
    });

ava('calculateProductChanges: logs error and exits if items table is missing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      bqTableStub.withArgs('items').returns({
        delete: sinon.stub().rejects(new Error('Items table does not exist.')),
        exists: sinon.stub().resolves([false]),
        id: 'items',
        setMetadata: sinon.stub().withArgs(sinon.match.object),
      });
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'items table must exist before ' +
          'running the product calculation function.')));
      t.true(consoleError.calledWith(sinon.match(
          'One or more required tables do not exist. Aborting...')));
    });

ava('calculateProductChanges: logs error and exits if ' +
        'items_to_prevent_expiring table is missing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      bqTableStub.withArgs('items_to_prevent_expiring').returns({
        exists: sinon.stub().resolves([false]),
        delete: sinon.stub().rejects(
            new Error('items_to_prevent_expiring table does not exist.')),
        id: 'items_to_prevent_expiring',
        setMetadata: sinon.stub().withArgs(sinon.match.object),
      });
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'items_to_prevent_expiring table must exist before ' +
          'running the product calculation function.'));
      t.true(consoleError.calledWith(
          'One or more required tables do not exist. Aborting...'));
    });

ava('calculateProductChanges: logs error if COPY_ITEM_BATCH_QUERY had errors',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      streamingInsertJobResult = [
        {
          id: 1,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(
          consoleError.calledWith(sinon.match('QueryJob Error for Job ID 1')));
    });

ava('calculateProductChanges: should clean up items table upon failed ' +
        'Task creation',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskFailedStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bqTableDeleteStub_Success.called);
    });

ava('calculateProductChanges: should clean up EOF.lock upon failed ' +
        'Task creation',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskFailedStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_CleanupSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(eofDeleteStub_Success.called);
    });

ava('calculateProductChanges: should log error upon failed ' +
        'Task creation.',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskFailedStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Error occurred when ' +
          'creating a Task to start GAE')));
    });

ava('calculateProductChanges: should log error upon failed ' +
        'EOF.lock cleanup.',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskFailedStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_UnlockedDeleteException, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(eofDeleteStub_Exception.called);
      t.true(consoleError.calledWith(sinon.match('EOF.lock deletion failed.')));
    });

ava('calculateProductChanges: Should catch exception if table delete fails',
    async (t) => {
      // Arrange.
      bqTableStub = bqTableStub.withArgs('items').returns({
        delete: sinon.stub().rejects(new Error('BigQuery API died')),
        exists: sinon.stub().resolves([true]),
        id: 'items',
        setMetadata: sinon.stub().withArgs(sinon.match.object),
      });
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskFailureStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith('Items table could not be deleted.'));
    });

ava('calculateProductChanges: should log error if countChanges returns NaN',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': NaN}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match('Skipping deletion processing')));
    });

ava('calculateProductChanges: should continue with Task Queue even if ' +
        'delete query failed',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': NaN}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(t.context.createTaskDefaultStub.called);
    });

ava('calculateProductChanges: logs error if COUNT_DELETES_QUERY ' +
        'returns non number count',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': 'garbage'}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Count deletes could not be extracted ' +
          'from the query result.')));
    });

ava('calculateProductChanges: logs error if COUNT_DELETES_QUERY ' +
        'returns undefined',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': undefined}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Count deletes could not be extracted ' +
          'from the query result.')));
    });

ava('calculateProductChanges: logs error if COUNT_DELETES_QUERY ' +
        'returns an empty string',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': ''}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Query rows result for delete was null or empty.')));
    });

ava('calculateProductChanges: should log error if ' +
        'CALCULATE_ITEMS_FOR_DELETION_QUERY results in errors',
    async (t) => {
      // Arrange.
      getMetadataStub_Delete = sinon.stub().resolves(deleteMetadata_Error);
      deletesInsertJobResult = [
        {
          id: 2,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Delete
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('createQueryJob Error for Job ID 2')));
    });

ava('calculateProductChanges: should log error if ' +
        'CALCULATE_ITEMS_FOR_UPDATE_QUERY results in errors',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      updatesInsertJobResult = [
        {
          id: 3,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(
          consoleError.calledWith(sinon.match('QueryJob Error for Job ID 3')));
    });

ava('calculateProductChanges: should log error if ' +
        'CALCULATE_ITEMS_FOR_INSERTION_QUERY results in errors',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      insertsInsertJobResult = [
        {
          id: 4,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(
          consoleError.calledWith(sinon.match('QueryJob Error for Job ID 4')));
    });

ava('calculateProductChanges: should log error if COUNT_UPSERTS_QUERY ' +
        'returns a non-number count',
    async (t) => {
      // Arrange.
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 'garbage'}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Count upserts could not be extracted ' +
          'from the query result.')));
    });

ava('calculateProductChanges: logs error if Count Query returns null count',
    async (t) => {
      // Arrange.
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': null}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Query rows result for upsert was null or empty.')));
    });

ava('calculateProductChanges: logs error if COUNT_EXPIRING_QUERY ' +
        'returns non number count',
    async (t) => {
      // Arrange.
      t.context.countExpiringJobResult = [
        {
          id: 7,
          getQueryResults: sinon.stub().resolves([[{'f0_': 'garbage'}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Count prevent_expirings could not be extracted ' +
          'from the query result.')));
    });

ava('calculateProductChanges: logs error if GET_EXPIRING_ITEMS_QUERY ' +
        'results in errors',
    async (t) => {
      // Arrange.
      getMetadataStub_Default = sinon.stub().resolves(getMetadata_Error);
      expiringInsertJobResult = [
        {
          id: 5,
          promise: jobPromiseStub_Success,
          getMetadata: getMetadataStub_Default
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(
          consoleError.calledWith(sinon.match('QueryJob Error for Job ID 5')));
    });

ava('calculateProductChanges: should log error on invalid filename',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });
      t.context.data.name = 'FOE';

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('was not an empty EOF! Exiting Function...')));
    });

ava('calculateProductChanges: should log error on invalid filesize',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });
      t.context.data.size = 1;

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('was not an empty EOF! Exiting Function...')));
    });

ava('calculateProductChanges: logs error if deletes threshold was crossed',
    async (t) => {
      // Arrange.
      process.env.DELETES_THRESHOLD = 90000;
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': 99999}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Deletes count 99999 crossed deletes threshold ' +
          'of 90000 items. Skipping delete feed processing...')));
    });

ava('calculateProductChanges: logs error if deletes count query returns junk',
    async (t) => {
      // Arrange.
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([null]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Query rows result for delete was null or empty.')));
    });

ava('calculateProductChanges: logs error if upserts threshold was crossed',
    async (t) => {
      // Arrange.
      process.env.UPSERTS_THRESHOLD = 999999;
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 1000000}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Upsert count 1000000 crossed upserts threshold ' +
          'of 999999 items. Skipping upsert feed processing...')));
    });

ava('calculateProductChanges: clean streaming_items table if upserts ' +
        'threshold was crossed',
    async (t) => {
      // Arrange.
      process.env.UPSERTS_THRESHOLD = 999999;
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 1000000}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor = sinonHelpers.getStubConstructor(Tasks);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath = t.context.taskQueuePathDefaultStub;
        instance.createTask = t.context.createTaskDefaultStub;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {
          Storage: setupStorageInstanceStub(
              t.context.attemptedFilesBucketSuccessStub,
              eofLockStub_FileUnlockedSuccess, updateBucketStub_Success,
              t.context.completedFilesBucketSuccessStub)
        },
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith(
          sinon.match('DML query with job ID 6 completed.')));
    });

/**
 * Sets stub behavior on a sinon-helpers instance with
 * the specified test context objects.
 *
 * @param {!StubConstructor} bigQueryInstance The BigQuery sinon-helpers
 *   constructor instance.
 * @param {!Object} t The test framework instance with context object.
 */
function setupBigQueryInstanceStubs(bigQueryInstance, t) {
  bigQueryInstance.dataset.returns({table: bqTableStub});
  bigQueryInstance.createQueryJob.withArgs(t.context.streamingQueryJobObject)
      .resolves(streamingInsertJobResult)
      .withArgs(t.context.deletesQueryJobObject)
      .resolves(deletesInsertJobResult)
      .withArgs(t.context.updatesQueryJobObject)
      .resolves(updatesInsertJobResult)
      .withArgs(t.context.insertsQueryJobObject)
      .resolves(insertsInsertJobResult)
      .withArgs(t.context.expiringQueryJobObject)
      .resolves(expiringInsertJobResult)
      .withArgs(t.context.truncateStreamingQueryJobObject)
      .resolves(truncateStreamingJobResult)
      .withArgs(t.context.countDeletesQueryJobObject)
      .resolves(t.context.countDeletesJobResult)
      .withArgs(t.context.countUpsertsQueryJobObject)
      .resolves(t.context.countUpsertsJobResult)
      .withArgs(t.context.countExpiringQueryJobObject)
      .resolves(t.context.countExpiringJobResult);
}
