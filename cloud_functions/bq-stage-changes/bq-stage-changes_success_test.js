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
 * @fileoverview Unit tests for success cases of the calculateProductChanges
 * Cloud Function.
 */
const ava = require('ava');
const {BigQuery} = require('@google-cloud/bigquery');
const {CloudTasksClient} = require('@google-cloud/tasks');
const process = require('process');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const sinonHelpers = require('sinon-helpers');
const {Storage} = require('@google-cloud/storage');

const DELETE_COUNT = 1;
const EXPIRING_COUNT = 2;
const UPSERT_COUNT = 3;

let bqTableDeleteStub_Success;
let bqTableStub_Default;
let clock;
let consoleError;
let consoleLog;
let deletesInsertJobResult;
let eofLockFileStub_Exists;
let eofLockFileStub_Locked;
let eofLockFileStub_Unlocked;
let eofStub_Success;
let fileMoveStub_Success;
let getExpiringItemsJobResult;
let getFeedFiles_Failure;
let getFeedFiles_Success;
let getMetadataStub_Success;
let insertsInsertJobResult;
let jobPromiseStub_Success;
let queries;
let streamingInsertJobResult;
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
  queries = require('./queries');
  const getMetadata_Success = [{
    status: {
      errors: null,
    },
  }];
  getMetadataStub_Success = sinon.stub().resolves(getMetadata_Success);
  jobPromiseStub_Success = sinon.stub().resolves();
  bqTableStub_Default = sinon.stub();
  bqTableDeleteStub_Success = sinon.stub().resolves(['response']);
  bqTableStub_Default.withArgs('streaming_items')
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
      });
  clock = sinon.useFakeTimers({
    now: new Date('2019-01-01T12:00:00'),
    shouldAdvanceTime: false,
  });
  fileMoveStub_Success = sinon.stub().resolves();
  eofStub_Success = sinon.stub();
  eofStub_Success.withArgs(sinon.match('EOF')).returns({
    move: sinon.stub().resolves({}),
    name: 'EOF',
  });
  eofLockFileStub_Exists = sinon.stub().resolves([true]);
  eofLockFileStub_Locked = sinon.stub();
  eofLockFileStub_Locked.withArgs(sinon.match('EOF.lock')).returns({
    delete: sinon.stub().resolves({}),
    exists: eofLockFileStub_Exists,
  });
  eofLockFileStub_Unlocked = sinon.stub();
  eofLockFileStub_Unlocked.withArgs(sinon.match('EOF.lock')).returns({
    delete: sinon.stub().resolves({}),
    exists: sinon.stub().resolves([false]),
  });
  consoleLog = sinon.stub(console, 'log');
  consoleError = sinon.stub(console, 'error');
});

ava.beforeEach((t) => {
  consoleLog.resetHistory();
  consoleError.resetHistory();
  bqTableStub_Default.resetHistory();
  fileMoveStub_Success.resetHistory();
  streamingInsertJobResult = [
    {
      id: 1,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Success,
    },
  ];
  deletesInsertJobResult = [
    {
      id: 2,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Success,
    },
  ];
  updatesInsertJobResult = [
    {
      id: 3,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Success,
    },
  ];
  insertsInsertJobResult = [
    {
      id: 4,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Success,
    },
  ];
  getExpiringItemsJobResult = [
    {
      id: 5,
      promise: jobPromiseStub_Success,
      getMetadata: getMetadataStub_Success,
    },
  ];
  getFeedFiles_Success = [[
    {
      name: 'file1.txt',
      move: fileMoveStub_Success,
    },
    {
      name: 'file2.txt',
      move: fileMoveStub_Success,
    },
    {
      name: 'file3.txt',
      move: fileMoveStub_Success,
    },
  ]];
  getFeedFiles_Failure = [[
    {
      name: 'file1.txt',
    },
    {
      name: 'file2.txt',
    },
  ]];
  t.context = {
    attemptedFilesBucketStub_Success: {
      getFiles: sinon.stub()
                    .onCall(0)
                    .resolves(getFeedFiles_Success)
                    .onCall(1)
                    .resolves([[{move: sinon.stub().resolves()}]]),
    },
    context: {},
    createTaskStub_Success:
        sinon.stub().withArgs(sinon.match.object).resolves([{name: 'success'}]),
    completedFilesBucketStub_Failed: {
      getFiles: sinon.stub().resolves(getFeedFiles_Failure),
      deleteFiles: sinon.stub().yields(null),
    },
    completedFilesBucketStub_Success: {
      getFiles: sinon.stub().resolves(getFeedFiles_Success),
      deleteFiles: sinon.stub().yields(null),
    },
    countDeletesJobResult: [
      {
        id: 5,
        getQueryResults: sinon.stub().resolves([[{'f0_': DELETE_COUNT}]]),
      },
    ],
    countDeletesQueryJobObject: {
      query: queries.COUNT_DELETES_QUERY,
      location: 'US',
    },
    countExpiringJobResult: [
      {
        id: 7,
        getQueryResults: sinon.stub().resolves([[{'f0_': EXPIRING_COUNT}]]),
      },
    ],
    countExpiringQueryJobObject: {
      query: queries.COUNT_EXPIRING_QUERY,
      location: 'US',
    },
    countUpsertsJobResult: [
      {
        id: 6,
        getQueryResults: sinon.stub().resolves([[{'f0_': UPSERT_COUNT}]]),
      },
    ],
    countUpsertsQueryJobObject: {
      query: queries.COUNT_UPSERTS_QUERY,
      location: 'US',
    },
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
    expiringItemsQueryJobObject: {
      destination: sinon.match.any,
      query: queries.GET_EXPIRING_ITEMS_QUERY,
      writeDisposition: 'WRITE_TRUNCATE',
    },
    expirationMetadataArg: {'expirationTime': sinon.match.string},
    failedFilesBucketStub: {
      file: sinon.stub().withArgs('REPROCESS_TRIGGER_FILE').returns({
        save: sinon.stub().withArgs(sinon.match.string).resolves(null),
      }),
    },
    insertsQueryJobObject: {
      destination: sinon.match.any,
      query: queries.CALCULATE_ITEMS_FOR_INSERTION_QUERY,
      writeDisposition: 'WRITE_APPEND',
    },
    storageStub_Success:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(process.env.FEED_BUCKET)
              .returns(t.context.attemptedFilesBucketStub_Success)
              .withArgs(process.env.LOCK_BUCKET)
              .returns({
                file: eofLockFileStub_Unlocked,
              })
              .withArgs(process.env.UPDATE_BUCKET)
              .returns({
                file: eofStub_Success,
              });
          instance.bucket.withArgs(process.env.COMPLETED_FILES_BUCKET)
              .returns(t.context.completedFilesBucketStub_Success);
        }),
    streamingQueryJobObject: {
      destination: sinon.match.any,
      query: sinon.match(
          'TO_BASE64(SHA1(CONCAT(IFNULL(CAST(Items.item_id AS STRING), \'NULL\'), ' +
          'IFNULL(CAST(Items.title AS STRING), \'NULL\'))))'),
      writeDisposition: 'WRITE_APPEND',
    },
    streamingQueryJobWithMcIdObject: {
      destination: sinon.match.any,
      query: sinon.match('google_merchant_id,'),
      writeDisposition: 'WRITE_APPEND',
    },
    validConfig: {
      'mapping': [
        {'csvHeader': 'item_id', 'bqColumn': 'item_id', 'columnType': 'STRING'},
        {'csvHeader': 'title', 'bqColumn': 'title', 'columnType': 'STRING'},
      ],
    },
    validConfigWithMcId: {
      'mapping': [
        {
          'csvHeader': 'google_merchant_id',
          'bqColumn': 'google_merchant_id',
          'columnType': 'INTEGER',
        },
        {'csvHeader': 'title', 'bqColumn': 'title', 'columnType': 'STRING'},
      ],
    },
    updatesQueryJobObject: {
      destination: sinon.match.any,
      query: sinon.match(
          'TO_BASE64(SHA1(CONCAT(IFNULL(CAST(Items.item_id AS ' +
          'STRING), \'NULL\'), IFNULL(CAST(Items.title AS STRING), \'NULL\'))))'),
      writeDisposition: 'WRITE_TRUNCATE',
    },
    updatesQueryJobWithMcIdObject: {
      destination: sinon.match.any,
      query: sinon.match(
          'TO_BASE64(SHA1(CONCAT(IFNULL(CAST(Items.google_merchant_id AS ' +
          'STRING), \'NULL\'), IFNULL(CAST(Items.title AS STRING), \'NULL\'))))'),
      writeDisposition: 'WRITE_TRUNCATE',
    },
  };
});

ava.after((t) => {
  clock.restore();
});

ava('calculateProductChanges: should check existence of lock file',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const storageConstructor = sinonHelpers.getStubConstructor(Storage);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const storageLockedStub = storageConstructor.withInit((instance) => {
        instance.bucket.withArgs(process.env.FEED_BUCKET)
            .returns(t.context.feedFilesBucketStub)
            .withArgs(process.env.LOCK_BUCKET)
            .returns({
              file: eofLockFileStub_Locked,
            });
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: storageLockedStub},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(eofLockFileStub_Exists.called);
    });

ava('calculateProductChanges: should continue if unlocked', async (t) => {
  // Arrange.
  const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
  const tasksConstructor = sinonHelpers.getStubConstructor(CloudTasksClient);
  const bigQueryStub = bigQueryConstructor.withInit((instance) => {
    setupBigQueryInstanceStubs(instance, t);
  });
  const tasksStub = tasksConstructor.withInit((instance) => {
    instance.queuePath =
        sinon.stub()
            .withArgs(
                sinon.match.string, sinon.match.string, sinon.match.string)
            .returns({});
    instance.createTask = t.context.createTaskStub_Success;
  });
  const calculateProductChanges = proxyquire('./index', {
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
    '@google-cloud/storage': {Storage: t.context.storageStub_Success},
    '@google-cloud/tasks': {CloudTasksClient: tasksStub},
    './config.json': t.context.validConfig,
  });

  // Act.
  await calculateProductChanges.calculateProductChanges(
      t.context.data, t.context.context);

  // Assert.
  t.true(consoleLog.calledWith(sinon.match(
      'Empty EOF file detected. ' +
      'Starting diff calculation in BigQuery...')));
});

ava('calculateProductChanges: should exit if locked', async (t) => {
  // Arrange.
  const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
  const tasksConstructor = sinonHelpers.getStubConstructor(CloudTasksClient);
  const storageConstructor = sinonHelpers.getStubConstructor(Storage);
  const bigQueryStub = bigQueryConstructor.withInit((instance) => {
    setupBigQueryInstanceStubs(instance, t);
  });
  const tasksStub = tasksConstructor.withInit((instance) => {
    instance.queuePath =
        sinon.stub()
            .withArgs(
                sinon.match.string, sinon.match.string, sinon.match.string)
            .returns({});
    instance.createTask = t.context.createTaskStub_Success;
  });
  const storageLockedStub = storageConstructor.withInit((instance) => {
    instance.bucket.withArgs(process.env.FEED_BUCKET)
        .returns(t.context.feedFilesBucketStub)
        .withArgs(process.env.LOCK_BUCKET)
        .returns({
          file: eofLockFileStub_Locked,
        });
  });
  const calculateProductChanges = proxyquire('./index', {
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
    '@google-cloud/storage': {Storage: storageLockedStub},
    '@google-cloud/tasks': {CloudTasksClient: tasksStub},
    './config.json': t.context.validConfig,
  });

  // Act.
  await calculateProductChanges.calculateProductChanges(
      t.context.data, t.context.context);

  // Assert.
  t.true(consoleError.calledWith(sinon.match('An EOF.lock file was found')));
  t.false(consoleLog.calledWith(sinon.match(
      'Empty EOF file detected. ' +
      'Starting diff calculation in BigQuery...')));
});

ava('calculateProductChanges: should check existence of tables', async (t) => {
  // Arrange.
  const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
  const tasksConstructor = sinonHelpers.getStubConstructor(CloudTasksClient);
  const bigQueryStub = bigQueryConstructor.withInit((instance) => {
    setupBigQueryInstanceStubs(instance, t);
  });
  const tasksStub = tasksConstructor.withInit((instance) => {
    instance.queuePath =
        sinon.stub()
            .withArgs(
                sinon.match.string, sinon.match.string, sinon.match.string)
            .returns({});
    instance.createTask = t.context.createTaskStub_Success;
  });
  const calculateProductChanges = proxyquire('./index', {
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
    '@google-cloud/storage': {Storage: t.context.storageStub_Success},
    '@google-cloud/tasks': {CloudTasksClient: tasksStub},
    './config.json': t.context.validConfig,
  });

  // Act.
  await calculateProductChanges.calculateProductChanges(
      t.context.data, t.context.context);

  // Assert.
  t.false(consoleError.calledWith(
      'streaming_items table must exist before ' +
      'running the product calculation function. Aborting...'));
  t.false(consoleError.calledWith(
      'items_to_upsert table must exist before ' +
      'running the product calculation function. Aborting...'));
  t.false(consoleError.calledWith(
      'items_to_delete table must exist before ' +
      'running the product calculation function. Aborting...'));
  t.false(consoleError.calledWith(
      'items_to_prevent_expiring table must exist before ' +
      'running the product calculation function. Aborting...'));
});

ava('calculateProductChanges: Should replace queries with ' +
        'schema in the supplied config',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.streamingQueryJobObject));
    });

ava('calculateProductChanges: Should replace streaming_items query with ' +
        'google_merchant_id if included in config',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfigWithMcId,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.streamingQueryJobWithMcIdObject));
    });

ava('calculateProductChanges: should move files correctly while archiving.',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith('Finished archiving.'));
    });

ava('calculateProductChanges: should set items table expiration date',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const setMetadataStub =
          sinon.stub().withArgs(t.context.expirationMetadataArg);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
        instance.dataset.returns({
          table: bqTableStub_Default.withArgs('items').returns({
            delete: bqTableDeleteStub_Success,
            exists: sinon.stub().resolves([true]),
            id: 'items',
            setMetadata: setMetadataStub,
          }),
        });
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      const expected = new Date('2019-01-02T00:00:00').getTime();
      t.true(setMetadataStub.calledWith(t.context.expirationMetadataArg));
      t.true(parseInt(setMetadataStub.args[0][0].expirationTime) === expected);
    });

ava('calculateProductChanges: should succeed in inserting streaming items ' +
        'using createQueryJob',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.streamingQueryJobObject));
    });

ava('calculateProductChanges: should call the bigQueryJob promise ' +
        'after createQueryJob',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(jobPromiseStub_Success.called);
    });

ava('calculateProductChanges: should call getMetadata after the job promise',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(getMetadataStub_Success.called);
    });

ava('calculateProductChanges: should log success after metadata check',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith(sinon.match(
          'createQueryJob into streaming_items table job 1 completed.')));
    });

ava('calculateProductChanges: should call CALCULATE_ITEMS_FOR_DELETION_QUERY ' +
        'after the COPY_ITEM_BATCH_QUERY is called',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.streamingQueryJobObject));
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.deletesQueryJobObject));
    });

ava('calculateProductChanges: should call COUNT_DELETES_QUERY after the ' +
        'delete calculation job',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith({
        query: queries.COUNT_DELETES_QUERY,
        location: 'US',
      }));
      t.true(consoleLog.calledWith(
          `Number of rows to delete in this run: ${DELETE_COUNT}`));
    });

ava('calculateProductChanges: delete count === 0 does not cause validation ' +
        'failure',
    async (t) => {
      // Arrange.
      process.env.UPSERTS_THRESHOLD = 1000000;
      t.context.countDeletesJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 0}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith(
          sinon.match(`Number of rows to delete in this run: 0`)));
    });

ava('calculateProductChanges: CALCULATE_ITEMS_FOR_UPDATE_QUERY query job ' +
        'should be called and succeed',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.updatesQueryJobObject));
      t.true(consoleLog.calledWith(sinon.match(
          'createQueryJob into items_to_upsert table job 3 completed.')));
    });

ava('calculateProductChanges: CALCULATE_ITEMS_FOR_INSERTION_QUERY should ' +
        'be called and succeed',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.insertsQueryJobObject));
      t.true(consoleLog.calledWith(sinon.match(
        'createQueryJob into items_to_upsert table job 4 completed.')));
    });

ava('calculateProductChanges: should call the COUNT_UPSERTS_QUERY after the ' +
        'update/insert calculation jobs',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.updatesQueryJobObject));
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.insertsQueryJobObject));
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith({
        query: queries.COUNT_UPSERTS_QUERY,
        location: 'US',
      }));
      t.true(consoleLog.calledWith(
          `Number of rows to upsert in this run: ${UPSERT_COUNT}`));
    });

ava('calculateProductChanges: Should set default delete threshold if it is' +
        ' blank and pass if count is under the threshold',
    async (t) => {
      // Arrange.
      process.env.DELETES_THRESHOLD = '';
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': 99999}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(t.context.createTaskStub_Success.called);
    });

ava('calculateProductChanges: Set default delete threshold if it is -1 ' +
        'and pass if count is under the threshold',
    async (t) => {
      // Arrange.
      process.env.DELETES_THRESHOLD = -1;
      t.context.countDeletesJobResult = [
        {
          id: 5,
          getQueryResults: sinon.stub().resolves([[{'f0_': 99999}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(t.context.createTaskStub_Success.called);
    });

ava('calculateProductChanges: Should skip upsert threshold check if it is' +
        ' blank and pass if count is under the threshold',
    async (t) => {
      // Arrange.
      process.env.UPSERTS_THRESHOLD = '';
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 999999999}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(t.context.createTaskStub_Success.called);
    });

ava('calculateProductChanges: Set default upsert threshold if it is too high ' +
        'and pass if count is under the threshold',
    async (t) => {
      // Arrange.
      process.env.UPSERTS_THRESHOLD = 1000001;
      t.context.countUpsertsJobResult = [
        {
          id: 6,
          getQueryResults: sinon.stub().resolves([[{'f0_': 1000001}]]),
        },
      ];
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(t.context.createTaskStub_Success.called);
    });

ava('calculateProductChanges: GET_EXPIRING_ITEMS_QUERY should ' +
        'be called and succeed',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.expiringItemsQueryJobObject));
      t.true(consoleLog.calledWith(sinon.match(
        'createQueryJob into items_to_prevent_expiring table job 5 completed.')));
    });

ava('calculateProductChanges: should call the COUNT_EXPIRING_QUERY after the ' +
        'expiring calculation jobs',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.updatesQueryJobObject));
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith(
          t.context.insertsQueryJobObject));
      t.true(bigQueryConstructor.getInstance().createQueryJob.calledWith({
        query: queries.COUNT_UPSERTS_QUERY,
        location: 'US',
      }));
      t.true(consoleLog.calledWith(
          `Number of rows to prevent_expiring in this run: ${EXPIRING_COUNT}`));
    });

ava('ensureAllFilesWereImported: should detect no difference in storage ' +
        'filesets and continue with function processing',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith(sinon.match('All the feeds are loaded')));
    });

ava('ensureAllFilesWereImported: should detect difference in storage ' +
        'filesets and triggers the retry cloud function',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const storageConstructor = sinonHelpers.getStubConstructor(Storage);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const storageFilesMismatchStub =
          storageConstructor.withInit((instance) => {
            instance.bucket.withArgs(process.env.FEED_BUCKET)
                .returns(t.context.attemptedFilesBucketStub_Success)
                .withArgs(process.env.LOCK_BUCKET)
                .returns({
                  file: eofLockFileStub_Unlocked,
                })
                .withArgs(process.env.UPDATE_BUCKET)
                .returns({
                  file: eofStub_Success,
                });
            instance.bucket.withArgs(process.env.COMPLETED_FILES_BUCKET)
                .returns(t.context.completedFilesBucketStub_Failed);
            instance.bucket.withArgs(process.env.RETRIGGER_BUCKET)
                .returns(t.context.failedFilesBucketStub);
          });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: storageFilesMismatchStub},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.true(consoleLog.calledWith(
          '1 files are missing. Inserted Retry Trigger.'));
    });

ava('calculateProductChanges: should skip item table deletion on completion',
    async (t) => {
      // Arrange.
      const bigQueryConstructor = sinonHelpers.getStubConstructor(BigQuery);
      const tasksConstructor =
          sinonHelpers.getStubConstructor(CloudTasksClient);
      const bigQueryStub = bigQueryConstructor.withInit((instance) => {
        setupBigQueryInstanceStubs(instance, t);
      });
      const tasksStub = tasksConstructor.withInit((instance) => {
        instance.queuePath =
            sinon.stub()
                .withArgs(
                    sinon.match.string, sinon.match.string, sinon.match.string)
                .returns({});
        instance.createTask = t.context.createTaskStub_Success;
      });
      const calculateProductChanges = proxyquire('./index', {
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        '@google-cloud/storage': {Storage: t.context.storageStub_Success},
        '@google-cloud/tasks': {CloudTasksClient: tasksStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await calculateProductChanges.calculateProductChanges(
          t.context.data, t.context.context);

      // Assert.
      t.false(bqTableDeleteStub_Success.called);
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
  bigQueryInstance.dataset.returns({table: bqTableStub_Default});
  bigQueryInstance.createQueryJob.withArgs(t.context.streamingQueryJobObject)
      .resolves(streamingInsertJobResult)
      .withArgs(t.context.streamingQueryJobWithMcIdObject)
      .resolves(streamingInsertJobResult)
      .withArgs(t.context.deletesQueryJobObject)
      .resolves(deletesInsertJobResult)
      .withArgs(t.context.updatesQueryJobObject)
      .resolves(updatesInsertJobResult)
      .withArgs(t.context.updatesQueryJobWithMcIdObject)
      .resolves(updatesInsertJobResult)
      .withArgs(t.context.insertsQueryJobObject)
      .resolves(insertsInsertJobResult)
      .withArgs(t.context.expiringItemsQueryJobObject)
      .resolves(getExpiringItemsJobResult)
      .withArgs(t.context.countDeletesQueryJobObject)
      .resolves(t.context.countDeletesJobResult)
      .withArgs(t.context.countUpsertsQueryJobObject)
      .resolves(t.context.countUpsertsJobResult)
      .withArgs(t.context.countExpiringQueryJobObject)
      .resolves(t.context.countExpiringJobResult);
  bigQueryInstance.createJob.callsArgWith(
      1, null, {getQueryResults: sinon.stub().yields(null, null)});
}
