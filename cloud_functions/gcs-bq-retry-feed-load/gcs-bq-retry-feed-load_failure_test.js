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
 * @fileoverview Unit tests for failure cases of the
 * reprocessFeedfile Cloud Function.
 */
const ava = require('ava');
const {BigQuery} = require('@google-cloud/bigquery');
const process = require('process');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const sinonHelpers = require('sinon-helpers');
const {Storage} = require('@google-cloud/storage');

const jobId = 12345;

let consoleLog = {};
let consoleError = {};
let consoleWarn = {};
let completedFilesSaveFailureStub;
let loadException;
let readStreamStub;
let retriggerSaveFailureStub;
let retriggerSaveSuccessStub;

ava.before((t) => {
  consoleLog = sinon.stub(console, 'log');
  consoleError = sinon.stub(console, 'error');
  consoleWarn = sinon.stub(console, 'warn');
  readStreamStub = sinon.stub().returns({
    on: sinon.stub().returns({
      on: sinon.stub().callsArgWith(1, new Buffer([0x00, 0x01, 0x02])).returns({
        on: sinon.stub().callsArg(1),
      }),
    }),
  });
  completedFilesSaveFailureStub =
      sinon.stub().returns(Promise.reject(new Error('Completed Save Failed')));
  loadException = new Error('Something Went Wrong');
  retriggerSaveSuccessStub = sinon.stub().returns(Promise.resolve());
  retriggerSaveFailureStub =
      sinon.stub().returns(Promise.reject(new Error('Retrigger Save Failed.')));
  process.env.FEED_BUCKET = 'testfeedbucket';
  process.env.COMPLETED_FILES_BUCKET = 'testcompletedbucket';
  process.env.UPDATE_BUCKET = 'updatebucket';
  process.env.BQ_DATASET = 'testbqdataset';
  process.env.RETRIGGER_BUCKET = 'retriggerbucket';
});

ava.beforeEach((t) => {
  consoleLog.resetHistory();
  consoleError.resetHistory();
  consoleWarn.resetHistory();
  t.context = {
    bqLoadErrorResult: [
      {
        status: {
          state: 'DONE',
          errors: [loadException],
        },
        id: jobId,
      },
    ],
    bqLoadSuccessResult: [
      {
        status: {
          state: 'DONE',
        },
        id: jobId,
      },
    ],
    context: {},
    emptyConfig: {
      'mapping': [],
    },
    invalidConfigString: {
      'mapping': 'invalidValue',
    },
    invalidConfigColumnKey: {
      'mapping': [
        {
          'csvHeader': 'google_merchant_id',
          'bqColumn': 'google_merchant_id',
          'invalidColumn': 'STRING',
        },
      ],
    },
    fileName: 'REPROCESS_TRIGGER_FILE',
    fileReference: {
      name: 'REPROCESS_TRIGGER_FILE',
      bucket: 'testfeedbucket',
      size: 100,
    },
    jobId: jobId,
    storageCompletedSaveFailureStub:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(t.context.fileReference.bucket)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  name: t.context.fileName,
                  save: sinon.stub().resolves({}),
                })
              })
              .withArgs(process.env.COMPLETED_FILES_BUCKET)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  save: completedFilesSaveFailureStub,
                }),
              })
              .withArgs(process.env.UPDATE_BUCKET)
              .returns({
                file: sinon.stub().withArgs('EOF.retry').returns({
                  save: sinon.stub()
                            .withArgs(sinon.match.any)
                            .resolves('success'),
                }),
              })
              .withArgs(process.env.RETRIGGER_BUCKET)
              .returns({
                file: sinon.stub().withArgs('REPROCESS_TRIGGER_FILE').returns({
                  createReadStream: readStreamStub,
                  save: retriggerSaveSuccessStub,
                }),
              });
        }),
    storageFileListUploadFailureStub:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(t.context.fileReference.bucket)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  name: t.context.fileName,
                  save: sinon.stub().resolves({}),
                })
              })
              .withArgs(process.env.COMPLETED_FILES_BUCKET)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  save: sinon.stub().resolves({}),
                }),
              })
              .withArgs(process.env.UPDATE_BUCKET)
              .returns({
                file: sinon.stub().withArgs('EOF.retry').returns({
                  save: sinon.stub()
                            .withArgs(sinon.match.any)
                            .resolves('success'),
                }),
              })
              .withArgs(process.env.RETRIGGER_BUCKET)
              .returns({
                file: sinon.stub().withArgs('REPROCESS_TRIGGER_FILE').returns({
                  createReadStream: readStreamStub,
                  save: retriggerSaveFailureStub,
                }),
              });
        }),
    storageDefaultStub:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(t.context.fileReference.bucket)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  name: t.context.fileName,
                  save: sinon.stub().resolves({}),
                }),
              })
              .withArgs(process.env.COMPLETED_FILES_BUCKET)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  save: sinon.stub().resolves({}),
                }),
              })
              .withArgs(process.env.UPDATE_BUCKET)
              .returns({
                file: sinon.stub().withArgs('EOF.lock').returns({
                  exists: sinon.stub().resolves([false]),
                }),
              })
              .withArgs(process.env.RETRIGGER_BUCKET)
              .returns({
                file: sinon.stub().withArgs('REPROCESS_TRIGGER_FILE').returns({
                  createReadStream: readStreamStub,
                  save: sinon.stub().resolves({}),
                }),
              });
        }),
    validConfig: {
      'mapping': [
        {
          'csvHeader': 'google_merchant_id',
          'bqColumn': 'google_merchant_id',
          'columnType': 'STRING',
        },
        {'csvHeader': 'id', 'bqColumn': 'id', 'columnType': 'STRING'},
      ],
    },
  };
});

ava('reprocessFeedfile: should handle error of failed file list re-load ' +
        'into GCS',
    async (t) => {
      // Arrange.
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadSuccessResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const reprocessFeedfile = proxyquire('./index', {
        '@google-cloud/storage':
            {Storage: t.context.storageFileListUploadFailureStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await reprocessFeedfile.reprocessFeedfile(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(retriggerSaveFailureStub.called);
    });


ava('reprocessFeedfile: should handle error of failed load call', async (t) => {
  // Arrange.
  const bqLoadFailureStub = sinon.stub().throws(new Error('BQ Load died.'));
  const bqTableStub = sinon.stub().returns({
    table: 'items',
    load: bqLoadFailureStub,
  });
  const bigQueryStub =
      sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
        instance.dataset.returns({table: bqTableStub});
      });
  const reprocessFeedfile = proxyquire('./index', {
    '@google-cloud/storage':
        {Storage: t.context.storageCompletedSaveFailureStub},
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
    './config.json': t.context.validConfig,
  });

  // Act.
  await reprocessFeedfile.reprocessFeedfile(
      t.context.fileReference, t.context.context);

  // Assert.
  t.true(bqLoadFailureStub.called);
});

ava('reprocessFeedfile: should log error on failed load result', async (t) => {
  // Arrange.
  const bqLoadFailureStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
  const bqTableStub = sinon.stub().returns({
    table: 'items',
    load: bqLoadFailureStub,
  });
  const bigQueryStub =
      sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
        instance.dataset.returns({table: bqTableStub});
      });
  const reprocessFeedfile = proxyquire('./index', {
    '@google-cloud/storage':
        {Storage: t.context.storageCompletedSaveFailureStub},
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
    './config.json': t.context.validConfig,
  });

  // Act.
  await reprocessFeedfile.reprocessFeedfile(
      t.context.fileReference, t.context.context);

  // Assert.
  t.true(consoleError.calledWith(
      sinon.match('Load into BigQuery returned, but with errors')));
});

ava('reprocessFeedfile: should stop processing on failed ' +
        'load job',
    async (t) => {
      // Arrange.
      const bqLoadFailureStub =
          sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadFailureStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const reprocessFeedfile = proxyquire('./index', {
        '@google-cloud/storage':
            {Storage: t.context.storageCompletedSaveFailureStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await reprocessFeedfile.reprocessFeedfile(
          t.context.fileReference, t.context.context);

      // Assert.
      t.false(retriggerSaveSuccessStub.called);
    });

ava('reprocessFeedfile: should stop processing on failed save of the ' +
        'reprocessed filename to GCS',
    async (t) => {
      // Arrange.
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadSuccessResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const reprocessFeedfile = proxyquire('./index', {
        '@google-cloud/storage':
            {Storage: t.context.storageCompletedSaveFailureStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await reprocessFeedfile.reprocessFeedfile(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(completedFilesSaveFailureStub.called);
      t.false(retriggerSaveSuccessStub.called);
    });
