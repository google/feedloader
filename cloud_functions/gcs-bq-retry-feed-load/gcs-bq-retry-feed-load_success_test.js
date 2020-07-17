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
 * @fileoverview Unit tests for success cases of the
 * reprocessFeedfile Cloud Function.
 */
const ava = require('ava');
const {BigQuery} = require('@google-cloud/bigquery');
const process = require('process');
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const sinonHelpers = require('sinon-helpers');
const {Storage} = require('@google-cloud/storage');

let consoleLog = {};
let consoleError = {};
let consoleWarn = {};
let readStreamStub;
let completedFilesSaveStub;
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
  completedFilesSaveStub = sinon.stub().withArgs('').resolves('success');
  retriggerSaveSuccessStub =
      sinon.stub().withArgs(sinon.match.any).resolves('success');
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
    bqLoadResult: [
      {
        status: {
          state: 'DONE',
        },
        id: 12345,
      },
    ],
    context: {},
    fileName: 'REPROCESS_TRIGGER_FILE',
    fileRange: '0-10',
    fileReference: {
      name: 'REPROCESS_TRIGGER_FILE',
      bucket: 'testfeedbucket',
    },
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
                file: sinon.stub().withArgs(sinon.match.any).returns({
                  save: completedFilesSaveStub,
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

ava('reprocessFeedfile: should fetch BigQuery Dataset and Table', async (t) => {
  // Arrange.
  const bqLoadStub = sinon.stub().resolves(t.context.bqLoadResult);
  const bqTableStub = sinon.stub().returns({
    table: 'items',
    load: bqLoadStub,
  });
  const bigQueryStub =
      sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
        instance.dataset.returns({table: bqTableStub});
      });
  const reprocessFeedfile = proxyquire('./index', {
    '@google-cloud/storage': {Storage: t.context.storageDefaultStub},
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
  });

  // Act.
  await reprocessFeedfile.reprocessFeedfile(
      t.context.fileReference, t.context.context);

  // Assert.
  t.true(bigQueryStub.getInstance().dataset.called);
  t.true(bqTableStub.called);
});

ava('reprocessFeedfile: should call BQ load function and log a completed load',
    async (t) => {
      // Arrange.
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const reprocessFeedfile = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageDefaultStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
      });

      // Act.
      await reprocessFeedfile.reprocessFeedfile(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(bqLoadStub.called);
      t.true(consoleLog.calledWith(
          sinon.match('was loaded into BigQuery successfully')));
    });

ava('reprocessFeedfile: should save filename to storage ' +
        'after loading data into BigQuery successfully',
    async (t) => {
      // Arrange.
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const reprocessFeedfile = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageDefaultStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
      });

      // Act.
      await reprocessFeedfile.reprocessFeedfile(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(completedFilesSaveStub.called);
    });

ava('reprocessFeedfile: should re-load file list into GCS', async (t) => {
  // Arrange.
  const bqLoadStub = sinon.stub().resolves(t.context.bqLoadResult);
  const bqTableStub = sinon.stub().returns({
    table: 'items',
    load: bqLoadStub,
  });
  const bigQueryStub =
      sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
        instance.dataset.returns({table: bqTableStub});
      });
  const reprocessFeedfile = proxyquire('./index', {
    '@google-cloud/storage': {Storage: t.context.storageDefaultStub},
    '@google-cloud/bigquery': {BigQuery: bigQueryStub},
  });

  // Act.
  await reprocessFeedfile.reprocessFeedfile(
      t.context.fileReference, t.context.context);

  // Assert.
  t.true(retriggerSaveSuccessStub.called);
});
