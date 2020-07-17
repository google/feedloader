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
 * importStorageFileIntoBigQuery Cloud Function.
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

ava.before((t) => {
  consoleLog = sinon.stub(console, 'log');
  consoleError = sinon.stub(console, 'error');
  consoleWarn = sinon.stub(console, 'warn');
  process.env.COMPLETED_FILES_BUCKET = 'testcompletedbucket';
  process.env.UPDATE_BUCKET = 'updatebucket';
  process.env.BQ_DATASET = 'testbqdataset';
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
          errors: [new Error('Something Went Wrong')],
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
    fileName: 'part-r-00001',
    fileReference: {
      name: 'part-r-00001',
      bucket: 'myBucket',
      size: 100,
    },
    jobId: jobId,
    storageFailureStub:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(t.context.fileReference.bucket)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  name: t.context.fileName,
                })
              })
              .withArgs(process.env.COMPLETED_FILES_BUCKET)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  save: sinon.stub().rejects('Some Storage Error.'),
                }),
              })
              .withArgs(process.env.UPDATE_BUCKET)
              .returns({
                file: sinon.stub().withArgs('EOF').returns({
                  exists: sinon.stub().resolves([false]),
                }).withArgs('EOF.lock').returns({
                  exists: sinon.stub().resolves([false]),
                }),
              });
        }),
    storageSuccessStub:
        sinonHelpers.getStubConstructor(Storage).withInit((instance) => {
          instance.bucket.withArgs(t.context.fileReference.bucket)
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  name: t.context.fileName,
                  save: sinon.stub().resolves({}),
                }),
              })
              .withArgs('testcompletedbucket')
              .returns({
                file: sinon.stub().withArgs(t.context.fileName).returns({
                  save: sinon.stub().resolves({}),
                }),
              })
              .withArgs('updatebucket')
              .returns({
                file: sinon.stub().withArgs('EOF').returns({
                  exists: sinon.stub().resolves([false]),
                }).withArgs('EOF.lock').returns({
                  exists: sinon.stub().resolves([false]),
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

ava('importStorageFileIntoBigQuery: log error on file range without dash',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = '210';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'FILE_RANGE environment variable is incorrectly ' +
          'formatted. Format must be {(int)-(int)}.'));
    });

ava('importStorageFileIntoBigQuery: stops executing on file range without dash',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = '210';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.false(bqLoadStub.called);
    });

ava('importStorageFileIntoBigQuery: log error on file range with 2 dashes',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = '2-10-200';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'FILE_RANGE environment variable is incorrectly ' +
          'formatted. Format must be {(int)-(int)}.'));
    });

ava('importStorageFileIntoBigQuery: stop executing on file range with 2 dashes',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = '2-10-200';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.false(bqLoadStub.called);
    });

ava('importStorageFileIntoBigQuery: log error on file range with no numbers',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = 'a-b';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'FILE_RANGE environment variable is incorrectly ' +
          'formatted. Format must be {(int)-(int)}.'));
    });

ava('importStorageFileIntoBigQuery: stop execution on file range w/o numbers',
    async (t) => {
      // Arrange.
      process.env.FILE_RANGE = 'a-b';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.false(bqLoadStub.called);
    });

ava('importStorageFileIntoBigQuery: log error on filename without number',
    async (t) => {
      // Arrange.
      t.context.fileReference = {
        name: 'invalid_filename.txt',
        bucket: 'myBucket',
        size: 100,
      };
      process.env.FILE_RANGE = '1-10';
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match('Filename was not properly numbered ')));
    });

ava('importStorageFileIntoBigQuery: logs error if config has empty schema',
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
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.emptyConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });

ava('importStorageFileIntoBigQuery: logs error if config has invalid value',
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
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.invalidConfigString,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });

ava('importStorageFileIntoBigQuery: logs error if config has invalid column',
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
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.invalidConfigColumnKey,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(sinon.match(
          'Unable to map any columns from the schema config. Aborting...')));
    });


ava('importStorageFileIntoBigQuery: should log error on bq load exception',
    async (t) => {
      // Arrange.
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: sinon.stub().throws(new Error('load failed!')),
                                 setMetadata:
                                     sinon.stub().withArgs(sinon.match.object),
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          sinon.match(`BigQuery load job failed. Failed file: ${
              t.context.fileReference.name}`)));
    });


ava('importStorageFileIntoBigQuery: should log error on bq load error',
    async (t) => {
      // Arrange.
      const bqLoadStub = sinon.stub().resolves(t.context.bqLoadErrorResult);
      const bqTableStub = sinon.stub().returns({
        table: 'items',
        load: bqLoadStub,
      });
      const bigQueryStub =
          sinonHelpers.getStubConstructor(BigQuery).withInit((instance) => {
            instance.dataset.returns({table: bqTableStub});
          });
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageSuccessStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'One or more errors occurred while ' +
          'loading the feed into BigQuery.'));
    });

ava('importStorageFileIntoBigQuery: should log error on storage load error',
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
      const importStorageFileIntoBigQuery = proxyquire('./index', {
        '@google-cloud/storage': {Storage: t.context.storageFailureStub},
        '@google-cloud/bigquery': {BigQuery: bigQueryStub},
        './config.json': t.context.validConfig,
      });

      // Act.
      await importStorageFileIntoBigQuery.importStorageFileIntoBigQuery(
          t.context.fileReference, t.context.context);

      // Assert.
      t.true(consoleError.calledWith(
          'An error occurred when saving the ' +
          'completed filename to GCS.'));
    });
