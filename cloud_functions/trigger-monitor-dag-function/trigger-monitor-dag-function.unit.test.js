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
 * Copyright 2018 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const proxyquire = require(`proxyquire`).noCallThru();
const sinon = require(`sinon`);
const ava = require(`ava`);

let consoleError;
let consoleLog;

ava.before((t) => {
  process.env.WEBSERVER_ID = 'testwebserverid';
  process.env.DAG_NAME = 'testdagname';
  process.env.CLIENT_ID = 'testclientid';
  process.env.PROJECT_ID = 'testprojectid';
  consoleError = sinon.stub(console, 'error');
  consoleLog = sinon.stub(console, 'log');
});

ava(`Successfully calls fetch in makeIapPostRequest`, async (t) => {
  // Arrange
  const event = {
    data: {
      file: `some-file`,
    },
  };
  const serviceAccountAccessTokenRes = {
    json: sinon.stub().resolves(
        {access_token: 'default-access-token', error: ''}),
  };
  const signJsonClaimRes = {
    json:
        sinon.stub().resolves({signature: 'default-jwt-signature', error: ''}),
  };
  const getTokenRes = {
    json: sinon.stub().resolves({id_token: 'default-id-token', error: ''}),
  };
  const makeIapPostRequestRes = {
    ok: true,
  };
  const fetchStub = sinon.stub()
                        .onCall(0)
                        .resolves(serviceAccountAccessTokenRes)
                        .onCall(1)
                        .resolves(signJsonClaimRes)
                        .onCall(2)
                        .resolves(getTokenRes)
                        .onCall(3)
                        .resolves(makeIapPostRequestRes);
  const triggerDag = proxyquire(`./`, {
    'node-fetch': fetchStub,
  });

  // Act
  await triggerDag.triggerDag(event, {});

  // Assert
  t.true(fetchStub.getCall(3).calledWith(
      'testwebserverid/api/experimental/dags/testdagname/dag_runs',
      sinon.match.any));
  t.true(consoleLog.calledWith(sinon.match('makeIapPostRequest Succeeded.')));
});

ava(`Handles error in JSON body`, async (t) => {
  // Arrange
  const event = {
    data: {
      file: `some-file`,
    },
  };
  const expectedMsg = `Error occurred during request authorization`;
  const bodyJson = {error: expectedMsg};
  const body = {
    json: sinon.stub().resolves(bodyJson),
  };
  const triggerDag = proxyquire(`./`, {
    'node-fetch': sinon.stub().resolves(body),
  });

  // Act
  await triggerDag.triggerDag(event, {});

  // Assert
  t.true(consoleError.calledWith(sinon.match(expectedMsg)));
});

ava(`Handles error in IAP response.`, async (t) => {
  // Arrange
  const event = {
    data: {
      file: `some-file`,
    },
  };
  const expectedMsg = 'Error occurred during makeIapPostRequest';
  const serviceAccountAccessTokenRes = {
    json: sinon.stub().resolves(
        {access_token: 'default-access-token', error: ''}),
  };
  const signJsonClaimRes = {
    json:
        sinon.stub().resolves({signature: 'default-jwt-signature', error: ''}),
  };
  const getTokenRes = {
    json: sinon.stub().resolves({id_token: 'default-id-token', error: ''}),
  };
  const makeIapPostRequestRes = {
    ok: false,
    text: sinon.stub().resolves(expectedMsg),
  };
  const fetchStub = sinon.stub()
                        .onCall(0)
                        .resolves(serviceAccountAccessTokenRes)
                        .onCall(1)
                        .resolves(signJsonClaimRes)
                        .onCall(2)
                        .resolves(getTokenRes)
                        .onCall(3)
                        .resolves(makeIapPostRequestRes);
  const triggerDag = proxyquire(`./`, {
    'node-fetch': fetchStub,
  });

  // Act
  await triggerDag.triggerDag(event, {});

  // Assert
  t.true(consoleError.calledWith(sinon.match(expectedMsg)));
});
