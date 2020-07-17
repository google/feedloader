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

'use strict';

const fetch = require('node-fetch');
const FormData = require('form-data');

/**
 * Cloud Function that triggers a Cloud Composer DAG.
 *
 * IAP authorization based on:
 * https://stackoverflow.com/questions/45787676/how-to-authenticate-google-cloud-functions-for-access-to-secure-app-engine-endpo
 * and
 * https://cloud.google.com/iap/docs/authentication-howto
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
 *
 * @return {!Promise<undefined>}
 */
exports.triggerDag = async (fileReference, eventMetadata) => {
  const WEBSERVER_URL = `${process.env.WEBSERVER_ID}/api/experimental/dags/${
      process.env.DAG_NAME}/dag_runs`;
  const USER_AGENT = 'gcf-event-trigger';
  const BODY = {conf: JSON.stringify(fileReference)};

  try {
    const iap = await authorizeIap(
        process.env.CLIENT_ID, process.env.PROJECT_ID, USER_AGENT);

    return makeIapPostRequest(WEBSERVER_URL, BODY, iap.idToken, USER_AGENT);
  } catch (error) {
    console.error('Error occurred during request authorization', error);
  }
};

/**
 * @param {string} clientId The client id associated with the Composer webserver
 *     application.
 * @param {string} projectId The id for the project containing the Cloud
 *     Function.
 * @param {string} userAgent The user agent string which will be provided with
 *     the webserver request.
 * @return {!Object} An object containing the jwt and the id token.
 */
const authorizeIap = async (clientId, projectId, userAgent) => {
  const SERVICE_ACCOUNT = `${projectId}@appspot.gserviceaccount.com`;
  const JWT_HEADER = Buffer.from(JSON.stringify({alg: 'RS256', typ: 'JWT'}))
                         .toString('base64');

  let jwt = '';
  let jwtClaimset = '';

  const oAuthEndpoint =
      'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/';
  // Obtain an Oauth2 access token for the appspot service account.
  const response = await fetch(oAuthEndpoint + `${SERVICE_ACCOUNT}/token`, {
    headers: {'User-Agent': userAgent, 'Metadata-Flavor': 'Google'},
  });

  const tokenResponse = await response.json();
  if (tokenResponse.error) {
    return Promise.reject(tokenResponse.error);
  }

  const accessToken = tokenResponse.access_token;
  const iat = Math.floor(new Date().getTime() / 1000);
  const claims = {
    iss: SERVICE_ACCOUNT,                               // Issuer
    aud: 'https://www.googleapis.com/oauth2/v4/token',  // Audience
    iat: iat,                                           // Issued At
    exp: iat + 60,                                      // Expiration Time
    target_audience: clientId,
  };
  jwtClaimset = Buffer.from(JSON.stringify(claims)).toString('base64');
  const toSign = [JWT_HEADER, jwtClaimset].join('.');

  const blob = await fetch(
      `https://iam.googleapis.com/v1/projects/${projectId}/serviceAccounts/${
          SERVICE_ACCOUNT}:signBlob`,
      {
        method: 'POST',
        body: JSON.stringify({
          bytesToSign: Buffer.from(toSign).toString('base64'),
        }),
        headers: {
          'User-Agent': userAgent,
          Authorization: `Bearer ${accessToken}`,
        },
      });

  const blobJson = await blob.json();
  if (blobJson.error) {
    return Promise.reject(blobJson.error);
  }

  // Request service account signature on header and claimset
  const jwtSignature = blobJson.signature;
  jwt = [JWT_HEADER, jwtClaimset, jwtSignature].join('.');

  const form = new FormData();
  form.append('grant_type', 'urn:ietf:params:oauth:grant-type:jwt-bearer');
  form.append('assertion', jwt);

  const token = await fetch('https://www.googleapis.com/oauth2/v4/token', {
    method: 'POST',
    body: form,
  });

  const tokenJson = await token.json();
  if (tokenJson.error) {
    return Promise.reject(tokenJson.error);
  }

  return {
    jwt: jwt,
    idToken: tokenJson.id_token,
  };
};

/**
 * @param {string} url The url that the post request targets.
 * @param {string} body The body of the post request.
 * @param {string} idToken Bearer token used to authorize the iap request.
 * @param {string} userAgent The user agent to identify the requester.
 */
const makeIapPostRequest = async (url, body, idToken, userAgent) => {
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'User-Agent': userAgent,
      Authorization: `Bearer ${idToken}`,
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const error = await res.text();
    console.error('Error occurred during makeIapPostRequest', error);
  }
  console.log(
      'makeIapPostRequest Succeeded. ' +
      `Request sent to authenticate Cloud Composer DAG at ${url}`);
};
