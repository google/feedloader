# FeedLoader

_Copyright 2019 Google LLC. This solution, including any related sample code or
data, is made available on an “as is,” “as available,” and “with all faults”
basis, solely for illustrative purposes, and without warranty or representation
of any kind. This solution is experimental, unsupported and provided solely for
your convenience. Your use of it is subject to your agreements with Google, as
applicable, and may constitute a beta feature as defined under those agreements.
To the extent that you make any data available to Google in connection with your
use of the solution, you represent and warrant that you have all necessary and
appropriate rights, consents and permissions to permit Google to use and process
that data. By using any portion of this solution, you acknowledge, assume and
accept all risks, known and unknown, associated with its usage, including with
respect to your deployment of any portion of this solution in your systems, or
usage in connection with your business, if at all._

- [FeedLoader](#feedloader)
  - [Introduction](#introduction)
    - [What does it do?](#what-does-it-do)
    - [Who is it for?](#who-is-it-for)
    - [What does it cost?](#what-does-it-cost)
  - [Setup Guide](#setup-guide)
    - [Prerequisites](#prerequisites)
    - [Configuration](#configuration)
    - [Installation](#installation)
  - [Usage](#usage)
  - [MCID-Per-Item Feature](#mcid-per-item-feature)
  - [Optimizations](#optimizations)
  - [Testing](#testing)
    - [Unit Tests](#unit-tests)
    - [End-To-End Tests](#end-to-end-tests)
  - [Syncing your deployment with future Feedloader releases](#syncing-your-deployment-with-future-feedloader-releases)
  - [Dev Ops Maintenance Guide](#dev-ops-maintenance-guide)

## Introduction

### What does it do?

Feedloader is a packaged solution for Google Cloud that automates ingestion of
Google Merchant Center ("GMC") Shopping feeds, and sends the data to GMC via a
Content API interface at minimal cost and maximal performance.

Feedloader also features automatic prevention of product expiration in GMC,
along with logging, alerts, and potential for user-contributed optimization
expansions.

### Who is it for?

Users of GMC that have a Multi-Client Account ("MCA") or sub-accounts with
Merchant Center IDs and feed files (CSV/TSV format) of their products can
leverage Feedloader to automate Shopping data uploads at scale.

### What does it cost?

The only costs required to operate Feedloader are those to run the GCP
environment's components, which are listed below. **Cost to run Feedloader will
vary depending on usage frequency and volume of data processed**.

Individual product pricing can be found on their respective pricing pages, but
we have taken precautions and design considerations to reduce GCP usage cost as
much as possible, as many of the services fall under the free-tier usage limit
for most use cases of Feedloader.

-   App Engine (https://cloud.google.com/appengine/pricing)
-   BigQuery (https://cloud.google.com/bigquery/pricing)
-   Cloud Build (https://cloud.google.com/cloud-build/pricing)
-   Cloud Composer (https://cloud.google.com/composer/pricing)
-   Cloud Functions (https://cloud.google.com/functions/pricing)
-   Cloud Run (https://cloud.google.com/run/pricing)
-   Cloud Source Repositories
    (https://cloud.google.com/source-repositories/pricing)
-   Cloud Storage (https://cloud.google.com/storage/pricing)
-   Cloud Tasks (https://cloud.google.com/tasks/docs/pricing)
-   Key Management Service (https://cloud.google.com/kms/pricing)
-   PubSub (https://cloud.google.com/pubsub/pricing)
-   Cloud Logging (https://cloud.google.com/stackdriver/pricing)

## Setup Guide

This section will explain how to setup Feedloader for use in a GCP environment.

**DISCLAIMER: This guide assumes that your GCP environment is at default
settings and is not customized in any way. Any pre-existing GCP settings
modifications, configurations, or customizations may affect the installation and
operation of this solution. For the best experience, use a new/unused
GCP project when proceeding with the below guide.**

### Prerequisites

-   [Install the Google Cloud SDK](https://cloud.google.com/sdk/) to be able to
    run CLI commands. Ensure both alpha and beta are installed, and update
    components to the latest versions:

    -   `gcloud components install alpha`

    -   `gcloud components install beta`

    -   `gcloud components update`

-   [Install Python version 3 or greater](https://www.python.org/downloads/).

    -   Then install the following packages:

        `pip3 install google-auth requests`

-   Have a Google Cloud Project available, or
    [create a new one](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

    -   If a billing account has not been set on the project, set one by
        [following these instructions](https://cloud.google.com/billing/docs/how-to/modify-project).

-   Clone the code from this repository.

### Configuration

-   Move to the root directory of the project and open the file
    `feed_schema_config.json`. Customize the mapping by naming the `csvHeader`
    fields to match your feed file’s schema/headers.

    -   The fields for 'bqColumn' should not be changed, but if your feed has
        Content API fields that are not in the default json schema, additional
        fields can be added. Refer to the full Content API product spec
        [here](https://developers.google.com/shopping-content/v2/reference/v2/products#resource-representations).

    -   Ensure that the naming and order matches your feed file’s headers
        **exactly**, or the resulting data in GMC will be corrupted.

    -   If your feeds specify different GMC IDs per-item, append the following
        column entry for the GMC ID, and ensure your feed contains the header
        `google_merchant_id` for these IDs:

        `{"csvHeader": "google_merchant_id", "bqColumn": "google_merchant_id",
        "columnType": "STRING"}`

        See the section [MCID-Per-Item](#mcid-per-item-feature) for details.

-   Edit the env.sh file in the repository root directory and supply values for
    all of the variables.

    -   For the variables ending in "BUCKET", follow the
        [naming conventions](https://cloud.google.com/storage/docs/naming) for
        Google Cloud Storage buckets or they will not be created properly.

### Installation

-   Run the initialization script in the root directory:

    `bash install_to_gcp.sh`

    -   **Due to dependencies on Cloud Composer environment initialization, the
        installation could take a long time (~45 minutes).**

    -   If the scripts hangs (no log output for > 30 minutes), try using Ctrl-C
        to kill it. It might unblock it (if something was hanging) or kill it.
        If it gets killed, you can re-run it safely as the script is written to
        be re-runnable without harm.

    -   Check the logs of the script for any errors.

-   Add the service account created in the script to GMC by following these
    steps:

    -   Navigate to your GMC UI.

    -   Go to User access for GMC (section: “Invite a new user”).

    -   Add the Service account’s email address (replace [PROJECT_ID] with your
        GCP project ID):

        merchant-center@[PROJECT_ID].iam.gserviceaccount.com

-   In order to deploy the Feedloader solution to your GCP project, it is
    required to perform a git push to your GCP project's Cloud Source Repository
    (reference:
    [Cloud Source Repositories](https://cloud.google.com/source-repositories/docs/pushing-code-from-a-repository)).
    Cloud Build will auto-trigger on the git push and deploy the code to GCP.

    -   Generate a new identifier for Cloud Source Repositories by running the
        command shown on https://source.developers.google.com/new-password
    -   Run the following git commands in the same local repository you cloned
        and ran the initialization script in:

        1.  `git remote add google
            ssh://[EMAIL]@source.developers.google.com:2022/p/[PROJECT_ID]/r/[REPO_NAME]`
            (Where `PROJECT_ID` is your GCP project ID and `REPO_NAME` is the
            repository name you set for `SOURCE_REPO` in env.sh)
        2.  `git add -A`
        3.  `git status`
        4.  Ensure no service_account .json files are being added, just the .enc
            files and a variables.json file.
        5.  `git commit -m "[Your commit message]"`
        6.  `git push --all google` (If you see a prompt for choosing a
            configuration, choose option 1)

    -   This will trigger the build scripts installed in your GCP project to
        deploy all the code and necessary resources. It should take around 2
        minutes. [You can check the status in your GCP Console's Cloud Build
        dashboard or history
        tab](https://cloud.google.com/cloud-build/docs/view-build-results#viewing_build_results).
        Ensure that the logs show no errors.

-   Feedloader should now be ready to use.

## Usage

-   Set the following environment variables in your shell:

    -   `GCP_PROJECT`: The ID of your GCP project.

    -   `FEED_PATH`: Set this to the directory where your feed data files
        reside. **They must be in CSV or TSV format**. It

        can be a relative path, absolute path, or ~ path, but do not use double
        quotes if you use a ~ in the path.

    -   `FEED_BUCKET`: The GCP Storage Bucket to upload feed files to, e.g.
        “gs://shopping-feed-bucket”.

    -   `UPDATE_BUCKET`: The GCP Storage Bucket to upload the EOF file to, e.g.
        “gs://update-bucket”.

-   Login to GCP with gcloud and set your current project:

    `gcloud auth login`

    `gcloud config set project $GCP_PROJECT`

-   Upload your feed files to GCP Cloud Storage using this command:

    `gsutil cp -j csv $FEED_PATH/* $FEED_BUCKET`

-   **Wait until all feed files are uploaded.** Depending on the number and size
    of the files, this may take several minutes to fully load into BigQuery. We
    recommend to wait approximately 5 minutes for a batch of 100 files at
    ~50,000 products per file.

-   Prepare an empty zero-byte file named “EOF” (do not put it in the same
    folder as the feed files) and upload it to the UPDATE_BUCKET GCS bucket:

    `touch ~/[PATH_TO_EOF]/EOF`

    `gsutil cp [PATH_TO_EOF]/EOF $UPDATE_BUCKET`

-   Feedloader will process items that require changes in Content API (new
    items, updated items, deleted items, and items about to expire), and send
    them to GMC. You can view the results in your GMC UI.

## MCID-Per-Item Feature
**Important: This feature is only supported when all MCIDs fall under a single
MCA**

Some users of Feedloader will want to send different items within a single feed
file to more than one Merchant Center ID ("MCID").

For these cases, Feedloader supports the addition of an optional
“google_merchant_id” column in the feed files: a column with the header
"google_merchant_id" may be added to your feeds, which specifies the destination
Merchant Center to route the Content API call for that item row.

Feedloader will automatically handle routing of this item to the MC ID via
Content API (if an MCID was specified for that item row).

This value will be used as the account id specified in calls to Content API and
will determine the destination account that items are sent to.

The “google_merchant_id” column is loaded into BigQuery in the same way as other
columns.

Feedloader's behavior when this column is included in the feed file differs
depending on whether the environment variable "IS_MCA" is set to True or False.
This behavior is defined in the below table:

||IS_MCA = True|IS_MCA = False|
|:----|:----|:----|
|**"google_merchant_id" column is missing or value is equivalent to boolean False (empty, 0, etc)**|Item is "skipped" in processing and not added to the batch + a warning is logged| The env var "DEFAULT_MERCHANT_ID" is used as the destination merchant_id in API calls|
|**"google_merchant_id" exists and is set to a value not equivalent to boolean False**|The "google_merchant_id" value in each row of the feed file is used as the destination merchant_id in API calls| The env var "DEFAULT_MERCHANT_ID" is used as the destination merchant_id in API calls|

## Optimizations

Before Feedloader sends product data to Content API, it has the option to
interface with a "Shoptimizer" REST API, a standalone solution that can be
hosted in a Docker container which includes various optimization logic that
attempts to automatically "fix" the product data being uploaded (based on best
practices) before going into GMC.

This is controlled by setting the environment variables
"SHOPTIMIZER_API_INTEGRATION_ON" and "SHOPTIMIZER_URL" upon installation of
Feedloader. See the env.sh file for explanations of these variables.

The Shoptimizer API can also be used standalone, for users that already have a
Content API interface.

See the Shoptimizer repository [here](https://github.com/google/shoptimizer).

## Testing

Feedloader comes packaged with suites of unit tests, API integration tests, and
end-to-end tests.

### Unit Tests

Some unit tests are currently run as part of the build process. i.e., the code
will not deploy unless the tests all pass in Cloud Build. These include the
Shoptimizer API unit tests.

Other tests will need to be run manually via Ava (for Cloud Functions) and the
packaged test runners for Python App Engine code.

### End-To-End Tests

Feedloader comes packaged with several bash test scripts that kick off actual
data uploads and EOF triggers to run the solution automatically for various use
cases, and each time the scripts are run, GCP resources are force-cleaned so
that they can be run again and again.

## Syncing your deployment with future Feedloader releases

This repository may be updated with new releases. To get the newly released code
deployed to your GCP environment, either perform a git pull from this
repository, or overwrite your local repository with one of the tagged releases.
Then perform a git add, commit, and push into your Cloud Source Repository.
Refer to the diagram below for an overview.

If infrastructure or environment changes are required (i.e. not solely
code-based changes), then documentation will be provided on how to update for
that release.

![Code Sharing Process](./img/CodeSharingDesign.png)

## Dev Ops Maintenance Guide
For Developers/Maintainers of a Feedloader deployment, please see this
[guide](./documentation/maintenance-guide.md) for common troubleshooting
procedures and frequently asked questions.
