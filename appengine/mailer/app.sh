# Copyright 2021 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash -u
CURRENT_DIRECTORY=$(dirname "$0")

if [[ $# = 0 ]] || [[ $1 = 'dev' ]]; then
  python -m main
elif [[ $1 == 'test' ]]; then
  python -m runner /usr/lib/google-cloud-sdk/
elif [[ $1 = 'prod' ]]; then
  if [[ "$#" -le 3 ]]; then
    echo "Project ID, Pubsub Token, and Email To Address is required when deploying to prod."
  else
    pip install -U setuptools
    pip install -r "$CURRENT_DIRECTORY"/requirements.txt -t "$CURRENT_DIRECTORY"/lib
    sed -e "s/<PROJECT_ID>/$2/g; s/<PUBSUB_TOKEN>/$3/g; s/<EMAIL_TO>/$4/g" \
      "$CURRENT_DIRECTORY"/app_template.yaml > "$CURRENT_DIRECTORY"/app.yaml
    gcloud beta app deploy "$CURRENT_DIRECTORY"/app.yaml \
      --project "$2" --quiet \
      && echo "Mailer app has been successfully deployed to $2."
    rm "$CURRENT_DIRECTORY"/app.yaml
  fi
else
  echo "You must specify a correct environment to run the application. Select from dev or prod."
fi
