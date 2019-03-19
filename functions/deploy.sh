#!/bin/bash

# --set-env-vars=

gcloud functions deploy $1 \
  --entry-point $1 \
  --project dog-project-234515 \
  --source  https://source.developers.google.com/projects/dog-project-234515/repos/github_dog-project_backend/moveable-aliases/develop/paths/functions/ \
  --runtime python37 \
  --trigger-http \
  --memory 128MB \
  --region us-east1