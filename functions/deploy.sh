#!/bin/bash

# --set-env-vars=


gcloud functions deploy $1 \
  --source  https://source.developers.google.com/projects/dog-project-234515/repos/github_dog-project_backend/moveable-aliases/master/paths/functions/ \
  --entry-point $1 \
  --project dog-project-234515 \
  --runtime python37 \
  --trigger-http \
  --memory 128MB \
  --region us-east1 \
  --env-vars-file .env.yaml