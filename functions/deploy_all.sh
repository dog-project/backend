#!/usr/bin/env bash

./deploy.sh get_dog &
./deploy.sh submit_dog &
./deploy.sh get_dog_pair &
./deploy.sh submit_vote &
./deploy.sh register_voter &
./deploy.sh get_votes &
./deploy.sh list_dogs &
./deploy.sh submit &
./deploy.sh get_demographics &

echo "Running deployment, this may take a sec."
