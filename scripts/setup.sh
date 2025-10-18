#!/bin/bash

readonly -r ENDPOINT_URL="http://localhost:9324"

create_queue() {
  local queue_name="$1"
  
  aws sqs create-queue \
    --queue-name "$queue_name" \
    --endpoint-url "$ENDPOINT_URL"
}

create_queue "test-queue"
