#!/usr/bin/env bash
set -euo
set -o pipefail

: "${GCS_PROCESSED_PATH:?GCS_PROCESSED_PATH is required}"
: "${S3_LANDING_PATH:?S3_LANDING_PATH is required}"

echo "Syncing from ${GCS_PROCESSED_PATH} to ${S3_LANDING_PATH}"
rclone sync "${GCS_PROCESSED_PATH}" "${S3_LANDING_PATH}" --progress --fast-list
echo "Sync complete."
