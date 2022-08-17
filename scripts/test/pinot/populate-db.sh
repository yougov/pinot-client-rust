#!/bin/bash

set -e
python wait-for-broker.py
docker exec -it pinot-client-rust-pinot-controller bin/pinot-admin.sh AddTable   \
  -tableConfigFile /db/score_sheet/offline_table.json   \
  -schemaFile /db/score_sheet/schema.json -exec
docker exec -it pinot-client-rust-pinot-controller bin/pinot-admin.sh LaunchDataIngestionJob \
  -jobSpecFile /db/score_sheet/job_spec.yaml
