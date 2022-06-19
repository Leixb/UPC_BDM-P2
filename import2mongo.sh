#!/usr/bin/env bash

set -e

# This script is used to import all the data into mongo
# Modify the variables below to match your environment

DATA_DIR="${DATA_DIR:-../P2_data/}"
DATABASE="${DATABASE:-test}"

FILES=(
    "${DATA_DIR}/income_opendata/income_opendata_neighborhood.json"
    "${DATA_DIR}/lookup_tables/"*.json
    )

for file in "${FILES[@]}"; do
    echo "Importing ${file}..."
    mongoimport --db "${DATABASE}" --collection "$(basename "${file}" .json)" --file "${file}"
done

# Our additional data source folder
INCIDENT_FOLDER="${INCIDENT_FOLDER:-"${DATA_DIR}/incidents/"}"

# We need jq and sed to make mongo import the data properly
for i in "${INCIDENT_FOLDER}/"*.json ; do
    echo "Importing ${file}..."
    cat "$i" | jq .[] | sed 's/Número/Numero/' | mongoimport --db "${DATABASE}" -c incidents
done
