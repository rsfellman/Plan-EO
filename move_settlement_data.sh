#!/bin/bash

SOURCE_DIR="output_dir_test"
DEST_DIR="../Individual_country_data"
DATA_TYPE="02_Settlement_data"

for country_path in "$SOURCE_DIR"/*; do
  if [ -d "$country_path" ]; then
    country_name=$(basename "$country_path")
    
    # Replace spaces with underscores for destination
    country_dest_name="${country_name// /_}"

    src="$SOURCE_DIR/$country_name/$DATA_TYPE"
    dest="$DEST_DIR/$country_dest_name/$DATA_TYPE"

    if [ -d "$src" ]; then
      # mkdir -p "$dest"
      echo "Moving from '$src' to '$dest'"
      mv "$src"/* "$dest"/
    else
      echo "Skipping: '$src' does not exist."
    fi
  fi
done