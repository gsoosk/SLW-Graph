#!/bin/bash

# Stop and remove any existing Docker container
docker stop my_postgres && docker rm -v my_postgres

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
  echo "No values provided. Please provide the values as input arguments."
  exit 1
fi

# Store the input arguments as values
values=("$@")
OS=$(uname)

# Loop over each value
for value in "${values[@]}"
do
  echo "Processing with value: $value"

  # Step 1: Make a copy of tpcc_config.xml
  cp tpcc_config.xml tpcc_config_temp.xml

  # Step 2: Modify the tpcc_config_temp.xml with the current value
  if [ "$OS" == "Darwin" ]; then
    # macOS
    sed -i '' "s/PLACEHOLDER_VALUE/$value/g" tpcc_config_temp.xml
  elif [ "$OS" == "Linux" ]; then
    # Linux
    sed -i "s/PLACEHOLDER_VALUE/$value/g" tpcc_config_temp.xml
  else
    echo "Unsupported OS: $OS"
    exit 1
  fi

  # Step 3: Run Postgres Docker container
  docker run -d \
    --name my_postgres \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -p 5432:5432 \
    postgres:16-alpine

  # Give time for Postgres to start
  sleep 10

  # Step 4: Run BenchBase to populate the database
  cd benchbase-postgres
  java -jar benchbase.jar -b tpcc -c ../tpcc_config_temp.xml --create=true --load=true --execute=false
  cd ..

  # Step 5: Get pg_dump to a .sql file outside of the Docker image
  docker exec -t my_postgres pg_dump -U user -d postgres > ./init_$value.sql

  # Step 6: Stop and remove the Postgres container and its volumes
  docker stop my_postgres && docker rm -v my_postgres

  # Step 7: Remove the temporary tpcc_config_temp.xml file
  rm tpcc_config_temp.xml

done
