#!/bin/bash

# Target service to monitor
SERVICE_NAME="client"
# PROB_VALUES=(100)
MODE_VALUES=("slw" "bamboo" "ww")
DELAY_VALUES=(10 30 50)
PROB_VALUES=(100 90 80 70 60 50 40 30 20 10 0)
HOT_VALUES=(16 32 64 128)
# HOT_VALUES=(64)
NUM_OF_ITEM=(-1)
# NUM_OF_ITEM=(-1 20 30 40)

sudo docker-compose --compatibility down --volumes 2>/dev/null


for MODE in "${MODE_VALUES[@]}"; do
    for DELAY in "${DELAY_VALUES[@]}"; do
        for ITEM in "${NUM_OF_ITEM[@]}"; do
            for HOT in "${HOT_VALUES[@]}"; do
                for PROB in "${PROB_VALUES[@]}"; do
                    echo "Starting test with PROB=$PROB and HOT_RECORDS=$HOT and ITEM_NUM=$ITEM"

                    # Create and populate the .env file
                    echo "PROB=$PROB" > ".env"
                    echo "HOT=$HOT" >> ".env"
                    echo "ITEM=$ITEM" >> ".env"
                    echo "MODE=$MODE" >> ".env"
                    echo "DELAY=$DELAY" >> ".env"

                    sudo docker-compose --compatibility up -d 2>/dev/null

                    # Wait for the specified service to exit
                    while [ $(sudo docker ps -q -f name="$SERVICE_NAME" | wc -l) -gt 0 ]; do
                        sleep 5  # Checks every 5 seconds
                    done

                    # When the service exits, bring everything down
                    sudo docker-compose --compatibility down --volumes 2>/dev/null

                done
            done
        done
    done
done