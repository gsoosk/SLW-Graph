#!/bin/bash

# Target service to monitor
SERVICE_NAME="client"
# PROB_VALUES=(100)
MODE_VALUES=("ww")
DELAY_VALUES=(2)
PROB_VALUES=(0)
# PROB_VALUES=(100 90 80 70 60 50 40 30 20 10 0)
# HOT_VALUES=(16 32 64 128)
HOT_VALUES=(64)
# NUM_OF_ITEM=(-1)
NUM_OF_ITEM=(20)

sudo docker-compose --compatibility down --volumes 2>/dev/null


for DELAY in "${DELAY_VALUES[@]}"; do
    for ITEM in "${NUM_OF_ITEM[@]}"; do
        for HOT in "${HOT_VALUES[@]}"; do
            for MODE in "${MODE_VALUES[@]}"; do
                for PROB in "${PROB_VALUES[@]}"; do

                    echo "Starting test for $MODE with DELAY=$DELAY PROB=$PROB and HOT_RECORDS=$HOT and ITEM_NUM=$ITEM"

                    # Create and populate the .env file
                    echo "PROB=$PROB" > ".env"
                    echo "HOT=$HOT" >> ".env"
                    echo "ITEM=$ITEM" >> ".env"
                    echo "MODE=$MODE" >> ".env"
                    echo "DELAY=$DELAY" >> ".env"

                    sudo docker-compose --compatibility up -d 2>/dev/null

                    # Define timeout in seconds
                    TIMEOUT=300  # 5 minutes timeout
                    ELAPSED=0
                    SLEEP_INTERVAL=5
                    # Wait for the specified service to exit
                    while [ $(sudo docker ps -q -f name="$SERVICE_NAME" | wc -l) -gt 0 ]; do
                        if [ $ELAPSED -ge $TIMEOUT ]; then
                            echo "Timeout reached. Service did not exit within the timeout period. restart!"
                            sudo docker-compose --compatibility down --volumes 2>/dev/null
                            sleep 10
                            sudo docker-compose --compatibility up -d 2>/dev/null
                            sleep 10
                            ELAPSED=0
                            echo "New Elapsed $ELAPSED"
                        fi
                        sleep $SLEEP_INTERVAL
                        ELAPSED=$((ELAPSED + SLEEP_INTERVAL))
                    done

                    # When the service exits, bring everything down
                    sudo docker-compose --compatibility down --volumes 2>/dev/null

                done
            done
        done
    done
done
