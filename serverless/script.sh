#!/bin/bash

IMAGE_NAME="bixi-lambda:latest"
CONTAINER_NAME="bixi-lambda-local"

if [ ! -f .env ]; then
    echo ".env file not found."
    exit 1
fi

echo "Building Docker image."
docker buildx build --platform linux/arm64 --provenance=false -t $IMAGE_NAME .

if [ $? -ne 0 ]; then
    echo "Docker build failed."
    exit 1
fi

# rm and stop old containers
docker rm -f $CONTAINER_NAME 2>/dev/null

echo "Starting new container"
docker run --name $CONTAINER_NAME \
           --env-file .env \
           --platform linux/arm64 \
           -p 9000:8080 \
           -d $IMAGE_NAME

if [ $? -ne 0 ]; then
    echo "Docker run failed."
    exit 1
fi

echo "Container is running."
echo "test with"
echo "curl -X POST \"http://localhost:9000/2015-03-31/functions/function/invocations\" -d '{}'"
