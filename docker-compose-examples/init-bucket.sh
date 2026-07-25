#!/bin/sh
# Wait for MinIO to be up
sleep 5

# Create a bucket using mc (MinIO Client)
mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc mb -p myminio/semoss
