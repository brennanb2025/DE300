#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock


# Create a Docker volume for data persistence
echo "Creating Docker volume: homework2-heart-disease"
docker volume create --name etl-database

# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-ETL-jupyter"
docker build -f dockerfiles/dockerfile-ETL-jupyter -t etl .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --name jupyter-etl \
	   -v ./src:/app/src \
	   -p 8888:8888 \
	   etl

CMD ["/bin/bash"]