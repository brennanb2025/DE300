#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock


# Create a Docker volume for data persistence
echo "Creating Docker volume: homework1-heart-disease"
docker volume create --name eda-database

# Create a Docker network for container communication
echo "Creating Docker network: eda-network"
docker network create eda-network

# Build MySQL Docker image
echo "Building MySQL Docker image from docker's default mysql image"
docker pull mysql:latest
docker run -d --name my-mysql \
    -e MYSQL_ROOT_PASSWORD=root_pwd \
    -e MYSQL_DATABASE=DE300db \
    --network eda-network \
    mysql

# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-eda-jupyter"
docker build -f dockerfiles/dockerfile-eda-jupyter -t eda .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network eda-network \
	   --name jupyter-eda \
	   -v ./src:/app/src \
	   -v ./staging_data:/app/staging_data \
	   -p 8888:8888 \
	   eda

CMD ["/bin/bash"]