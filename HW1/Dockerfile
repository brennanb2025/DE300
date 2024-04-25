# Use a Python base image
FROM python:3.11-bullseye

# Set working directory
WORKDIR /app

# Copy the script and CSV file into the container
COPY EDA.ipynb .
COPY heart_disease.csv .

# Install MySQL client library
# RUN apt-get update && apt-get install -y mysql-client

# Install Python dependencies
RUN pip install mysql-connector-python pandas jupyter sqlalchemy matplotlib seaborn scipy statsmodels

# Expose the Jupyter port
EXPOSE 8888

# Start Jupyter Notebook when the container starts
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
