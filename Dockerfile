# Inside your Dockerfile for data-preprocessors-image
FROM python:3.8-slim

WORKDIR /app

# Copy the Python script and data file into the Docker image
COPY server.py /app/server.py
COPY data_preparation.py /app/data_preparation.py
COPY airbnb_ratings_new_2.csv /app/airbnb_ratings_new_2.csv

# Install required dependencies
RUN pip install --trusted-host pypi.python.org pandas==1.3.3 matplotlib==3.4.3 scikit-learn==0.24.2

CMD ["python", "server.py"]
