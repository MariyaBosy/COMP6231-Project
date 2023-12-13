# Inside your Dockerfile for data-preprocessors-image
FROM python:3.8-slim
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the input file into the Docker image
COPY airbnb_sample.csv /app/airbnb_sample.csv

# Ensure the necessary tools or script is included
COPY server.py /app/server.py

# Update the CMD to run the script
CMD ["python", "server.py"]
