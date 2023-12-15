import argparse
import logging
import socket
import sys
import threading
import os
import subprocess
import time
import matplotlib.pyplot as plt
import pandas as pd
from data_preparation import load_data, clean_data, transform_data, feature_engineering, scale_data, save_data
from flask import Flask, request, jsonify
from load_balancer import LoadBalancer
from waitress import serve

app = Flask(__name__)

class Server:
    load_balancer = None

    def __init__(self, host, port):
        self.host = host
        self.port = port

        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def set_load_balancer(cls, load_balancer):
        cls.load_balancer = load_balancer

    # def start_server(self):
    #     # Register the server with the load balancer
    #     self.register_with_load_balancer()

    #     # Run the Flask app using Gunicorn
    #     print(f"Starting Flask server on {self.host}:{self.port}")
    #     serve(app, host=self.host, port=int(self.port))

    def register_with_load_balancer(self):
        self.load_balancer.register_server(self.host, self.port)

    def start_server(self):
        # Register the server with the load balancer
        self.register_with_load_balancer()

        # Run the Flask app using Gunicorn
        self.logger.info(f"Starting Flask server on {self.host}:{self.port}")

        # Start the main loop for accepting client connections
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        while True:
            client_socket, addr = server_socket.accept()
            self.logger.info(f"Accepted connection from {addr}")
            
            # Generate a unique identifier for the client (e.g., based on address)
            client_identifier = addr

            # Pass the client identifier to the client_handler thread
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket, client_identifier))
            client_handler.start()

    def start_flask(self):
        # Run the Flask app using Gunicorn
        print(f"Starting Flask server on {self.host}:{self.port}")
        serve(app, host=self.host, port=int(self.port))

    # Modify the handle_client method in your Server class
    def handle_client(self, client_socket, client_identifier):
        # Receive the command from the client
        command = client_socket.recv(1024).decode()

        response = None  # Initialize response variable
        
        # Use the client identifier to determine the target host consistently
        target_host = self.load_balancer.get_next_server(client_identifier)

        self.logger.info(f"Received command: {command}")

        # Forward the client request to the selected server
        # forward_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # forward_socket.connect((target_host[0], target_host[1]))
        # forward_socket.send(command.encode())

        if command == "data_preparation":
            # Update the file paths as needed
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.data_preparation(file_paths)
        elif command == "data_prep":
            response = self.data_prep()
        elif command == "preprocessing_data":
            # Receive additional arguments (file_paths)
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.preprocessing_data(file_paths)
        elif command == "containerize_elasticsearch":
            self.containerize_elasticsearch()
            response = "Elasticsearch containerized successfully!"
        elif command == "run_docker_container":
            self.run_docker_container()
            response = "Docker container started successfully!"
        elif command == "stop_docker_container":
            self.stop_docker_container()
            response = "Docker container stopped successfully!"
        elif command == "deploy_kubernetes":
            self.deploy_kubernetes()
            response = "Kubernetes deployment created successfully!"
        elif command == "undeploy_kubernetes":
            self.undeploy_kubernetes()
            response = "Kubernetes deployment deleted successfully!"
        else:
            response = "Invalid command"
        
        # Send the response back to the client
        client_socket.send(response.encode())

        # Close the sockets
        # forward_socket.close()
        client_socket.close()


    def data_preparation(self, file_paths):
        print("Entering data_preparation method")
        try:
            response = None  # Initialize response variable

            for file_path in file_paths:
                print(f"Processing file: {file_path}")
                # Load the dataset
                df = load_data(file_path)

                if df is not None:
                    # Data preparation steps
                    try:
                        df = clean_data(df)
                        df = transform_data(df)
                        df = feature_engineering(df)
                        df = scale_data(df)

                        # Save the prepared data to a new file
                        output_file = os.path.splitext(file_path)[0] + "_prepared.csv"
                        save_data(df, output_file)

                        print(f"Data preparation completed for {file_path}. Prepared data saved to {output_file}")
                    except Exception as e:
                        response = f"Error during data preparation steps: {e}"
                else:
                    response = f"Data preparation skipped for {file_path} due to loading errors."

            if response is None:
                response = "Data preparation completed successfully!"
        except Exception as e:
            response = f"Error during data preparation: {e}"

        return response

    def data_prep(self):
        # Load the dataset
        df = load_data("./files/airbnb-reviews.csv")

        # Data preparation steps
        df = clean_data(df)
        df = transform_data(df)
        df = feature_engineering(df)
        df = scale_data(df)

        # Save the prepared data to a new file
        save_data(df, "./files/airbnb-reviews_prepared.csv")

        response = "Data preparation for 1GB file completed successfully!"
        return response

    def preprocessing_data(self, file_paths):
        print("Entering preprocessing method")
        try:
            for file_path in file_paths:
                # Update the path to the CSV file inside the Docker container
                container_input_path = f"/app/{file_path}"
                subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}:/app", "data-preprocessors-image", "python", "data_preparation.py", container_input_path], check=True)

            response = "Data preprocessing completed successfully!"
        except subprocess.CalledProcessError as e:
            response = f"Error during data preprocessing: {e}"

        return response

    def containerize_elasticsearch(self):
        # Assume Elasticsearch is installed and running locally
        # Stop the Elasticsearch service if it's running
        subprocess.run(["docker", "stop", "elasticsearch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Remove the existing Elasticsearch container if it exists
        subprocess.run(["docker", "rm", "elasticsearch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run Elasticsearch in a Docker container
        subprocess.run(["docker", "run", "-d", "--name", "elasticsearch", "-p", "9200:9200", "elasticsearch:8.11.1"])

        return "Elasticsearch containerized successfully!"

    def run_docker_container(self):
        # Stop the existing Docker container if it's running
        subprocess.run(["docker", "stop", "nginx-container"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Remove the existing Docker container if it exists
        subprocess.run(["docker", "rm", "nginx-container"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run a sample Docker container (nginx in this case)
        subprocess.run(["docker", "run", "-d", "--name", "nginx-container", "-p", "8080:80", "nginx"])

        return "Docker container started successfully!"

    def stop_docker_container(self):
        # Stop the sample Docker container
        subprocess.run(["docker", "stop", "nginx-container"])

        return "Docker container stopped successfully!"

    def deploy_kubernetes(self):
        # Deploy a sample Kubernetes deployment (nginx in this case)
        subprocess.run(["kubectl", "create", "deployment", "nginx-deployment", "--image=nginx"])

        return "Kubernetes deployment created successfully!"

    def undeploy_kubernetes(self):
        # Undeploy the sample Kubernetes deployment
        subprocess.run(["kubectl", "delete", "deployment", "nginx-deployment"])

        return "Kubernetes deployment deleted successfully!"


def run_server(host, port, load_balancer):
    server = Server(host=host, port=port, load_balancer=load_balancer)
    server.register_with_load_balancer()  # Register the server with the load balancer

    # Start the Flask app in a separate thread
    flask_thread = threading.Thread(target=server.start)
    flask_thread.start()

    # Start the server as before
    server.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Flask server with load balancing.")
    parser.add_argument("--host", type=str, help="Host address of the server")
    parser.add_argument("--port", type=int, help="Port number for the server")
    args = parser.parse_args()

    load_balancer = LoadBalancer([])  # Empty list, to be populated dynamically

    # Assuming you have three servers running on different hosts and ports
    server1 = ("127.0.0.1", 5001)
    server2 = ("127.0.0.1", 5002)
    server3 = ("127.0.0.1", 5003)

    load_balancer = LoadBalancer([server1, server2, server3])

    # Set the load balancer for all instances
    Server.set_load_balancer(load_balancer)

    if args.host and args.port:
        # Run a specific server specified by command-line arguments
        server_instance = Server(args.host, args.port)
        server_instance.start_server()
    else:
        # Run each server on a separate thread
        server_instance1 = Server(server1[0], server1[1])
        server_instance2 = Server(server2[0], server2[1])
        server_instance3 = Server(server3[0], server3[1])

        # Start each server in a separate thread
        thread1 = threading.Thread(target=server_instance1.start_server)
        thread2 = threading.Thread(target=server_instance2.start_server)
        thread3 = threading.Thread(target=server_instance3.start_server)

        # Start the threads
        thread1.start()
        thread2.start()
        thread3.start()

        # Wait for all threads to finish
        thread1.join()
        thread2.join()
        thread3.join()
