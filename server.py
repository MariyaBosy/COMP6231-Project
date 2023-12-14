import socket
import threading
import os
import subprocess
import matplotlib.pyplot as plt
import pandas as pd
from data_preparation import load_data, clean_data, transform_data, feature_engineering, scale_data, save_data

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.airbnb_dataset_path = "C:/Users/Reema Reny/Documents/GitHub/DSD/files/airbnb_ratings_new.csv"  # Update this path to your Airbnb dataset

    def start(self):
        self.s_socket.bind((self.host, self.port))
        self.s_socket.listen()

        print(f"Server listening on {self.host}:{self.port}")

        while True:
            client_socket, client_address = self.s_socket.accept()
            print(f"Accepted connection from {client_address}")

            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()

    def data_preparation(self, file_paths):
        try:
            response = None  # Initialize response variable

            for file_path in file_paths:
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

    #     return response
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
        try:
            for file_path in file_paths:
                # Update the path to the CSV file inside the Docker container
                container_input_path = f"/app/{file_path}"
                subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}:/app", "data-preprocessors-image", "python", "data_preparation.py", container_input_path], check=True)

            response = "Data preprocessing completed successfully!"
        except subprocess.CalledProcessError as e:
            response = f"Error during data preprocessing: {e}"

        return response


    # def post_process_data(self):
    #     # Load the Airbnb dataset for demonstration
    #     df = pd.read_csv(self.airbnb_dataset_path)

    #     # Generate a simple visualization (histogram)
    #     plt.hist(df['review_scores_rating'].dropna(), bins=20, color='blue', alpha=0.7)
    #     plt.xlabel('Review Scores Rating')
    #     plt.ylabel('Frequency')
    #     plt.title('Distribution of Review Scores Rating')
    #     plt.savefig('review_scores_histogram.png')

    #     # Save the visualization as an image
    #     print("Visualization generated: review_scores_histogram.png")

    #     # Generate a simple report
    #     report_file = 'data_processing_report.txt'
    #     with open(report_file, 'w') as report:
    #         report.write("Data Processing Report\n\n")
    #         report.write(f"Total records in the dataset: {len(df)}\n")
    #         report.write(f"Avg review scores rating: {df['review_scores_rating'].mean()}\n")

    #     print(f"Report generated: {report_file}")

    def handle_client(self, client_socket):
        # Receive the command from the client
        command = client_socket.recv(1024).decode()

        if command == "data_preparation":
            # Update the file paths as needed
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.data_preparation(file_paths)
        elif command == "data_prep":
            response = self.data_prep()
        elif command == "preprocessing_data":
            # Receive additional arguments (file_paths)
            file_paths = client_socket.recv(1024).decode().split(',')
            response = self.preprocessing_data(file_paths)
        # Add a case for "preprocessing_data" here
        elif command == "preprocessing_data":
            # Receive additional arguments (file_paths)
            file_paths = client_socket.recv(1024).decode().split(',')
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

        client_socket.send(response.encode())
        client_socket.close()


    def containerize_elasticsearch(self):
        # Assume Elasticsearch is installed and running locally
        # Stop the Elasticsearch service if it's running
        subprocess.run(["docker", "stop", "elasticsearch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Remove the existing Elasticsearch container if it exists
        subprocess.run(["docker", "rm", "elasticsearch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run Elasticsearch in a Docker container
        subprocess.run(["docker", "run", "-d", "--name", "elasticsearch", "-p", "9200:9200", "elasticsearch:8.11.1"])

    def run_docker_container(self):
        # Stop the existing Docker container if it's running
        subprocess.run(["docker", "stop", "nginx-container"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Remove the existing Docker container if it exists
        subprocess.run(["docker", "rm", "nginx-container"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run a sample Docker container (nginx in this case)
        subprocess.run(["docker", "run", "-d", "--name", "nginx-container", "-p", "8080:80", "nginx"])

    def stop_docker_container(self):
        # Stop the sample Docker container
        subprocess.run(["docker", "stop", "nginx-container"])

    def deploy_kubernetes(self):
        # Deploy a sample Kubernetes deployment (nginx in this case)
        subprocess.run(["kubectl", "create", "deployment", "nginx-deployment", "--image=nginx"])

    def undeploy_kubernetes(self):
        # Undeploy the sample Kubernetes deployment
        subprocess.run(["kubectl", "delete", "deployment", "nginx-deployment"])

def run_server():
    HOST = "127.0.0.1"
    PORT = 65432

    server = Server(HOST, PORT)
    server.start()

if __name__ == "__main__":
    run_server()
