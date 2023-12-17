import argparse
import logging
import socket
import threading
import os
import subprocess
from elasticsearch import Elasticsearch
import pandas as pd
import socketserver
from concurrent.futures import ThreadPoolExecutor
from data_preparation import load_data, clean_data, transform_data, feature_engineering, scale_data, save_data
from flask import Flask
from load_balancer import LoadBalancer
from waitress import serve
import json
from elasticsearch.exceptions import RequestError

app = Flask(__name__)

# Load datasets
ratings = pd.read_csv("./files/airbnb_ratings_new_prepared.csv")
reviews = pd.read_csv("./files/airbnb-reviews.csv")
sample_data = pd.read_csv("./files/airbnb_sample_prepared.csv")
la_listings = pd.read_csv("./files/LA_Listings_prepared.csv")
ny_listings = pd.read_csv("./files/NY_Listings_prepared.csv")

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        command = self.request.recv(1024).decode()
        if command == "data_preparation":
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv", "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.server.data_preparation(file_paths)
        elif command == "data_prep":
            response = self.server.data_prep()
        elif command == "preprocessing_data":
            file_paths = self.request.recv(1024).decode().split(',')
            response = self.server.preprocessing_data(file_paths)
        elif command == "containerize_elasticsearch":
            self.server.containerize_elasticsearch()
            response = "Elasticsearch containerized successfully!"
        elif command == "run_docker_container":
            self.server.run_docker_container()
            response = "Docker container started successfully!"
        elif command == "stop_docker_container":
            self.server.stop_docker_container()
            response = "Docker container stopped successfully!"
        elif command == "deploy_kubernetes":
            self.server.deploy_kubernetes()
            response = "Kubernetes deployment created successfully!"
        elif command == "undeploy_kubernetes":
            self.server.undeploy_kubernetes()
            response = "Kubernetes deployment deleted successfully!"
        else:
            response = "Invalid command"

        self.request.sendall(response.encode())

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

class Server:
    load_balancer = None

    def __init__(self, host, port):
        self.host = host
        self.port = port

        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.executor = ThreadPoolExecutor(max_workers=10)

    def start(self):
        server = ThreadedTCPServer((self.host, self.port), ThreadedTCPRequestHandler)
        server.data_preparation = self.data_preparation
        server.data_prep = self.data_prep
        server.preprocessing_data = self.preprocessing_data
        server.containerize_elasticsearch = self.containerize_elasticsearch
        server.run_docker_container = self.run_docker_container
        server.stop_docker_container = self.stop_docker_container
        server.deploy_kubernetes = self.deploy_kubernetes
        server.undeploy_kubernetes = self.undeploy_kubernetes

    @classmethod
    def set_load_balancer(cls, load_balancer):
        cls.load_balancer = load_balancer

    def register_with_load_balancer(self):
        self.load_balancer.register_server(self.host, self.port)

    def start_server(self):
        self.register_with_load_balancer()

        self.logger.info(f"Starting Flask server on {self.host}:{self.port}")
        print("-------------------------------")

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)

        # with server_socket:
        #     server_thread = self.executor.submit(server_socket.serve_forever)
        #     server_thread.result()

        while True:
            client_socket, addr = server_socket.accept()
            print("-------------------------------")
            self.logger.info(f"Accepted connection from {addr}")

            
            client_identifier = addr

            client_handler = threading.Thread(target=self.handle_client, args=(client_socket, client_identifier))
            client_handler.start()
        

    def start_flask(self):
        # print(f"Starting Flask server on {self.host}:{self.port}")
        serve(app, host=self.host, port=int(self.port))

    
    def handle_client(self, client_socket, client_identifier):
        # Receive the command from the client
        command = client_socket.recv(1024).decode()

        # Split the command and arguments
        command, *args = command.split(',')

        response = None  
        
        # Use the client identifier to determine the target host consistently
        target_host = self.load_balancer.get_next_server(client_identifier)

        self.logger.info(f"Received command: {command}")
        print("-------------------------------")

        if command == "data_preparation":
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.data_preparation(file_paths)
        elif command == "data_prep":
            response = self.data_prep()
        elif command == "preprocessing_data":
            file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
            response = self.preprocessing_data(file_paths)
        elif command == "containerize_elasticsearch":
            self.containerize_elasticsearch()
            print("-------------------------------")
            response = "Elasticsearch containerized successfully!"
            print("-------------------------------")
        elif command == "run_docker_container":
            self.run_docker_container()
            print("-------------------------------")
            response = "Docker container started successfully!"
            print("-------------------------------")
        elif command == "stop_docker_container":
            self.stop_docker_container()
            print("-------------------------------")
            response = "Docker container stopped successfully!"
            print("-------------------------------")
        elif command == "deploy_kubernetes":
            self.deploy_kubernetes()
            print("-------------------------------")
            response = "Kubernetes deployment created successfully!"
            print("-------------------------------")
        elif command == "undeploy_kubernetes":
            self.undeploy_kubernetes()
            print("-------------------------------")
            response = "Kubernetes deployment deleted successfully!"
            print("-------------------------------")
        elif command == "question_1":
            response = self.question_1(la_listings, ny_listings)
        elif command == "question_2":
            response = self.question_2(la_listings, ny_listings)
        elif command == "question_3":
            response = self.question_3(la_listings)
        elif command == "question_4":
            response = self.question_4(la_listings, ny_listings)
        elif command == "question_5":
            response = self.question_5(ratings)
        else:
            response = "Invalid command"
        
        client_socket.send(response.encode())

        # Close the sockets
        client_socket.close()

    
    def setup_elasticsearch(self):
        # Connect to the Elasticsearch cluster
        es = Elasticsearch(['http://localhost:9200'], verify_certs=False, ssl_version='PROTOCOL_TLSv1')

        try:
            # Example operation: Create an index
            index_name = 'my_test_index'
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                })
                return "Elasticsearch index setup completed."
            else:
                return f"Elasticsearch index '{index_name}' already exists."

        except RequestError as e:
            # Handle specific request errors, e.g., index already exists
            return f"Error setting up Elasticsearch: {str(e)}"
        except Exception as e:
            # Handle other exceptions or log errors
            return f"Error setting up Elasticsearch: {str(e)}"


    def get_index_settings(self, index_name):
        es = Elasticsearch(['http://localhost:9200'], verify_certs=False, ssl_version='PROTOCOL_TLSv1')  # Adjust the host if necessary
        try:
            settings = es.indices.get_settings(index=index_name)
            print(settings)
            #print(f'Type of settings: {type(settings)}')
            relevant_info = {
                'number_of_shards': settings['my_test_index']['settings']['index']['number_of_shards'],
                'number_of_replicas': settings['my_test_index']['settings']['index']['number_of_replicas']
            }

            # Convert the relevant information to a JSON string
            response_json = json.dumps(relevant_info)
            return response_json
        except Exception as e:
            return str(e)
   


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
                        print("-------------------------------")
                        print(f"Data preparation completed for {file_path}. Prepared data saved to {output_file}")
                        print("-------------------------------")
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
        df = load_data("./files/airbnb-reviews.csv")

        # Data preparation steps
        df = clean_data(df)
        df = transform_data(df)
        df = feature_engineering(df)
        df = scale_data(df)

        # Save the prepared data to a new file
        save_data(df, "./files/airbnb-reviews_prepared.csv")
        print("-------------------------------")
        response = "Data preparation for 1GB file completed successfully!"
        print("-------------------------------")
        return response

    def preprocessing_data(self, file_paths):
        print("Entering preprocessing method")
        try:
            for file_path in file_paths:
                # Update the path to the CSV file inside the Docker container
                container_input_path = f"/app/{file_path}"
                subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}:/app", "data-preprocessors-image", "python", "data_preparation.py", container_input_path], check=True)
            print("-------------------------------")
            response = "Data preprocessing completed successfully!"
            print("-------------------------------")
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
    
    '''
    Question: 1
    Find the average rating for each host in Los Angeles and New York, considering 
    only the listings with more than 10 reviews. Display the host ID, average rating, 
    and the number of reviews for each host.
    '''
    def question_1(self, la_listings, ny_listings):
        combined_listings = pd.concat([la_listings, ny_listings])
        filtered_listings = combined_listings[combined_listings['number of reviews'] >= 90]
        grouped_data = filtered_listings.groupby(['host id']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
        result = grouped_data[grouped_data['review scores rating'] > 70]
        result.columns = ['host id', 'average_rating', 'num_listings']
        return str(result)

    '''
    Question: 2
    Identify the top 5 neighborhoods in Los Angeles and New York with the highest 
    average ratings. Include the neighborhood name, the average rating, and the 
    total number of listings in each neighborhood.
    '''
    def question_2(self, la_listings, ny_listings):
        combined_listings = pd.concat([la_listings, ny_listings])
        grouped_data = combined_listings.groupby(['neighbourhood cleansed']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
        result = grouped_data.sort_values(by=['review scores rating', 'listing id'], ascending=False).head(10)
        result.columns = ['neighbourhood', 'average_rating', 'num_listings']
        print("-------------------------------")
        return str(result)

    '''
    Question: 3
    Determine the correlation between the quantity of the Airbnb amenities (number of amenities) 
    and the price. Consider only listings in Los Angeles.
    '''
    def question_3(self, la_listings):
        la_listings['total_amenities'] = la_listings['amenities'].apply(lambda x: len(str(x).split(";")))
        correlation = la_listings[['total_amenities', 'price']].corr()['price']['total_amenities']
        print("-------------------------------")
        return str(correlation)

    '''
    Question: 4
    Find hosts who have listings in both Los Angeles and New York. 
    Display the host ID and the number of listings they have in each city.
    '''
    def question_4(self, la_listings, ny_listings):
        la_host_listings = la_listings[['host id']]
        result = pd.merge(la_host_listings, ny_listings, on='host id')
        result = result.groupby(['host id']).agg({'listing id': 'count'}).reset_index()
        result.columns = ['host id', 'num_listings_LA_NY']
        print("-------------------------------")
        return str(result)

    '''
    Question: 5
    Find the city which has the most AirBNB number of listing. 
    Display the city and the total number of listing.
    '''
    def question_5(self, ratings):
        result = ratings.groupby(['city']).agg({'listing id': 'count'}).reset_index()
        result = result[result['listing id'] == result['listing id'].max()]
        print("-------------------------------")
        return str(result)


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
