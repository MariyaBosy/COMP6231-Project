import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_command(self, command, *args):
        c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c_socket.connect((self.host, self.port))

        try:
            # Send the command
            c_socket.send(command.encode())

            # Send additional arguments
            for arg in args:
                # Convert list to a string and then send
                arg_str = ','.join(arg)
                c_socket.send(arg_str.encode())

            # Receive and print the response
            response = c_socket.recv(1024).decode()
            print(response)
        finally:
            c_socket.close()

    
    def data_preparation(self):
        """
        Send the "data_preparation" command to the server.
        """
        self.send_command("data_preparation")

    def data_prep(self):
        """
        Send the "data_preparation" command to the server.
        """
        self.send_command("data_prep")

    # def preprocessing_data(self):
    #     """
    #     Send the "preprocessing_data" command to the server.
    #     """
    #     self.send_command("preprocessing_data")


def run_client():
    HOST = "127.0.0.1"
    PORT = 65432

    client = Client(HOST, PORT)

    # Command to containerize Elasticsearch
    client.send_command("containerize_elasticsearch")

    # Command to run a Docker container
    client.send_command("run_docker_container")

    # Command to stop a Docker container
    client.send_command("stop_docker_container")

    # Command to deploy a Kubernetes deployment
    client.send_command("deploy_kubernetes")

    # Command to undeploy a Kubernetes deployment
    client.send_command("undeploy_kubernetes")

    # Command to preprocess data
    client.data_preparation()

    client.data_prep()

    # Command to preprocess data
    # client.preprocessing_data()
    file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
    client.send_command("preprocessing_data", file_paths)

    # Command to post-process data on the server
    # client.send_command("post_process_data")

if __name__ == "__main__":
    run_client()


