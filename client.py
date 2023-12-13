import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_command(self, command):
        c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c_socket.connect((self.host, self.port))

        try:
            c_socket.send(command.encode())
            response = c_socket.recv(1024).decode()
            print(response)
        finally:
            c_socket.close()
    
    # def preprocess_data(self):
    #     """
    #     Send the "preprocess_data" command to the server.
    #     """
    #     self.send_command("preprocess_data")

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
    # client.preprocess_data()

    # Command to post-process data on the server
    # client.send_command("post_process_data")

if __name__ == "__main__":
    run_client()
