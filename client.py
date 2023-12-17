import socket
import time

class Client:
    def __init__(self, host, port, inc):
        self.host = host
        self.port = port
        self.inc = []

    def send_command(self, command, *args):
        c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"Connecting to {self.host}:{self.port}")
            c_socket.connect((self.host, self.port))

            # Send the command
            print(f"Sending command...{command}")  # Add this line
            c_socket.send(command.encode())

            # Send additional arguments
            for arg in args:
                arg_str = ','.join(arg)
                print(arg_str)
                c_socket.send(arg_str.encode())

            # c_socket.settimeout(5)  # Set a timeout for receiving responses (e.g., 10 seconds)

            # Receive and print the response
            response = c_socket.recv(1024).decode()
            print(response)
            if response == "Server Shut down":
                self.inc.append(command)
        except ConnectionRefusedError:
            print("Connection to the server was refused.")
        # except socket.timeout:
        #     print("Socket timed out. No response received.")
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
    # Update the host and port for each server
    host1 = "127.0.0.1"
    port1 = 5001

    host2 = "127.0.0.1"
    port2 = 5002

    host3 = "127.0.0.1"
    port3 = 5003


    client1 = Client(host=host1, port=port1, inc=[])
    client2 = Client(host=host2, port=port2, inc=[])
    client3 = Client(host=host3, port=port3, inc=[])

    # Commands for each server
    client1.send_command("containerize_elasticsearch")
    client2.send_command("run_docker_container")
    client3.send_command("deploy_kubernetes")

    # # Command to stop a Docker container
    client2.send_command("stop_docker_container")

    # # Command to undeploy a Kubernetes deployment
    client3.send_command("undeploy_kubernetes")

    # Command to preprocess data
    client1.send_command("data_preparation")

    client1.data_prep()

    # Command to preprocess data
    # client.preprocessing_data()
    file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv"]
    client1.send_command("preprocessing_data")

    # Command to post-process data on the server
    # client.send_command("post_process_data")
    if (len(client1.inc) != 0):
        #print(client1.inc)
        for cmd in client1.inc:
            client2.send_command(cmd)

if __name__ == "__main__":
    run_client()


