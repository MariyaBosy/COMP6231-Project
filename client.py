import concurrent.futures
import socket
import time
import json


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        # self.inc = []

    def send_command(self, command, *args):
        c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            c_socket.connect((self.host, self.port))
            # print(f"Connecting to {self.host}:{self.port}")

            print(f"Sending command '{command}' in {self.host}:{self.port}")
            print("----------------------------------")
            c_socket.send(command.encode())

            for arg in args:
                arg_str = ','.join(arg)
                c_socket.send(arg_str.encode())

            response = c_socket.recv(1024).decode()
            if response.startswith('{') and response.endswith('}'):
                response_json = json.loads(response)
                print(response_json)
            # elif response == "Server Shut down":
            #     self.inc.append(command)
            else:
                # Handle the case where the response is not a JSON string
                print(response)        
        except ConnectionRefusedError:
            print("Connection to the server was refused.")
        finally:
            c_socket.close()

 
    def setup_elasticsearch(self):
        """
        Send command to server to set up Elasticsearch index.
        """
        self.send_command("setup_elasticsearch")

    def get_index_settings(self, index_name):
        command = f"get_index_settings|{index_name}"
        self.send_command(command)

   
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
  

def run_client():
    host1 = "127.0.0.1"
    port1 = 5001

    host2 = "127.0.0.1"
    port2 = 5002

    host3 = "127.0.0.1"
    port3 = 5003

    client1 = Client(host=host1, port=port1)
    client2 = Client(host=host2, port=port2)
    client3 = Client(host=host3, port=port3)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit commands to the executor
        futures = [
            # executor.submit(client3.send_command, "containerize_elasticsearch"),
            executor.submit(client2.send_command, "run_docker_container"),
            executor.submit(client3.send_command, "deploy_kubernetes"),
            executor.submit(client2.send_command, "stop_docker_container"),
            executor.submit(client3.send_command, "undeploy_kubernetes"),
            executor.submit(client1.send_command, "data_preparation"),
            executor.submit(client2.send_command, "data_prep"),
            executor.submit(client1.send_command, "search_hosts_by_rating"),
            executor.submit(client1.send_command, "preprocessing_data"),
            executor.submit(client1.send_command, "question_1"),
            executor.submit(client2.send_command, "question_2"),
            executor.submit(client3.send_command, "question_3"),
            executor.submit(client2.send_command, "question_4"),
            executor.submit(client3.send_command, "question_5"),
            executor.submit(client3.send_command, "setup_elasticsearch"),
            executor.submit(client3.get_index_settings, "my_test_index"),
        ]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)
    
    # if (len(client1.inc) != 0):
    #     #print(client1.inc)
    #     for cmd in client1.inc:
    #         client2.send_command(cmd)

if __name__ == "__main__":
    run_client()

