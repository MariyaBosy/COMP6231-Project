import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_command(self, command, *args):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c_socket:
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

def run_client():
    HOST = "127.0.0.1"
    PORT = 65432

    client = Client(HOST, PORT)

    # Example commands
    client.send_command("data_preparation")
    client.send_command("data_prep")

if __name__ == "__main__":
    run_client()