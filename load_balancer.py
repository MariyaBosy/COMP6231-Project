import threading


class LoadBalancer:
    def __init__(self, servers):
        self.servers = servers
        self.current_index = 0
        self.lock = threading.Lock()

    def register_server(self, host, port):
        with self.lock:
            self.servers.append((host, port))
        print(f"Server registered: {host}:{port}")

    def get_next_server(self, client_identifier):
        with self.lock:
            # Implement your logic to select the server based on client_identifier
            server = self.servers[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.servers)
        return server
