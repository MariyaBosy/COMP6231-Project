import threading
import time
import socket


class LoadBalancer:
    def __init__(self, servers):
        self.servers = servers
        self.current_index = 0
        self.lock = threading.Lock()
        
        ## my code
        # self.server_tasks = {}  # Dictionary to store tasks assigned to each server

    def register_server(self, host, port):
        with self.lock:
            self.servers.append((host, port))
            
            ## my code
            # self.server_tasks[(host, port)] = []  # Initialize tasks for the new server
            
        # print(f"Server registered: {host}:{port}")

    def get_next_server(self, client_identifier):
        with self.lock:
            # Implement your logic to select the server based on client_identifier
            server = self.servers[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.servers)
            
            ## my code
            # self.server_tasks[server].append(client_identifier)  # Assign task to the selected server
            
        return server
    
    # ## my code
    # def health_check(self):
    #     while True:
    #         time.sleep(10)  # Check server health every 10 seconds
    #         with self.lock:
    #             for server in list(self.server_tasks.keys()):
    #                 print(f"host {server[0]}, port {server[1]}")
    #             for server in list(self.server_tasks.keys()):
    #                 if not self.is_server_alive(server[0], server[1]):
    #                     print(f"Server {server} is not responding. Removing from the server list.")
    #                     self.redistribute_tasks(server)
    #                     del self.servers[self.servers.index(server)]
    #                     del self.server_tasks[server]
    #                     self.current_index = 0
    
    # ## my code
    # def remove_server(self, server):
    #     with self.lock:
    #         if server in self.servers:
    #             self.servers.remove(server)
    #             del self.server_tasks[server]
    
    # ## my code
    # def redistribute_tasks(self, failed_server):
    #     with self.lock:
    #         tasks_to_redistribute = self.server_tasks[failed_server]
    #         print(f"tasks_to_redistribute {tasks_to_redistribute}")
    #         del self.server_tasks[failed_server]  # Remove the failed server
    #         # Implement logic to choose a new server for the tasks
    #         new_server = self.servers[self.current_index]
    #         self.current_index = (self.current_index + 1) % len(self.servers)
    #         self.server_tasks[new_server].extend(tasks_to_redistribute)  # Assign tasks to the new server
    #     return new_server
    
    # ## my code
    # def is_server_alive(self, host, port):
    #     # Implement a simple check to see if the server is alive (e.g., attempt to connect)
    #     try:
    #         with socket.create_connection((host, port), timeout=1):
    #             pass
    #         return True
    #     except (socket.timeout, ConnectionRefusedError):
    #         return False
