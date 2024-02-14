import json
import logging
import os
import random
import socket
import threading
import time
import config


class Bootstrap:

    def __init__(self, port):
        self.host = config.IP
        self.port = port
        self.client_list = []
        self.authentication_nodes = []          # List to store Authentication nodes
        self.auth_node_index = 0                # Index to keep track of the next least loaded auth node
        self.FileDistribution_node = []         # List to store File Distribution nodes
        self.FileDistribution_node_index = 0    # Index to keep track of the next least loaded FDN node
        self.control_node = []
        self.control_node_count = 0
        self.connected_client_count = 0         # Track the number of connected clients
        self.auth_node_load = {}
        self.fdn_node_load = {}
        self.auth_node_spawned = False          # Flag to prevent multiple spawns of auth nodes
        self.fdn_node_spawned = False           # Flag to prevent multiple spawns of FDN nodes
        self.fdn_zero_load_count = {}           # Tracks zero load count for each FDN

    def start_server(self):
        """Start the server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Server started, listening on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

    def handle_connection(self, client_socket, addr):
        """Handle a connection from a node."""
        try:
            while True:  # Keep the connection open to handle multiple messages
                data = client_socket.recv(1024).decode('utf-8')
                if not data:
                    break  # No more data, close the connection
                if "request_fdn_info" in data:
                    self.send_fdn_info(client_socket)
                else:
                    message_data = json.loads(data)
                    self.process_message(message_data, client_socket)
        except Exception as e:
            logging.error(f"Error handling node: {e}")
        finally:
            client_socket.close()  # Close the connection

    def process_message(self, message_data, client_socket):
        """Process a message from a node."""
        try:
            if message_data.get("message") == "Shutdown":
                self.handle_node_shutdown(message_data)
            elif message_data.get("message") == "LoginData":
                self.update_login_data(message_data.get("data"))
            elif message_data.get("message") == "Auth Heartbeat":
                self.handle_auth_heartbeat(message_data)
            elif message_data.get("message") == "FDN Heartbeat":
                self.handle_fdn_heartbeat(message_data)
            else:
                self.handle_node_type(message_data, client_socket)
        except json.JSONDecodeError:
            logging.error("Error decoding JSON data")

    def handle_node_shutdown(self, node_info):
        """Handle a node shutdown."""
        node_address = (node_info['ip'], node_info['port'])
        # Remove node from the lists
        if node_address in self.authentication_nodes:
            self.authentication_nodes.remove(node_address)
            self.auth_node_load.pop(node_address, None)

        if node_address in self.FileDistribution_node:
            self.FileDistribution_node.remove(node_address)
            self.fdn_node_load.pop(node_address, None)

        print(f"Node {node_address} shutdown and removed from Bootstrap.")

    def handle_auth_heartbeat(self, heartbeat_data):
        """Handle a heartbeat from an Authentication node."""
        try:
            # Extract the node's address from the heartbeat data
            node_address = (heartbeat_data['ip'], heartbeat_data['port'])
            new_load = heartbeat_data.get('client_count', 0)

            # If the node is not already known, add it to the list of authentication nodes
            if node_address not in self.authentication_nodes:
                self.authentication_nodes.append(node_address)
                print(f"New Auth node registered: {node_address}")

            # Update the load of the node
            self.auth_node_load[node_address] = new_load
            print(f"Updated Auth node load: {node_address} -> {new_load}")

            # Check if the load is high and a new node needs to be spawned
            if new_load >= 2 and not self.auth_node_spawned:
                # Only spawn a new node if there are control nodes available
                if len(self.authentication_nodes) < len(self.control_node):
                    self.spawn_new_auth_node()
                    self.auth_node_spawned = True
                    print(f"Spawning new Auth node due to high load on {node_address}")

        except Exception as e:
            print(f"Exception in handle_auth_heartbeat: {e}")

    def handle_fdn_heartbeat(self, heartbeat_data):
        """Handle a heartbeat from a File Distribution node."""
        try:
            # Extract the node's address from the heartbeat data
            node_address = (heartbeat_data['ip'], heartbeat_data['port'])
            new_load = heartbeat_data.get('client_count', 0)

            # If the node is not already known, add it to the list of FDN nodes
            if node_address not in self.FileDistribution_node:
                self.FileDistribution_node.append(node_address)
                print(f"New FDN node registered: {node_address}")

            # Update the load of the node
            self.fdn_node_load[node_address] = new_load
            print(f"Updated FDN node load: {node_address} -> {new_load}")

            # Check if the load is high and a new node needs to be spawned
            if new_load >= 2 and not self.fdn_node_spawned:
                # Only spawn a new node if there are control nodes available
                if len(self.FileDistribution_node) < len(self.control_node):
                    self.spawn_new_fdn_node()
                    self.fdn_node_spawned = True
                    print(f"Spawning new FDN node due to high load on {node_address}")

        except Exception as e:
            print(f"Exception in handle_fdn_heartbeat: {e}")

    def spawn_new_fdn_node(self):
        """Spawn a new File Distribution node."""
        available_control_nodes = [node for node in self.control_node if node not in self.FileDistribution_node]
        if available_control_nodes:
            selected_control_node = random.choice(available_control_nodes)
            self.send_command_to_control_node(selected_control_node[0], selected_control_node[1],
                                              "start_file_distribution")
            print(f"Spawning new FDN node on control node: {selected_control_node}")
        else:
            print("No available control nodes to spawn new FDN service")

    def send_fdn_info(self, client_socket):
        """Send the address of a File Distribution node to a client."""
        least_loaded_fdn = self.get_least_loaded_fdn_node()
        if least_loaded_fdn:
            fdn_info = json.dumps({
                "ip": least_loaded_fdn[0],
                "port": least_loaded_fdn[1]
            })
            client_socket.sendall(fdn_info.encode('utf-8'))
            print(f"Sent least loaded File Distribution Node info: {fdn_info}")
        else:
            print("No File Distribution nodes available")
            client_socket.sendall("Error: No File Distribution nodes available".encode('utf-8'))

    def get_least_loaded_fdn_node(self):
        """Return the address of the least loaded File Distribution node."""
        if not self.fdn_node_load:
            return None

        min_load = min(self.fdn_node_load.values())
        min_load_nodes = [node for node, load in self.fdn_node_load.items() if load == min_load]

        if len(min_load_nodes) == 1:
            return min_load_nodes[0]

        random_index = int(time.time()) % len(min_load_nodes)
        return min_load_nodes[random_index]

    def spawn_new_auth_node(self):
        """Spawn a new Authentication node."""
        available_control_nodes = [node for node in self.control_node if node not in self.authentication_nodes]
        if available_control_nodes:
            selected_control_node = random.choice(available_control_nodes)
            self.send_command_to_control_node(selected_control_node[0], selected_control_node[1],
                                              "start_authentication")
            print(f"Spawning new Auth node on control node: {selected_control_node}")
        else:
            print("No available control nodes to spawn new Auth service")

    def update_login_data(self, login_data):
        print(f"Received login data: {login_data}")
        """Update the login data."""
        try:
            # Read existing login data
            existing_logins = {}
            try:
                with open("login.txt", 'r') as file:
                    for line in file:
                        # Skip empty lines or lines that don't contain ': '
                        if ': ' not in line:
                            continue
                        username, password = line.strip().split(': ')
                        existing_logins[username] = password
                        print(f"Existing login: {username} -> {password}")
            except FileNotFoundError:
                print("Login file not found, creating a new one.")

            # Process new login data
            with open("login.txt", "w") as file:  # Use "w" to overwrite and update the file
                for username, password in login_data.items():
                    if username in existing_logins and existing_logins[username] != password:
                        print(f"Invalid login attempt for user {username}.")
                        continue  # Skip updating this user
                    # Write/Update user
                    file.write(f"{username}: {password}\n")
                    if username in existing_logins:
                        print(f"User {username} successfully logged in.")
                    else:
                        print(f"New user {username} registered.")

                print("Updated login data in login.txt")

        except Exception as e:
            print(f"Error processing login data: {e}")

    def handle_node_type(self, node_info, client_socket):
        """Handle a node based on its type."""
        node_type = node_info['name']
        # Logic for handling different node types
        if node_type == "Authentication":
            self.handle_authentication_node(client_socket, node_info)
        elif node_type == "FileDistribution":
            self.handle_file_distribution_node(client_socket, node_info)
        elif node_type == "Client":
            self.handle_client(client_socket, node_info)
        elif node_type == "node":
            self.handle_node(client_socket, node_info)

    def handle_client(self, client_socket, node_info):
        """Handle connection from a client."""
        print(f"Client: {node_info}")
        addr = (node_info['ip'], node_info['port'])
        if addr not in self.client_list:
            self.client_list.append(addr)

        # Send the client a message
        client_socket.sendall("Hello Client".encode('utf-8'))

        # get response
        response = client_socket.recv(1024).decode()
        print(f"Received response: {response}")

        if response == "auth":
            try:
                # Get the next least loaded auth node
                least_loaded_node = self.get_next_least_loaded_auth_node()
                if least_loaded_node:
                    ip, port = least_loaded_node
                    print(f"Redirecting client to Auth node: {ip}:{port}")
                    client_socket.sendall(ip.encode('utf-8'))
                    client_socket.sendall(port.to_bytes(4, byteorder='big'))
                else:
                    print("No Authentication nodes available")
                    client_socket.sendall("Error: No authentication nodes available".encode('utf-8'))
            except Exception as e:
                print(f"Error in handling client auth request: {e}")

    # New method to get the next least loaded auth node
    def get_next_least_loaded_auth_node(self):
        """Return the address of the next least loaded Authentication node."""
        if not self.auth_node_load:
            return None

        min_load = min(self.auth_node_load.values())
        min_load_nodes = [node for node, load in self.auth_node_load.items() if load == min_load]

        if not min_load_nodes:
            return None

        selected_node = min_load_nodes[self.auth_node_index % len(min_load_nodes)]
        self.auth_node_index = (self.auth_node_index + 1) % len(min_load_nodes)
        return selected_node

    def handle_authentication_node(self, client_socket, node_info):
        """Handle connection from an Authentication node."""
        addr = (node_info['ip'], node_info['port'])
        if addr not in self.authentication_nodes:
            self.authentication_nodes.append(addr)
        print(f"Authentication Node registered: {addr}")
        # Initial load for this node is 0
        self.auth_node_load[addr] = 0

        login_file = "login.txt"
        with open(login_file, 'r') as file:
            file_data = file.read()
        client_socket.sendall(file_data.encode('utf-8'))

    def get_auth_node_address(self):
        """Get the address of an Authentication node."""
        if self.authentication_nodes:
            # Here you can implement your logic to select the appropriate Authentication node
            # For simplicity, we're just returning the first node in the list
            return self.authentication_nodes[0]
        return None

    def handle_file_distribution_node(self, client_socket, node_info):
        """Handle connection from a File Distribution node."""
        addr = (node_info['ip'], node_info['port'])

        if addr not in self.FileDistribution_node:
            self.FileDistribution_node.append(addr)
            print(f"File Distribution Node registered: {addr}")
            # Initialize the load for this node
            self.fdn_node_load[addr] = 0
        else:
            print(f"File Distribution Node already registered: {addr}")

        # Read files from the 'songs' directory
        songs_directory = 'Songs'
        music_files = [f for f in os.listdir(songs_directory) if f.endswith('.mp3')]

        client_socket.sendall(len(music_files).to_bytes(4, byteorder='big'))  # send count of files

        for music_file in music_files:
            file_path = os.path.join(songs_directory, music_file)
            with open(file_path, 'rb') as file:
                music_data = file.read()

            client_socket.sendall(len(music_data).to_bytes(8, byteorder='big'))  # send file size
            client_socket.sendall(music_data)  # send file data

        # send the names of the files
        client_socket.sendall(json.dumps(music_files).encode('utf-8'))

    def handle_node(self, client_socket, node_info):
        """Handle connection from a control node."""
        if "node" in node_info['name']:
            # Increment the counter
            self.control_node_count += 1

            # Store the control node's address
            self.control_node.append((node_info['ip'], node_info['port']))
            print(f"Control Node registered: {node_info['ip']}:{node_info['port']}")

            # Assign a role based on the order
            if self.control_node_count == 1:
                self.assign_role_to_control_node(node_info['ip'], node_info['port'], "start_authentication")
            elif self.control_node_count == 2:
                self.assign_role_to_control_node(node_info['ip'], node_info['port'], "start_file_distribution")
            elif self.control_node_count == 3:
                self.assign_role_to_control_node(node_info['ip'], node_info['port'], "start_client")

    def assign_role_to_control_node(self, ip, port, role_command):
        """Send a role command to the control node."""
        self.send_command_to_control_node(ip, port, role_command)

    def send_command_to_control_node(self, ip, port, command):
        """Send a command to a specific control node."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((ip, port))
                sock.sendall(command.encode('utf-8'))
                print(f"Sent command '{command}' to Control Node at {ip}:{port}")
        except Exception as e:
            print(f"Error sending command to Control Node: {e}")


if __name__ == "__main__":
    port = 50000
    BootstrapNode = Bootstrap(port)
    BootstrapNode.start_server()
