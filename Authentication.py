import logging
import socket
import json
import subprocess
import sys
import threading
import time
import config


class Authentication:

    def __init__(self, name):
        self.host = self.find_ip_starting_with_10()
        self.name = name
        self.port = self.find_open_port()
        self.stored_tokens = {}                # Dictionary of tokens
        self.connected_clients = []            # List of connected clients
        self.heartbeat_zero_client_count = {}  # Dictionary of SubAuthentication nodes with 0 clients
        self.connected_sub_auth = {}           # Dictionary of connected SubAuthentication nodes
        self.client_logins_dict = {}           # Dictionary of client logins
        self.start_sub_auth()
        self.sub_nodes_count = 0               # Initialize sub-nodes count
        self.heartbeat_thread_started = False  # Flag to track if heartbeat thread has started
        self.heartbeat_interval = 10           # Heartbeat every 10 seconds

    def start_sub_auth(self):
        """Start the SubAuthentication nodes."""
        auth_details = [self.host, str(self.port)]
        subprocess.Popen([sys.executable, "subAuth.py"] + auth_details,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)
        time.sleep(0.1)
        subprocess.Popen([sys.executable, "subAuth.py"] + auth_details,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)

    def send_heartbeat(self):
        """Send heartbeat messages to the Bootstrap server."""
        while True:
            time.sleep(self.heartbeat_interval)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((config.IP, config.PORT))  # Bootstrap server IP and port
                    heartbeat_data = {
                        "message": "Auth Heartbeat",
                        "client_count": len(self.connected_clients),
                        "ip": self.host,
                        "port": self.port
                    }
                    heartbeat_data_json = json.dumps(heartbeat_data)
                    sock.sendall(heartbeat_data_json.encode('utf-8'))
                    print(f"Sent heartbeat to Bootstrap server: {heartbeat_data_json}")
            except Exception as e:
                print(f"Error sending heartbeat to Bootstrap server: {e}")

    def find_ip_starting_with_10(self):
        """Find an IP address that starts with '10' using socket.gethostbyname_ex."""
        node_name = socket.gethostname()
        try:
            _, _, ip_addresses = socket.gethostbyname_ex(node_name)
            for ip in ip_addresses:
                if ip.startswith('10'):
                    return ip
        except socket.error as err:
            logging.error(f"Error retrieving IP address: {err}")
        raise RuntimeError("No IP address starting with '10' found on any interface")

    def find_open_port(self):
        """Find an open port in the range 50001-50010."""
        port_range = range(50001, 50011)
        for port in port_range:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind((self.host, port))
                    # If bind is successful, the port is available
                    return port
            except socket.error:
                # This port is already in use, try the next one
                continue
        # No available port found
        raise RuntimeError("No available ports in the specified range")

    def connect_to_bootstrap(self, bootstrap_host, bootstrap_port):
        """Connect to the Bootstrap server and send the client information."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((bootstrap_host, bootstrap_port))

            # Prepare the client information as JSON
            client_info = {"name": self.name, "ip": self.host, "port": self.port}

            # Send the client information to the Bootstrap Server
            sock.sendall(json.dumps(client_info).encode('utf-8'))

            print(f"Connected to Bootstrap Server and sent info: {client_info}")

            # Receive client information from the Bootstrap Server
            client_logins_str = sock.recv(1024).decode()
            print(f"Received response: {client_logins_str}")

            # Parse the received string into a set
            self.client_logins_dict = self.parse_client_logins_to_dict(client_logins_str)

            # Start listening for connections
            self.listen_for_connections()

    def parse_client_logins_to_dict(self, logins_str):
        """Parse the client logins string into a dictionary."""
        logins_dict = {}
        for login_pair in logins_str.strip().split('\n'):
            username, password = login_pair.split(': ')
            logins_dict[username.strip()] = password.strip()
        return logins_dict

    def listen_for_connections(self):
        """Listen for connections from clients and SubAuthentication nodes."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, self.port))
            sock.listen(5)
            print(f"Listening for connections on {self.host}:{self.port}...")
            while True:
                conn, addr = sock.accept()
                # Start a new thread to handle the connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        """Handle a connection from a client or SubAuthentication node."""
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break  # Exit loop if no data is received
                try:
                    client_info = json.loads(data)
                    self.process_client_info(client_info, addr, conn)
                except json.JSONDecodeError:
                    print("Received non-JSON data")

        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def process_client_info(self, client_info, addr, conn):
        """Process the client information received from a client or SubAuthentication node."""
        if client_info.get("message") == "SubAuth Heartbeat":
            sub_auth_addr = (client_info.get("ip"), client_info.get("port"))
            client_count = client_info.get("client_count", 0)
            # Increment or reset the heartbeat count
            if client_count == 0:
                self.heartbeat_zero_client_count[sub_auth_addr] = self.heartbeat_zero_client_count.get(sub_auth_addr,
                                                                                                       0) + 1
                if self.heartbeat_zero_client_count[sub_auth_addr] >= 15:
                    self.send_die_message(sub_auth_addr)
                    self.heartbeat_zero_client_count[sub_auth_addr] = 0
            else:
                self.heartbeat_zero_client_count[sub_auth_addr] = 0

            self.connected_sub_auth[sub_auth_addr] = client_count
            print(f"Updated SubAuth node {sub_auth_addr} with {client_count} clients")
            return

        node_name = client_info.get("name")
        if node_name == "Client":
            self.register_client(client_info)
            self.handle_login_process(client_info, conn)
        elif node_name == "SubAuthentication":
            self.register_sub_auth(client_info, addr)

        if node_name in ["Client", "SubAuthentication"]:
            return

    def send_die_message(self, sub_auth_addr):
        """Send a 'Die' message to the SubAuthentication server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(sub_auth_addr)
                sock.sendall("Die".encode('utf-8'))
                print(f"Sent 'Die' message to SubAuth node {sub_auth_addr}")
                # Remove the SubAuthentication node from the connected nodes
                del self.connected_sub_auth[sub_auth_addr]
                del self.heartbeat_zero_client_count[sub_auth_addr]
        except Exception as e:
            print(f"Error sending 'Die' message to SubAuth node {sub_auth_addr}: {e}")

    def register_client(self, client_info):
        """Register a client with the Authentication server."""
        client_addr = (client_info.get("ip"), client_info.get("port"))
        if client_addr not in self.connected_clients:
            self.connected_clients.append(client_addr)
            print(f"Clients connected: {client_addr}")

    def register_sub_auth(self, client_info, addr):
        """Register a SubAuthentication node with the Authentication server."""
        sub_auth_addr = (client_info.get("ip"), client_info.get("port"))
        if sub_auth_addr not in self.connected_sub_auth:
            self.connected_sub_auth[sub_auth_addr] = 0
            print(f"SubAuthentication node connected: {sub_auth_addr}")
            self.sub_nodes_count += 1  # Increment the count of connected sub-nodes

            # Check if both SubAuthentication nodes are connected
            if self.sub_nodes_count == 2:
                self.exchange_sub_auth_addresses()

        if self.sub_nodes_count >= 2 and not self.heartbeat_thread_started:
            threading.Thread(target=self.send_heartbeat, daemon=True).start()
            self.heartbeat_thread_started = True
            print("Heartbeat thread started.")

    def exchange_sub_auth_addresses(self):
        """Exchange the addresses of the two SubAuthentication nodes."""
        sub_auth_addresses = list(self.connected_sub_auth.keys())
        for addr in sub_auth_addresses:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(addr)
                    # Send the other SubAuthentication node's address in "ip:port" format
                    other_addr = sub_auth_addresses[1] if addr == sub_auth_addresses[0] else sub_auth_addresses[0]
                    addr_str = f"SubAuth Address: {other_addr[0]}:{other_addr[1]}"
                    sock.sendall(addr_str.encode('utf-8'))
                    print(f"Sent address of SubAuth node {other_addr} to {addr}")
            except Exception as e:
                print(f"Error sending SubAuth address: {e}")

    def handle_login_process(self, client_info, conn):
        """Handle the login process for a client."""
        while True:
            conn.sendall("Username:".encode())
            username = conn.recv(1024).decode().strip()

            conn.sendall("Password:".encode())
            password = conn.recv(1024).decode().strip()

            print(f"Received login credentials: {username}:{password}")

            # Validate the login and check if it's a new signup
            is_signup, is_valid = self.validate_login(username, password)
            if is_valid:
                response = "Login successful." if not is_signup else "Signed up successfully."
                conn.sendall(response.encode())
                break
            else:
                response = "Incorrect password. Try again."
                conn.sendall(response.encode())

        if response == "Login successful." or response == "Signed up successfully.":
            self.send_sub_auth_info_to_client(conn)

    def validate_login(self, username, password):
        """Validate the login credentials and return if it's a signup."""
        if username in self.client_logins_dict:
            if self.client_logins_dict[username] == password:
                return False, True  # Not a signup, login successful
            else:
                return False, False  # Not a signup, incorrect password
        else:
            self.client_logins_dict[username] = password
            self.send_login_data_to_bootstrap()  # Send updated data to Bootstrap
            return True, True  # Is a signup and successful

    def send_sub_auth_info_to_client(self, conn):
        """Send the SubAuthentication node information to the client."""
        if self.connected_sub_auth:
            # Select the SubAuth node with the least clients
            sub_auth_info = min(self.connected_sub_auth, key=self.connected_sub_auth.get)
            sub_auth_data = json.dumps({
                "sub_auth_ip": sub_auth_info[0],
                "sub_auth_port": sub_auth_info[1]
            })
            conn.sendall(sub_auth_data.encode())
            print(f"Sent SubAuthentication node info to client: {sub_auth_data}")
        else:
            conn.sendall("No available SubAuthentication nodes".encode())

    def send_login_data_to_bootstrap(self):
        """Send updated login data to Bootstrap server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((config.IP, config.PORT))  # Bootstrap server IP and port
                login_data = {
                    "message": "LoginData",
                    "data": self.client_logins_dict
                }
                login_data_json = json.dumps(login_data)
                sock.sendall(login_data_json.encode('utf-8'))
                print(f"Sent updated login data to Bootstrap server {login_data_json}")
        except Exception as e:
            print(f"Error sending login data to Bootstrap server: {e}")


if __name__ == "__main__":
    client = Authentication("Authentication")
    client.connect_to_bootstrap(config.IP, config.PORT)
