import json
import logging
import socket
import threading
import time
import sys
import random


class SubAuthentication:
    def __init__(self, name, sub_auth_ip, sub_auth_port):
        self.name = name
        self.host = self.find_ip_starting_with_10()
        self.port = self.find_open_port()
        self.auth_ip = sub_auth_ip
        self.auth_port = sub_auth_port
        self.heartbeat_interval = random.randint(5, 10)
        self.connected_clients = []
        self.running = True
        self.connected_sub_fd = []
        self.token_to_verify = []
        self.connect_to_auth()

    def connect_to_auth(self):
        """Connect to the Authentication server."""
        try:
            # Establish a persistent connection for heartbeat messages
            self.auth_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.auth_socket.connect((self.auth_ip, self.auth_port))
            print(f"Connected to Authentication server at {self.auth_ip}:{self.auth_port}")

            client_info = {"name": self.name, "ip": self.host, "port": self.port}
            print(f"Sending client info to Authentication server: {client_info}")
            self.auth_socket.sendall(json.dumps(client_info).encode('utf-8'))

            threading.Thread(target=self.send_delayed_heartbeat, daemon=True).start()

            self.listen_for_connections()

        except Exception as e:
            print(f"Error connecting to Authentication server: {e}")

    def send_delayed_heartbeat(self):
        time.sleep(3)
        print("Sending delayed heartbeat...")
        self.send_heartbeat()

    def listen_for_connections(self):
        # Listen for connections
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, self.port))
            sock.listen(5)
            print(f"Listening for connections on {self.host}:{self.port}...")
            while True:
                conn, addr = sock.accept()
                # Start a new thread to handle the connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        try:
            while True:
                data = conn.recv(1024).decode()

                # Handling other commands
                if data.startswith("SubAuth Address:"):
                    self.handle_other_subauth_address(data)
                elif data.startswith("Die"):
                    print("Received Die command. Shutting down... I don't want to die :( I'm too young to die :(")
                    self.shutdown()
                    break

                elif data:
                    try:
                        data_json = json.loads(data)
                        if 'token' in data_json:
                            self.handle_received_token(data_json['token'])
                        else:
                            self.process_client_info(data_json, conn)

                    except json.JSONDecodeError:
                        print(f"Error decoding JSON: {data}")

        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def handle_received_token(self, token):
        """Handle an incoming token."""
        self.token_to_verify.append(token)
        print(f"Received and stored token: {token}")

    def shutdown(self):
        """Shut down the server gracefully."""
        if self.token_to_verify:
            self.migrate_tokens()   # Migrate tokens before shutting down
        self.running = False        # Set the running flag to False
        self.auth_socket.close()    # Close the persistent socket connection

    def migrate_tokens(self):
        """Migrate tokens to the other SubAuthentication node."""
        if not self.other_sub_auth:
            print("No other SubAuth node found. Skipping token migration.")
            return
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.other_sub_auth)
                print(f"Connected to other SubAuth node: {self.other_sub_auth}")
                # if self.token_to_verify isn't empty, send the tokens to the other SubAuth node:
                for token in self.token_to_verify:
                    token_data = json.dumps({"token": token})
                    sock.sendall(token_data.encode('utf-8'))
                    print(f"Sent token to other SubAuth node: {token}")
            self.token_to_verify.clear()
        except Exception as e:
            print(f"Error migrating tokens: {e}")

    def handle_other_subauth_address(self, data):
        """Handle receiving the address of the other SubAuthentication node."""
        try:
            # Extract the address part after the prefix "SubAuth Address:"
            address_part = data.replace("SubAuth Address: ", "").strip()
            ip, port = address_part.split(':')
            self.other_sub_auth = (ip, int(port))
            print(f"Received address of other SubAuth node: {self.other_sub_auth}")
        except ValueError as e:
            print(f"Error decoding received SubAuth address data: {e}")

    def process_client_info(self, client_info, conn):
        node_name = client_info.get("name")
        if node_name == "Client":
            self.register_client(client_info, conn)

        elif node_name == "SubFileDistribution":
            self.register_sub_fd(client_info, conn)

    def register_client(self, client_info, conn):
        client_addr = (client_info.get("ip"), client_info.get("port"))
        if client_addr not in self.connected_clients:
            self.connected_clients.append(client_addr)
            # Generate and send a token to the client
            token = self.generate_token(self.host, self.port, 3600)  # 1 hour token lifetime
            # Store the token in token_to_verify
            self.token_to_verify.append(token)
            conn.sendall(json.dumps({"token": token}).encode('utf-8'))
            print(f"Sent token to client: {token}")

    def register_sub_fd(self, sub_fd_info, conn):
        sub_fd_addr = (sub_fd_info.get("ip"), sub_fd_info.get("port"))
        if sub_fd_addr not in self.connected_sub_fd:
            self.connected_sub_fd.append(sub_fd_addr)
            print(f"SubFD connected: {sub_fd_addr}")
            # Ask for a token from the SubFD as a message no json
            conn.sendall("I need a token".encode('utf-8'))
            # Validate the token
            token = conn.recv(1024).decode()
            print(f"Received token from SubFD: {token}")

            # Compare the token to the token_to_verify
            if token in self.token_to_verify:
                print("Token is valid")
                # Send a message to the SubFD that the token is valid
                conn.sendall("Token is valid".encode('utf-8'))

    def send_heartbeat(self):
        """Send a heartbeat message to the Authentication server periodically."""
        while self.running:
            time.sleep(self.heartbeat_interval)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.auth_ip, self.auth_port))
                    heartbeat_data = {
                        "message": "SubAuth Heartbeat",
                        "client_count": len(self.connected_clients),
                        "ip": self.host,
                        "port": self.port
                    }
                    sock.sendall(json.dumps(heartbeat_data).encode('utf-8'))
                    print(f"Sent heartbeat to Primary Authentication: {self.auth_ip}:{self.auth_port}")
            except Exception as e:
                print(f"Error sending heartbeat to Authentication server: {e}")
                if not self.running:
                    break
                # Handle reconnection or other logic as needed

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

    def generate_token(self, ip, port, lifetime_seconds):
        """Generate a token for the specified IP address and port."""
        current_time = int(time.time())
        # Calculate the expiry time
        expiry_time = current_time + lifetime_seconds
        # Create the token using pipes as separators
        token = f"{ip}|{port}|{expiry_time}"
        return token


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: subAuth.py [auth_ip] [auth_port]")
        sys.exit(1)
    auth_ip = sys.argv[1]
    auth_port = int(sys.argv[2])
    sub_auth = SubAuthentication("SubAuthentication", auth_ip, auth_port)
