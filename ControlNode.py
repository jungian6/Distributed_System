import json
import logging
import socket
import subprocess
import sys
import threading
import time
import config


class ControlNode:
    def __init__(self, name):
        self.name = name
        self.host = self.find_ip_starting_with_10()
        self.running = True  # Flag to keep the main thread running
        threading.Thread(target=self.start_command_listener, daemon=True).start()

    def start_command_listener(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener_socket:
            listener_socket.bind((self.host, self.find_open_port()))
            listener_socket.listen(1)
            self.listener_port = listener_socket.getsockname()[1]
            print(f"Listening for commands on port {self.listener_port}")

            while self.running:
                conn, _ = listener_socket.accept()
                command = conn.recv(1024).decode()
                if command:
                    print(f"Received command: {command}")
                    self.execute_command(command)
                conn.close()

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

    def execute_command(self, command):
        if command == "start_authentication":
            self.handle_authentication()
        elif command == "start_file_distribution":
            self.handle_fileDistribution()
        elif command == "start_client":
            self.handle_client()
        elif command == "shutdown":
            self.stop()

    def connect_to_bootstrap(self, bootstrap_host, bootstrap_port):
        connected = False
        while not connected and self.running:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((bootstrap_host, bootstrap_port))
                    my_ip = self.host
                    client_info = {"name": self.name, "ip": my_ip, "port": self.listener_port}
                    sock.sendall(json.dumps(client_info).encode('utf-8'))
                    print(f"Connected to Bootstrap Server and sent info: {client_info}")
                    connected = True  # Successful connection
            except socket.error as e:
                print(f"Failed to connect to Bootstrap Server: {e}. Retrying...")
                time.sleep(5)  # Wait for 5 seconds before retrying

        if not self.running:
            print("Control Node stopped before connecting to Bootstrap Server.")

    def handle_authentication(self):
        print("Authentication service setup complete")
        subprocess.Popen([sys.executable, "Authentication.py"],
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)

    def handle_fileDistribution(self):
        print("File Distribution service setup complete")
        subprocess.Popen([sys.executable, "FileDistribution.py"],
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)

    def handle_client(self):
        print("Client service setup complete")
        subprocess.Popen([sys.executable, "Client.py"],
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)

    def run(self):
        """Keep the main thread running."""
        self.connect_to_bootstrap(config.IP, config.PORT)
        while self.running:
            time.sleep(1)  # Sleep to prevent high CPU usage

    def stop(self):
        """Stop the Control Node."""
        self.running = False


if __name__ == "__main__":
    control_node = ControlNode("node")
    control_node.run()  # This will handle connection and keep the main thread running
