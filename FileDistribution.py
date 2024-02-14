import logging
import socket
import json
import subprocess
import sys
import threading
import time
import config


class FileDistribution:

    def __init__(self, name):
        self.host = self.find_ip_starting_with_10()
        self.name = name
        self.port = self.find_open_port()
        self.stored_tokens = {}
        self.connected_clients = []
        self.addresses_exchanged = False        # Flag to check if addresses have been exchanged
        self.connected_sub_fd = {}              # Dictionary to store connected SubFD nodes
        self.client_logins_dict = {}
        self.sub_fd_heartbeat_received = {}     # To track heartbeat status
        self.heartbeat_zero_client_count = {}   # Dictionary to track heartbeats with 0 clients
        self.heartbeat_interval = 10            # Heartbeat every 10 seconds
        self.sub_fd_spawned = 0
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def start_sub_fd(self):
        """Start a SubFD node."""
        if self.sub_fd_spawned < 2:  # Only spawn up to 2 SubFD nodes
            fd_details = [self.host, str(self.port)]
            subprocess.Popen([sys.executable, "subFD.py"] + fd_details,
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NEW_CONSOLE)
            self.sub_fd_spawned += 1

    def send_heartbeat(self):
        """Send heartbeat messages to the Bootstrap server."""
        while True:
            time.sleep(self.heartbeat_interval)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((config.IP, config.PORT))  # Bootstrap server IP and port
                    heartbeat_data = {
                        "message": "FDN Heartbeat",
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
        """Connect to the Bootstrap server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((bootstrap_host, bootstrap_port))

            # Prepare the client information as JSON
            client_info = {"name": self.name, "ip": self.host, "port": self.port}

            # Send the client information to the Bootstrap Server
            sock.sendall(json.dumps(client_info).encode('utf-8'))

            print(f"Connected to Bootstrap Server and sent info: {client_info}")

            num_of_files = int.from_bytes(sock.recv(4), byteorder='big')

            print(f"Receiving {num_of_files} files...")

            file_size = 0

            audio_file_size_list = []
            audio_data_list = []

            while file_size < num_of_files:
                # Receive file size
                audio_file_size = int.from_bytes(sock.recv(8), byteorder='big')
                print(f"File {file_size + 1} size: {audio_file_size}")

                mp3_file = b''
                while len(mp3_file) < audio_file_size:
                    chunk = sock.recv(min(4096, audio_file_size - len(mp3_file)))
                    if not chunk:
                        break
                    mp3_file += chunk

                audio_file_size_list.append(audio_file_size)
                audio_data_list.append(mp3_file)
                file_size += 1
                audio_file_size = 0

            print(f"Received {file_size} files.")
            print(f"File sizes: {audio_file_size_list}")

            # Receive audio file names as json
            audio_file_names_json = sock.recv(1024).decode()
            print(f"Received audio file names: {audio_file_names_json}")

            self.audio_files = json.loads(audio_file_names_json)
            self.audio_data = audio_data_list
            self.audio_file_size = audio_file_size_list

            time.sleep(3)

            self.listen_for_connections()

    def listen_for_connections(self):
        """Listen for connections from clients and SubFD nodes."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, self.port))
            sock.listen(5)
            print(f"Listening for connections on {self.host}:{self.port}...")
            self.start_sub_fd()
            while True:
                conn, addr = sock.accept()
                # Start a new thread to handle the connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        """Handle a client connection."""
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break  # Exit loop if no data is received

                try:
                    client_info = json.loads(data)
                    self.process_client_info(client_info, conn)
                except json.JSONDecodeError:
                    print("Received non-JSON data")

        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def send_files_to_sub_fd(self, sub_fd_socket):
        """Send the audio files to the SubFD node."""
        num_of_files = len(self.audio_data)
        sub_fd_socket.sendall(num_of_files.to_bytes(4, byteorder='big'))  # Send count of files

        # Iterate over each file and its corresponding name
        for music_data, music_file_name in zip(self.audio_data, self.audio_files):
            # Send file size
            sub_fd_socket.sendall(len(music_data).to_bytes(8, byteorder='big'))
            # Send file data
            sub_fd_socket.sendall(music_data)

        # Send the names of the files
        sub_fd_socket.sendall(json.dumps(self.audio_files).encode('utf-8'))

        if self.sub_fd_spawned == 1:
            self.start_sub_fd()

        print(f"Sent {num_of_files} files to Sub File Distribution node.")

    def process_client_info(self, client_info, conn):
        """Process the client information."""
        if client_info.get("message") == "SubFD Heartbeat":
            sub_fd_addr = (client_info.get("ip"), client_info.get("port"))
            client_count = client_info.get("client_count", 0)

            # Update heartbeat status and client count
            self.sub_fd_heartbeat_received[sub_fd_addr] = True
            self.connected_sub_fd[sub_fd_addr] = client_count
            print(f"Received heartbeat from SubFD node {sub_fd_addr} with {client_count} clients")

            # Track zero client heartbeats and send die message if necessary
            if client_count == 0:
                self.heartbeat_zero_client_count[sub_fd_addr] = self.heartbeat_zero_client_count.get(sub_fd_addr, 0) + 1
                if self.heartbeat_zero_client_count[sub_fd_addr] >= 15:
                    self.send_die_message(sub_fd_addr)
                    self.heartbeat_zero_client_count[sub_fd_addr] = 0  # Reset the count
            else:
                self.heartbeat_zero_client_count[sub_fd_addr] = 0  # Reset the count if any clients are connected

            # Check if it's time to exchange addresses
            if len(self.sub_fd_heartbeat_received) == 2 and all(self.sub_fd_heartbeat_received.values()):
                self.exchange_subfdn_addresses()

            return

        node_name = client_info.get("name")
        if node_name == "Client":
            self.register_client(client_info)
            self.send_subfd_info_to_client(conn)
        elif node_name == "SubFileDistribution":
            self.register_sub_fd(client_info)
            self.send_files_to_sub_fd(conn)

    def send_die_message(self, sub_fd_addr):
        """Send a 'die' message to the SubFD node."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(sub_fd_addr)
                sock.sendall("die".encode('utf-8'))
                print(f"Sent 'die' message to SubFD node {sub_fd_addr}")
                # Remove the SubFD node from the connected nodes
                del self.connected_sub_fd[sub_fd_addr]
                del self.heartbeat_zero_client_count[sub_fd_addr]  # Also remove it from the tracking dictionary
        except Exception as e:
            print(f"Error sending 'die' message to SubFD node {sub_fd_addr}: {e}")

    def exchange_subfdn_addresses(self):
        """Exchange the addresses of the SubFD nodes."""
        if not self.addresses_exchanged:
            sub_fd_addresses = list(self.connected_sub_fd.keys())
            for addr in sub_fd_addresses:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect(addr)
                        other_addr = sub_fd_addresses[1] if addr == sub_fd_addresses[0] else sub_fd_addresses[0]
                        addr_str = f"SubFDN Address: {other_addr[0]}:{other_addr[1]}"
                        sock.sendall(addr_str.encode('utf-8'))
                        print(f"Sent address of SubFD node {other_addr} to {addr}")
                except Exception as e:
                    print(f"Error sending SubFD address: {e}")
            # Set the flag to True after addresses are exchanged
            self.addresses_exchanged = True

    def register_client(self, client_info):
        """Register a client."""
        client_addr = (client_info.get("ip"), client_info.get("port"))
        if client_addr not in self.connected_clients:
            self.connected_clients.append(client_addr)
            print(f"Clients connected: {client_addr}")

    def register_sub_fd(self, client_info):
        """Register a SubFD node."""
        sub_fd_addr = (client_info.get("ip"), client_info.get("port"))
        if sub_fd_addr not in self.connected_sub_fd:
            # Initialize the client count for the new SubFD node
            self.connected_sub_fd[sub_fd_addr] = 0
            print(f"SubFD node connected: {sub_fd_addr}")

    def send_subfd_info_to_client(self, conn):
        """Send the SubFD node info to the client."""
        if self.connected_sub_fd:
            # Select the SubFD node with the least clients
            sub_fd_info = min(self.connected_sub_fd, key=self.connected_sub_fd.get)
            sub_fd_data = json.dumps({
                "sub_fd_ip": sub_fd_info[0],
                "sub_fd_port": sub_fd_info[1]
            })
            conn.sendall(sub_fd_data.encode())
            print(f"Sent SubFD node info to client: {sub_fd_data}")
        else:
            conn.sendall("No available SubFD nodes".encode())


if __name__ == "__main__":
    client = FileDistribution("FileDistribution")
    client.connect_to_bootstrap(config.IP, config.PORT)
