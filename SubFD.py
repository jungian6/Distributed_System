import json
import logging
import socket
import threading
import time
import sys
import random
import hashlib


class SubFileDistribution:
    def __init__(self, name, sub_fd_ip, sub_fd_port):
        self.name = name
        self.host = self.find_ip_starting_with_10()
        self.port = self.find_open_port()
        self.fd_ip = sub_fd_ip
        self.fd_port = sub_fd_port
        self.heartbeat_interval = random.randint(5, 10)
        self.connected_clients = []
        self.received = False
        self.tokens = None
        self.client_song_selections = {}
        self.connect_to_fd()

    def connect_to_fd(self):
        """Connect to the FileDistribution server."""
        try:
            # Establish a persistent connection for heartbeat messages
            self.fd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.fd_socket.connect((self.fd_ip, self.fd_port))
            print(f"Connected to File Distribution server at {self.fd_ip}:{self.fd_port}")

            print(self.port)

            client_info = {"name": self.name, "ip": self.host, "port": self.port}
            print(f"Sending client info to File Distribution server: {client_info}")
            self.fd_socket.sendall(json.dumps(client_info).encode('utf-8'))

            num_of_files = int.from_bytes(self.fd_socket.recv(4), byteorder='big')

            print(f"Receiving {num_of_files} files...")

            file_size = 0

            audio_file_size_list = []
            audio_data_list = []

            while file_size < num_of_files:
                # Receive file size
                audio_file_size = int.from_bytes(self.fd_socket.recv(8), byteorder='big')
                print(f"File {file_size + 1} size: {audio_file_size}")

                mp3_file = b''
                while len(mp3_file) < audio_file_size:
                    chunk = self.fd_socket.recv(min(4096, audio_file_size - len(mp3_file)))
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
            audio_file_names_json = self.fd_socket.recv(1024).decode()
            print(f"Received audio file names: {audio_file_names_json}")

            self.audio_files = json.loads(audio_file_names_json)
            self.audio_data = audio_data_list
            self.audio_file_size = audio_file_size_list

            # Files have been received, now start the heartbeat thread
            threading.Thread(target=self.send_delayed_heartbeat, daemon=True).start()

            self.listen_for_connections()

        except Exception as e:
            print(f"Error connecting to File Distribution server: {e}")

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
        """Handle a connection from a client or another SubFD."""
        try:
            while True:
                data = conn.recv(1024).decode()
                if data.startswith("SubFDN Address:"):
                    self.handle_other_sub_fd_address(data)
                    continue
                elif data.startswith("SubFD Data:"):
                    self.handle_data_from_other_sub_fd(data[len("SubFD Data:"):])
                    continue
                elif data.startswith("die"):
                    print("Received Die command. Shutting down... I'm so cold :(")
                    self.shutdown()
                    break
                if not data:
                    continue
                # Process client data
                try:
                    client_info = json.loads(data)
                    self.process_client_info(client_info, conn)
                except json.JSONDecodeError:
                    print(f"Received non-JSON data: {data}")
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            conn.close()

    def shutdown(self):
        """Shut down the server gracefully."""
        self.fd_socket.close()
        sys.exit(0)

    def handle_data_from_other_sub_fd(self, data):
        """Handle data received from another SubFD."""
        try:
            sub_fd_data = json.loads(data)
            print(f"Received data from other SubFD: {sub_fd_data}")

            # Convert string keys back to tuple
            tuple_keyed_data = {eval(key): value for key, value in sub_fd_data.items()}
            self.client_song_selections.update(tuple_keyed_data)

        except json.JSONDecodeError as e:
            print(f"Error decoding data from other SubFD: {e}")
        except SyntaxError as e:
            print(f"Error converting string keys back to tuples: {e}")

    def handle_other_sub_fd_address(self, data):
        """Handle receiving the address of the other SubFD node."""
        try:
            # Extract the address part after the prefix "SubFD Address:"
            address_part = data.replace("SubFDN Address: ", "").strip()
            ip, port = address_part.split(':')
            self.other_sub_fd = (ip, int(port))
            print(f"Received address of other SubFD node: {self.other_sub_fd}")
        except ValueError as e:
            print(f"Error decoding received SubFD address data: {e}")

    def process_client_info(self, client_info, conn):
        node_name = client_info.get("name")
        if node_name == "Client":
            self.register_client(client_info, conn)

    def register_client(self, client_info, conn):
        """Register a client with the SubFD server."""
        client_addr = (client_info.get("ip"), client_info.get("port"))
        if client_addr not in self.connected_clients:
            self.connected_clients.append(client_addr)
            print(f"Clients connected: {client_addr}")
            token = conn.recv(1024).decode()
            print(f"Received token: {token}")
            self.tokens = token
            print(f"Tokens: {self.tokens}")

            # split the token 10.39.31.96|50002|1705440066
            token_split = token.split("|")
            print(token_split)
            # get the ip and port
            ip = token_split[0]
            port = token_split[1]

            sub_auth_ip = ip
            sub_auth_port = int(port)

            print(sub_auth_ip, sub_auth_port)
            self.client_song_selections[client_addr] = None  # Initialize with None

            self.connect_to_sub_auth(sub_auth_ip, sub_auth_port)

            self.send_message_to_client(client_info, conn)

    def send_message_to_client(self, client_info, conn):
        """Send a message to the client."""
        conn.sendall(json.dumps(self.audio_files).encode('utf-8'))
        print(f"Sent song names to client: {self.audio_files}")

        # Receive the song name from the client
        song_name = conn.recv(1024).decode()
        print(f"Received song name from client: {song_name}")

        # Store song name with the client address
        client_addr = (client_info.get("ip"), client_info.get("port"))
        self.client_song_selections[client_addr] = song_name
        print(self.client_song_selections)

        # Send the client_song_selections to the other SubFD
        self.send_data_to_other_sub_fd(self.client_song_selections)

        # Check if another client already downloaded the song
        for other_addr, other_song_name in self.client_song_selections.items():
            if other_addr != client_addr and song_name == other_song_name:
                print(f"Client {client_addr} requested a song '{song_name}' already downloaded by client {other_addr}.")
                download_info = f"Song already downloaded by {other_addr[0]}:{other_addr[1]}"
                conn.sendall(download_info.encode('utf-8'))
                return

        print(f"Client {client_addr} requested a song '{song_name}' not yet downloaded by another client.")
        conn.sendall("Song not yet downloaded".encode('utf-8'))
        time.sleep(1)

        # Send the song data if not already downloaded by another client
        if song_name in self.audio_files:
            file_index = self.audio_files.index(song_name)
            file_size = self.audio_file_size[file_index]
            file_data = self.audio_data[file_index]

            md5_hash = hashlib.md5()
            md5_hash.update(file_data)
            file_hash = md5_hash.hexdigest()

            # Send the file size and hash
            conn.sendall(len(file_data).to_bytes(8, byteorder='big'))
            conn.sendall(file_hash.encode('utf-8'))

            time.sleep(1)

            # Send the file data
            conn.sendall(file_data)
            print(f"Sent song data to client: {song_name}")

    def send_data_to_other_sub_fd(self, data):
        """Send data to the other SubFD node."""
        try:
            # Convert tuple keys to string
            stringified_data = {str(key): value for key, value in data.items()}
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.other_sub_fd)
                prefixed_data = "SubFD Data:" + json.dumps(stringified_data)
                sock.sendall(prefixed_data.encode('utf-8'))
                print(f"Data sent to other SubFD: {stringified_data}")
        except Exception as e:
            print(f"Error sending data to other SubFD: {e}")

    def connect_to_sub_auth(self, sub_auth_ip, sub_auth_port):
        """Connect to the SubAuth node."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((sub_auth_ip, sub_auth_port))
            # Send the client information to the File Distribution Node
            client_info = json.dumps({"name": self.name, "ip": self.host, "port": self.port})
            sock.sendall(client_info.encode('utf-8'))

            print(f"Sent client info to SubAuth node: {client_info}")

            data = sock.recv(1024).decode()
            if data == "I need a token":
                print("I need to send a token")
                # send the token
                token = self.tokens
                sock.sendall(token.encode('utf-8'))
                print(f"Sent token to SubAuth node: {token}")
                # receive the message
                message = sock.recv(1024).decode()
                print(f"Received message from SubAuth node: {message}")
                if message == "Token is valid":
                    print("Token is valid")
                    # exit the loop
                    return
                else:
                    print("Token is invalid")
                    # close the connection
                    sock.close()

    def send_heartbeat(self):
        """Send a heartbeat message to the File Distribution server periodically."""
        while True:
            time.sleep(self.heartbeat_interval)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.fd_ip, self.fd_port))
                    heartbeat_data = {
                        "message": "SubFD Heartbeat",
                        "client_count": len(self.connected_clients),
                        "ip": self.host,
                        "port": self.port
                    }
                    sock.sendall(json.dumps(heartbeat_data).encode('utf-8'))
                    print(f"Sent heartbeat to Primary File Distribution: {self.fd_ip}:{self.fd_port}")
            except Exception as e:
                print(f"Error sending heartbeat to File Distribution server: {e}")
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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: SubFD.py [fd_ip] [fd_port]")
        sys.exit(1)
    fd_ip = sys.argv[1]
    fd_port = int(sys.argv[2])
    sub_fd = SubFileDistribution("SubFileDistribution", fd_ip, fd_port)
