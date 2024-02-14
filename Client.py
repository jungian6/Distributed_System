import hashlib
import logging
import os
import socket
import json
import threading
import time
import webbrowser
from AudioFileServer import AudioFileServer
import config


class Client:

    def __init__(self, name):
        self.token = None
        self.host = self.find_ip_starting_with_10()
        self.name = name
        self.port = self.find_open_port()
        self.auth_ip = None
        self.auth_port = None
        self.usernameVisual = None

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
        try:
            # Connect to the Bootstrap Server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((bootstrap_host, bootstrap_port))

                # Prepare the client information as JSON
                client_info = {"name": self.name, "ip": self.host, "port": self.port}

                # Send the client information to the Bootstrap Server
                sock.sendall(json.dumps(client_info).encode('utf-8'))

                print(f"Connected to Bootstrap Server and sent info: {client_info}")

                # Receive client information from the Bootstrap Server
                response = sock.recv(1024).decode()
                print(f"Received response: {response}")

                # send message to begin authentication
                sock.sendall("auth".encode('utf-8'))

                # receive auth ip and port
                self.auth_ip = sock.recv(1024).decode()
                self.auth_port = int.from_bytes(sock.recv(4), byteorder='big')

                print(f"Received Auth IP info: {self.auth_ip}")
                print(f"Received Auth Port info: {self.auth_port}")

                threading.Thread(target=self.connect_to_auth, args=(self.auth_ip, self.auth_port)).start()

        except Exception as e:
            print(e)

    def connect_to_auth(self, auth_host, auth_port):
        """Connect to the Authentication server."""
        try:
            # Connect to the Auth Server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((auth_host, auth_port))

                # Prepare the client information as JSON
                client_info = {"name": self.name, "ip": self.host, "port": self.port}
                sock.sendall(json.dumps(client_info).encode('utf-8'))
                print(f"Connected to Auth Server and sent info: {client_info}")

                while True:
                    # Receive username prompt from Auth Server and send username
                    username_prompt = sock.recv(1024).decode()
                    print(username_prompt, end='')
                    username = input()
                    sock.sendall(username.encode())

                    # Receive password prompt from Auth Server and send password
                    password_prompt = sock.recv(1024).decode()
                    print(password_prompt, end='')
                    password = input()
                    sock.sendall(password.encode())

                    # Receive response from Auth Server
                    full_response = sock.recv(1024).decode()
                    print(f"Received response: {full_response}")

                    if full_response == "Login successful." or full_response == "Signed up successfully.":
                        self.usernameVisual = username

                        # Expecting SubAuthentication node information as JSON
                        sub_auth_info_json = sock.recv(1024).decode()
                        sub_auth_info = json.loads(sub_auth_info_json)
                        print(f"Received SubAuthentication node info: {sub_auth_info}")

                        sub_auth_ip = sub_auth_info["sub_auth_ip"]
                        sub_auth_port = sub_auth_info["sub_auth_port"]
                        print(f"SubAuthentication node address: IP={sub_auth_ip}, Port={sub_auth_port}")

                        self.connect_to_sub_auth(sub_auth_ip, sub_auth_port)

        except Exception as e:
            print(e)

    def connect_to_sub_auth(self, sub_auth_host, sub_auth_port):
        """Connect to the SubAuthentication server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((sub_auth_host, sub_auth_port))
                print(f"Connected to SubAuthentication server at {sub_auth_host}:{sub_auth_port}")

                # Send the client information to the SubAuthentication server
                client_info = {"name": self.name, "ip": self.host, "port": self.port}
                sock.sendall(json.dumps(client_info).encode('utf-8'))

                # Receive and process the token from the SubAuthentication server
                token_json = self.receive_json(sock)
                if token_json.get("token"):
                    self.token = token_json["token"]
                    print(f"Received token from SubAuthentication server: {self.token}")
                    self.request_fdn_info()  # Proceed to request FDN info
                else:
                    print("Failed to receive token from SubAuthentication server")

        except Exception as e:
            print(f"Error connecting to SubAuthentication server: {e}")

    def receive_json(self, sock):
        """Receive a JSON object from the socket."""
        buffer = ""
        while True:
            data = sock.recv(1024).decode()
            buffer += data
            try:
                json_data = json.loads(buffer)
                return json_data
            except json.JSONDecodeError:
                # Continue receiving data if JSON is incomplete
                continue

    def request_fdn_info(self):
        """Request File Distribution Node information from the Bootstrap server."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((config.IP, config.PORT))

                # Send a message to request FDN information
                sock.sendall("request_fdn_info".encode('utf-8'))

                # Receive the FDN information
                fdn_info_json = sock.recv(1024).decode()
                if fdn_info_json:
                    fdn_info = json.loads(fdn_info_json)
                    print(f"Received File Distribution Node info: IP={fdn_info['ip']}, Port={fdn_info['port']}")
                    self.go_to_file_distribution(fdn_info['ip'], fdn_info['port'])
                else:
                    print("No File Distribution Node info received")
        except Exception as e:
            print(f"Error requesting FDN info: {e}")

    def go_to_file_distribution(self, fdn_host, fdn_port):
        """Connect to the File Distribution Node."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((fdn_host, fdn_port))

                # Send the client information to the File Distribution Node
                client_info = json.dumps({"name": self.name, "ip": self.host, "port": self.port})
                sock.sendall(client_info.encode('utf-8'))

                # Receive sub auth address as JSON
                sub_fd_data_json = sock.recv(1024).decode()
                sub_fd_data = json.loads(sub_fd_data_json)
                sub_fd_ip = sub_fd_data["sub_fd_ip"]
                sub_fd_port = sub_fd_data["sub_fd_port"]
                print(f"Redirected to SubFD node at {sub_fd_ip}:{sub_fd_port}")

                self.connect_to_sub_fd(sub_fd_ip, sub_fd_port)

        except Exception as e:
            print(f"Error connecting to File Distribution Node: {e}")

    def connect_to_sub_fd(self, sub_fd_host, sub_fd_port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((sub_fd_host, sub_fd_port))
                print(f"Connected to SubFD server at {sub_fd_host}:{sub_fd_port}")

                # Send the client information to the SubFD server
                client_info = {"name": self.name, "ip": self.host, "port": self.port}
                sock.sendall(json.dumps(client_info).encode('utf-8'))

                time.sleep(1)

                # Send the token to the SubFD server
                print(f"Sending token to SubFD server: {self.token}")
                sock.sendall(self.token.encode('utf-8'))

                # Receive the audio file names
                audio_file_names_json = sock.recv(1024).decode()
                audio_file_names = json.loads(audio_file_names_json)

                # Display the list of songs in a user-friendly format
                print("\nAvailable Songs:")
                for index, song in enumerate(audio_file_names, start=1):
                    print(f"{index}. {song}")
                print("\nPlease enter the number of the song you want to play:")

                # Get and validate the user's song choice
                while True:
                    try:
                        choice = int(input("Your choice: "))
                        if 1 <= choice <= len(audio_file_names):
                            song_choice = audio_file_names[choice - 1]
                            break
                        else:
                            print("Invalid choice, please enter a number from the list.")
                    except ValueError:
                        print("Please enter a valid number.")

                # Send the song choice to the sub File Distribution Node
                sock.sendall(song_choice.encode('utf-8'))

                data = sock.recv(1024).decode()
                print(data)

                if data.startswith("Song already downloaded by"):
                    # Extract the IP and port of the client who has the song
                    _, other_client_address = data.split(" by ")
                    other_client_ip, other_client_port = other_client_address.split(":")

                    # Connect to the other client
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as p2p_sock:
                        p2p_sock.connect((other_client_ip, int(other_client_port)))

                        print(f"Connected to peer at {other_client_ip}:{other_client_port}. Downloading the song...")

                        p2p_sock.sendall("Need song".encode('utf-8'))

                        time.sleep(1)

                        # Receive the size of the audio file
                        audio_file_size = int.from_bytes(p2p_sock.recv(8), byteorder='big')
                        print(f"Receiving audio file of size: {audio_file_size}")

                        # Receive the audio file hash
                        received_md5 = p2p_sock.recv(64).decode()
                        print(f"Receiving audio file with hash: {received_md5}")

                        # Receive the audio file data
                        audio_file_data = b''
                        total_received = 0
                        while len(audio_file_data) < audio_file_size:
                            chunk = p2p_sock.recv(min(4096, audio_file_size - len(audio_file_data)))
                            if not chunk:
                                break
                            audio_file_data += chunk
                            total_received += len(chunk)

                        # Save the audio file locally
                        with open(f"received_song.mp3", "wb") as audio_file:
                            audio_file.write(audio_file_data)
                            calculated_md5 = hashlib.md5(audio_file_data).hexdigest()

                        if received_md5 == calculated_md5:
                            print(f"File received successfully {calculated_md5}")
                            audio_server = AudioFileServer(port=50010, file_to_serve="received_song.mp3",
                                                           username=self.usernameVisual)
                            audio_server.serve_file()

                            webbrowser.open("http://localhost:50010/")

                        else:
                            print("File corruption detected.")

                elif data.startswith("Song not yet downloaded"):

                    time.sleep(1)

                    # Receive the size of the audio file
                    audio_file_size = int.from_bytes(sock.recv(8), byteorder='big')
                    print(f"Receiving audio file of size: {audio_file_size}")

                    # Receive the audio file hash
                    received_md5 = sock.recv(64).decode()
                    print(f"Receiving audio file with hash: {received_md5}")

                    # Receive the audio file data
                    audio_file_data = b''
                    total_received = 0
                    while len(audio_file_data) < audio_file_size:
                        chunk = sock.recv(min(4096, audio_file_size - len(audio_file_data)))
                        if not chunk:
                            break
                        audio_file_data += chunk
                        total_received += len(chunk)

                    # Save the audio file locally
                    with open(f"received_song.mp3", "wb") as audio_file:
                        audio_file.write(audio_file_data)
                        calculated_md5 = hashlib.md5(audio_file_data).hexdigest()

                    if received_md5 == calculated_md5:
                        print("File received successfully")
                        audio_server = AudioFileServer(port=50010, file_to_serve="received_song.mp3",
                                                       username=self.usernameVisual)
                        audio_server.serve_file()

                        webbrowser.open("http://localhost:50010/")

                    else:
                        print("File corruption detected.")

                self.listen_for_connections()

        except Exception as e:
            print(f"Error connecting to SubFD server: {e}")

    def listen_for_connections(self):
        """Listen for connections from other clients."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.host, self.port))
            sock.listen(5)
            print(f"Listening for connections on {self.host}:{self.port}...")
            while True:
                conn, addr = sock.accept()
                # Start a new thread to handle the connection
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        """Handle a connection from another client."""
        try:
            print(f"Connection from {addr}")
            data = conn.recv(1024).decode()
            print(f"Received data: {data}")

            if data == "Need song":
                with open("received_song.mp3", "rb") as audio_file:
                    # send the size of the audio file
                    audio_file_size = os.path.getsize("received_song.mp3")
                    conn.sendall(audio_file_size.to_bytes(8, byteorder='big'))
                    print(f"Sending audio file of size: {audio_file_size}")

                    time.sleep(1)  # Wait for the client to process the file size

                    # send the audio file hash
                    audio_file_data = audio_file.read()
                    md5_hash = hashlib.md5(audio_file_data).hexdigest()
                    conn.sendall(md5_hash.encode('utf-8'))

                    time.sleep(1)  # Wait for the client to process the hash

                    # send the audio file data in chunks
                    total_sent = 0
                    while total_sent < audio_file_size:
                        chunk = audio_file_data[total_sent:total_sent + 4096]  # Define the chunk size
                        sent = conn.send(chunk)
                        if sent == 0:
                            raise RuntimeError("Socket connection broken")
                        total_sent += sent

                print("File sent successfully")

            else:
                conn.sendall(b"Song not available")
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    client = Client("Client")
    client.connect_to_bootstrap(config.IP, config.PORT)
