from http.server import SimpleHTTPRequestHandler, HTTPServer
import threading


class CustomHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, username=None, **kwargs):
        self.username = username
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            with open('player.html', 'r') as file:
                html_content = file.read()
                custom_content = html_content.replace("{{username}}", self.username)
                self.wfile.write(custom_content.encode('utf-8'))
        else:
            return SimpleHTTPRequestHandler.do_GET(self)


class AudioFileServer:
    def __init__(self, port, file_to_serve, username):
        self.port = 50010
        self.file_to_serve = file_to_serve
        self.username = username

    def start_server(self):
        handler = lambda *args: CustomHTTPRequestHandler(*args, username=self.username, directory=".")
        httpd = HTTPServer(('localhost', self.port), handler)
        httpd.serve_forever()

    def serve_file(self):
        threading.Thread(target=self.start_server, daemon=True).start()
