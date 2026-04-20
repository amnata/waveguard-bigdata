import http.server, json

PORT = 9999
METRICS_FILE = '/tmp/waveguard_metrics.json'

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            try:
                with open(METRICS_FILE) as f:
                    data = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(data.encode())
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, *args):
        pass

print('[Server] http://localhost:9999/metrics')
http.server.HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
