import http.server, json

PORT = 9998
METRICS_FILE = '/tmp/waveguard_metrics.json'

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with open(METRICS_FILE) as f:
                data = json.load(f)
            output = f"""# HELP waveguard_velocity_alerts Alertes velocite
# TYPE waveguard_velocity_alerts gauge
waveguard_velocity_alerts {data.get('velocity_alerts', 0)}
# HELP waveguard_volume_alerts Alertes volume
# TYPE waveguard_volume_alerts gauge
waveguard_volume_alerts {data.get('volume_alerts', 0)}
# HELP waveguard_top_count Transactions top fraudeur
# TYPE waveguard_top_count gauge
waveguard_top_count {data.get('top_count', 0)}
"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(output.encode())
        except Exception as e:
            self.send_response(500)
            self.end_headers()
    def log_message(self, *args):
        pass

print('[Prometheus] Serveur démarré sur http://localhost:9998/metrics')
http.server.HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
