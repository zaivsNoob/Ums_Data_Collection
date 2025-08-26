import json
import websocket

class WebSocketClient:
    def __init__(self, websocket_url, token):
        self.websocket_url = websocket_url
        self.token = token
        self.ws = websocket.WebSocket()

    def connect(self):
        try:
            self.ws.connect(self.websocket_url, header=[f"Authorization: Bearer {self.token}"])
        except Exception as e:
            print(f"WebSocket connection failed: {e}")

    def send_notification(self, node_name, power, status):
        try:
            notification_data = {
                "node_name": node_name,
                "power": power,
                "status": status
            }
            self.ws.send(json.dumps(notification_data))
            print(f"Notification sent for node: {node_name}")
        except Exception as e:
            print(f"Error sending notification: {e}")

    def close(self):
        self.ws.close()
        print("WebSocket connection closed.")
