import json
import requests

class CommonLogger:
    def __init__(self, sender, log_service_url="http://your-file-info-service-url:5001/log"):
        self.sender = sender
        self.log_service_url = log_service_url

    def send_log(self, level, message):
        """Send a log entry to the centralized log service."""
        log_data = {
            "sender": self.sender,
            "level": level,
            "message": message
        }
        try:
            response = requests.post(self.log_service_url, json=log_data)
            if response.status_code != 200:
                print(f"Failed to send log. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error sending log: {e}")

    def info(self, message):
        self.send_log("INFO", message)

    def warning(self, message):
        self.send_log("WARNING", message)

    def error(self, message):
        self.send_log("ERROR", message)