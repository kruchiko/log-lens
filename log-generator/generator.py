import threading
import time
import random
import uuid
import json
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from queue import Queue

fake = Faker()

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPICS = {
    'FE': {
        'network': 'logs.fe.network',
        'application': 'logs.fe.application'
    },
    'BE': {
        'network': 'logs.be.network',
        'application': 'logs.be.application'
    }
}

FE_ENDPOINT_METHODS = {
    "/resource": ["GET"],
    "/resource/1": ["GET", "PUT"],
    "/resource/2": ["GET", "PUT", "DELETE"],
    "/login": ["POST"],
    "/logout": ["POST"],
    "/profile": ["GET", "PUT"],
    "/settings": ["GET", "PUT"]
}

BE_ENDPOINT_METHODS = {
    "/api/v1/resource": ["GET"],
    "/api/v1/resource/1": ["GET", "PUT"],
    "/api/v1/resource/2": ["GET", "PUT", "DELETE"],
    "/api/v1/auth/login": ["POST"],
    "/api/v1/auth/logout": ["POST"],
    "/api/v1/user/profile": ["GET", "PUT"],
    "/api/v1/user/settings": ["GET", "PUT"]
}

STATUS_CODES = {
    200: "INFO",
    201: "INFO",
    204: "INFO",
    400: "WARN",
    401: "WARN",
    403: "WARN",
    404: "WARN",
    500: "ERROR",
    502: "ERROR",
    503: "ERROR",
    504: "ERROR"
}

PREDEFINED_ERRORS = [
    {"message": "Database connection failed", "stack_trace": "DatabaseError: Failed to connect to database\n    at connect (database.js:34)\n    at process (app.js:12)"},
    {"message": "Null pointer exception", "stack_trace": "NullPointerError: Cannot read property 'foo' of undefined\n    at getFoo (utils.js:56)\n    at handleRequest (server.js:78)"},
    {"message": "Timeout while fetching data", "stack_trace": "TimeoutError: Request timed out\n    at fetchData (api.js:45)\n    at main (index.js:23)"},
    {"message": "Unauthorized access", "stack_trace": "AuthError: Unauthorized access\n    at checkPermissions (auth.js:22)\n    at handleLogin (login.js:11)"},
    {"message": "Resource not found", "stack_trace": "NotFoundError: Resource not found\n    at getResource (resource.js:30)\n    at handleRequest (server.js:78)"}
]

class LogGenerator:
    def __init__(self):
        self.error_occurred = False
        self.correlation_queue = Queue()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generate_correlation_id(self):
        return str(uuid.uuid4())

    def generate_network_log(self, app, correlation_id, endpoint, method, status_code):
        return {
            "timestamp": datetime.now().isoformat(),
            "application": app,
            "type": "network",
            "correlation_id": correlation_id,
            "request": {
                "method": method,
                "url": endpoint,
                "headers": {"User-Agent": fake.user_agent()},
                "status_code": status_code
            },
            "level": STATUS_CODES[status_code]
        }

    def generate_application_log(self, app, correlation_id, level):
        return {
            "timestamp": datetime.now().isoformat(),
            "application": app,
            "type": "application",
            "correlation_id": correlation_id,
            "message": fake.sentence(),
            "level": level
        }

    def generate_error_log(self, app, correlation_id):
        error = random.choice(PREDEFINED_ERRORS)
        return {
            "timestamp": datetime.now().isoformat(),
            "application": app,
            "type": "error",
            "correlation_id": correlation_id,
            "error": {
                "message": error["message"],
                "stack_trace": error["stack_trace"]
            },
            "level": "ERROR"
        }

    def traffic_pattern(self):
        current_hour = datetime.now().hour
        if 6 <= current_hour < 12:
            return 100
        elif 12 <= current_hour < 18:
            return 10
        elif 18 <= current_hour < 24:
            return 150
        else:
            return 5

    def generate_fe_logs(self):
        while True:
            correlation_id = self.generate_correlation_id()
            self.correlation_queue.put(correlation_id)

            traffic_rate = self.traffic_pattern()
            sleep_time = 1.0 / traffic_rate

            status_code = self.determine_status_code()
            level = STATUS_CODES[status_code]
            endpoint, method = self.select_endpoint_and_method(FE_ENDPOINT_METHODS)

            self.log_message('FE', correlation_id, endpoint, method, status_code, level)
            time.sleep(sleep_time)

            if self.should_generate_error(traffic_rate):
                self.generate_and_log_error('FE', correlation_id)

    def generate_be_logs(self):
        while True:
            correlation_id = self.correlation_queue.get()

            traffic_rate = self.traffic_pattern()
            sleep_time = 1.0 / traffic_rate

            status_code = self.determine_status_code()
            level = STATUS_CODES[status_code]
            endpoint, method = self.select_endpoint_and_method(BE_ENDPOINT_METHODS)

            self.log_message('BE', correlation_id, endpoint, method, status_code, level)
            time.sleep(sleep_time)

            if self.should_generate_error(traffic_rate):
                self.generate_and_log_error('BE', correlation_id)

    def determine_status_code(self):
        if not self.error_occurred:
            return random.choice([200, 201, 204])
        else:
            self.error_occurred = False
            return random.choice([400, 401, 403, 404, 500, 502, 503, 504])

    def select_endpoint_and_method(self, endpoint_methods):
        endpoint = random.choice(list(endpoint_methods.keys()))
        method = random.choice(endpoint_methods[endpoint])
        return endpoint, method

    def log_message(self, app, correlation_id, endpoint, method, status_code, level):
        network_log = self.generate_network_log(app, correlation_id, endpoint, method, status_code)
        application_log = self.generate_application_log(app, correlation_id, level)
        self.producer.send(TOPICS[app]['network'], network_log)
        self.producer.send(TOPICS[app]['application'], application_log)
        print(network_log)
        print(application_log)

    def should_generate_error(self, traffic_rate):
        return random.random() < (traffic_rate / 5000)

    def generate_and_log_error(self, app, correlation_id):
        error_log = self.generate_error_log(app, correlation_id)
        self.producer.send(TOPICS[app]['application'], error_log)
        print(error_log)
        if app == 'FE':
            be_error_log = self.generate_error_log('BE', correlation_id)
            self.producer.send(TOPICS['BE']['application'], be_error_log)
            print(be_error_log)
        self.error_occurred = True

    def start_generating_logs(self):
        threads = [
            threading.Thread(target=self.generate_fe_logs),
            threading.Thread(target=self.generate_be_logs)
        ]

        for thread in threads:
            thread.daemon = True
            thread.start()

        while True:
            time.sleep(1)

if __name__ == "__main__":
    log_generator = LogGenerator()
    log_generator.start_generating_logs()
