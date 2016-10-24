from api import app
from api.metrics_api import *
from config import AppConfig

if __name__ == "__main__":
    app.run(port=AppConfig.api_port)
