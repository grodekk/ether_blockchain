import os

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        self.CURRENT_DIRECTORY = os.path.dirname(__file__)
        self.API_KEY = "ZA1WF2Z9ZFJBGWPI8N4C6F6ARUVD3K7K5E"
        self.API_URL = "https://api.etherscan.io/api"
        self.REQUEST_DELAY = 0.2
        self.BLOCKS_DATA_DIR = os.path.join(self.CURRENT_DIRECTORY, 'blocks_data')
        self.BLOCKS_DATA_FILE = os.path.join(self.CURRENT_DIRECTORY, 'blocks_data.json')
        self.JSON_FILES = [file for file in os.listdir(self.BLOCKS_DATA_DIR) if file.endswith(".json")]
        self.LOG_DIRECTORY = os.path.join(self.CURRENT_DIRECTORY, 'logs')
        self.LOG_FILE = os.path.join(self.LOG_DIRECTORY, 'app.log')