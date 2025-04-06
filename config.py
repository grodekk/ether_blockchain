import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__()
        return cls._instance

    def __init__(self):
        self.BASE_DIR = os.getenv("APP_BASE_DIR", os.getcwd())
        self.API_KEY = os.getenv("API_KEY", "")
        self.API_URL = os.getenv("API_URL", "https://api.etherscan.io/api")
        self.REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", 0.25))

        self.LOG_DIR = os.path.join(self.BASE_DIR, "logs")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "app.log")
        self.DB_FILENAME = os.path.join(self.BASE_DIR, "baza_danych.db")

        self.INTERESTING_INFO_DIR = os.path.join(self.BASE_DIR, "interesting_info")
        self.WALLETS_ACTIVITY_FILENAME = os.path.join(self.BASE_DIR, 'interesting_info', 'Biggest_wallets_activity.json')
        self.BLOCKS_DATA_DIR = os.path.join(self.BASE_DIR, "blocks_data")
        self.BLOCKS_DATA_FILE = os.path.join(self.BASE_DIR, 'blocks_data.json')
        self.OUTPUT_FILE_PATH = os.path.join(self.BASE_DIR, "interesting_info", "Biggest_wallets_activity.json")
        self.JSON_FILES = [file for file in os.listdir(self.BLOCKS_DATA_DIR) if file.endswith(".json")]
        self.OUTPUT_FOLDER = "interesting_info"
        self.PROGRESS_DATA_FILE = os.path.join(self.BASE_DIR, "progress.json")