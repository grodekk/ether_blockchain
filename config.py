import os

class Config:
    CURRENT_DIRECTORY = os.path.dirname(__file__)
    API_KEY = "ZA1WF2Z9ZFJBGWPI8N4C6F6ARUVD3K7K5E"
    API_URL = "https://api.etherscan.io/api"
    REQUEST_DELAY = 0.2
    BLOCKS_DATA_DIR = os.path.join(CURRENT_DIRECTORY, 'blocks_data')
    BLOCKS_DATA_FILE = os.path.join(CURRENT_DIRECTORY, 'blocks_data.json')
    JSON_FILES = [file for file in os.listdir(BLOCKS_DATA_DIR) if file.endswith(".json")]  
    LOG_DIRECTORY = os.path.join(CURRENT_DIRECTORY, 'logs')
    LOG_FILE = os.path.join(LOG_DIRECTORY, 'app.log')