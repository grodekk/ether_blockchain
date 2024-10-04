import logging
import os
from config import Config
import os

class LoggerConfig:
    _instance = None    
    _lock = multiprocessing.Lock()

    def __new__(cls, log_file='app.log', log_level=logging.DEBUG):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(LoggerConfig, cls).__new__(cls)
                cls._instance._initialize(log_file, log_level)
        return cls._instance

    def _initialize(self, log_file, log_level):        
        self.logger = logging.getLogger("simple_logger")
        self.logger.setLevel(log_level)
        
        log_format = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
        
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        
        log_path = os.path.join(log_directory, log_file)        
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(log_format)
        self.logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        print("returned")
        return self.logger 