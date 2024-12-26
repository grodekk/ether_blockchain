import logging
import os
from config import Config
import os


class LoggerConfig:
    _instance = None    

    def __new__(cls):        
        if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self):        
        config = Config()

        self.logger = logging.getLogger("simple_logger")
        self.logger.setLevel(logging.DEBUG)
        
        log_format = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(message)s')
        
        log_path = config.LOG_FILE
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(log_format)
        self.logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        print("returned")
        return self.logger 


logger_config = LoggerConfig()
logger = logger_config.get_logger()