import logging
import os
from config import Config

class LoggerConfig:
    def __init__(self, log_file='app.log', log_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        self._configure_logger(log_file)

    def _configure_logger(self, log_file):
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        log_path = os.path.join(log_directory, log_file)

        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        self.logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(log_format)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger