import logging
from config import Config
import os
import inspect
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.custom_module = getattr(record, 'ex_custom_module', None) or record.module
        record.custom_className = getattr(record, 'ex_custom_className', None)
        record.custom_funcName = getattr(record, 'ex_custom_funcName', None)

        if not record.custom_className or not record.custom_funcName:
            frame = inspect.currentframe().f_back
            try:
                class_name = None
                while frame:
                    module = inspect.getmodule(frame)
                    if module and not module.__name__.startswith("logging"):
                        if 'self' in frame.f_locals:
                            caller_class = frame.f_locals['self'].__class__.__name__
                            class_name = caller_class if caller_class != 'CustomFormatter' else None
                            break
                        qualname = frame.f_code.co_qualname
                        if '.' in qualname:
                            class_name = qualname.split('.')[0]
                        break
                    frame = frame.f_back

                record.custom_className = class_name if class_name else 'UnknownClass'
                record.custom_funcName = getattr(record, 'funcName', 'UnknownFunc')

            finally:
                del frame

        return super().format(record)


class SingletonLogger:
    _instance: Optional['SingletonLogger'] = None
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._initialize_logger()
            self._initialized = True

    def _initialize_logger(self):
        config = Config()
        log_path = config.LOG_FILE
        test_mode = os.getenv('TEST_MODE', 'False') == 'True'

        self.logger = logging.getLogger("SingletonLogger")
        if self.logger.handlers:
            return

        self.logger.setLevel(logging.DEBUG)

        formatter = CustomFormatter(
            '%(asctime)s - %(custom_module)s.%(custom_className)s.%(custom_funcName)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger


logger = SingletonLogger().get_logger()