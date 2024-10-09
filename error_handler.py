from logger import LoggerConfig
import json
import inspect

logger_config = LoggerConfig()
logger = logger_config.get_logger()

class CustomProcessingError(Exception):
    def __init__(self, original_exception, context="", extra_info=""):
        super().__init__(f"Error in {context}: {original_exception} - {extra_info}")
        self.original_exception = original_exception
        self.context = context
        self.extra_info = extra_info

    @staticmethod
    def handle_processing_exception(e, json_file=None, context=None):
        file_info = f"in file {json_file}" if json_file else ""
        if not context:
            frame = inspect.currentframe().f_back
            context = f"{frame.f_locals['self'].__class__.__name__}.{frame.f_code.co_name}"

        if isinstance(e, KeyError):
            logger.error(f"{context} - Missing key {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="KeyError") from e

        elif isinstance(e, ValueError):
            logger.error(f"{context} - Invalid value {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="ValueError") from e

        elif isinstance(e, TypeError):
            logger.error(f"{context} - Type error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="TypeError") from e
        
        elif isinstance(e, AttributeError):
            logger.error(f"{context} - Attribute error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="AttributeError") from e

        elif isinstance(e, json.JSONDecodeError):
            logger.error(f"{context} - JSON decoding error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="JSONDecodeError") from e

        else:
            logger.exception(f"{context} - Unexpected error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="General Exception") from e