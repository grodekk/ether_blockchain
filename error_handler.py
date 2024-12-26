from logger import logger
import json
import inspect
import sqlite3
from functools import wraps


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

        # sqlite errors #

        elif isinstance(e, sqlite3.IntegrityError):
            logger.error(f"{context} - Integrity error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="IntegrityError") from e

        elif isinstance(e, sqlite3.OperationalError):
            logger.error(f"{context} - Operational error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="OperationalError") from e

        elif isinstance(e, sqlite3.DatabaseError):
            logger.error(f"{context} - Database error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="DatabaseError") from e

        else:
            logger.exception(f"{context} - Unexpected error {file_info}: {e}")
            raise CustomProcessingError(e, context=context, extra_info="General Exception") from e

    #exception_handler_decorator_one_method#
    @staticmethod
    def ehd(context="", json_file=None):        
        def decorator(func):             
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)

                except CustomProcessingError as cpe:
                    raise

                except Exception as e:
                    dynamic_json_file = kwargs.get("json_file") or (args[0] if args else json_file)
                    dynamic_context = context or f"{self.__class__.__name__}.{func.__name__}"
                    CustomProcessingError.handle_processing_exception(e, context=dynamic_context, json_file=dynamic_json_file)

            return wrapper
            
        return decorator

    #exception_handler_decorator_every_class_method#
    @staticmethod
    def ehdc(context="", json_file=None):     
        def decorator(cls):
            for attr_name, attr_value in cls.__dict__.items():
                if callable(attr_value) and not attr_name.startswith("__"):
                    # decorated by previous decorator
                    decorated_method = CustomProcessingError.ehd(context, json_file)(attr_value)
                    setattr(cls, attr_name, decorated_method)
            return cls
        return decorator