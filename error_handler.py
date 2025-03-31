from logger import logger
import json
import sqlite3
import requests
from functools import wraps

class CustomProcessingError(Exception):
    def __init__(self, original_exception: Exception, error_context: str = ""):
        exception_type = original_exception.__class__.__name__
        super().__init__(f"Error in {error_context}: {exception_type} - {original_exception} ")
        self.original_exception = original_exception
        self.error_context = error_context


class ErrorHandler:
    """Class for handling errors and displaying appropriate messages to the user."""
    DISABLE_DECORATORS: bool = False

    def __init__(self, mode: str = None) -> None:
        self.mode = mode

        self.user_error_map = {
            requests.HTTPError: "An error occurred while trying to connect to the server. "
                                "Please check your internet connection or the server's status.",
            sqlite3.DatabaseError: "There was an issue with the database. "
                                   "Please ensure the database is accessible and try again.",
            json.JSONDecodeError: "Data format error. "
                                  "Please check the input file for correct structure and format.",
            requests.ConnectionError: "Network connection error. "
                                      "Please verify your internet connection or server availability.",
            requests.Timeout: "The connection timed out. "
                              "Please check your internet connection and ensure the server is responsive.",
            requests.TooManyRedirects: "Too many redirects while trying to connect. "
                                       "Check your URL settings and server configuration.",
            requests.RequestException: "An error occurred while making the request. "
                                       "Please check your connection and the request parameters.",
            KeyError: "Required data is missing. "
                      "Please check the input data for missing fields.",
            ValueError: "Invalid value in the input data. "
                        "Ensure the data is in the correct format and within allowed ranges.",
            TypeError: "Data type error. "
                       "Please check the input data for correct types.",
            AttributeError: "The object is missing a required attribute. "
                            "Ensure the object is properly initialized and all necessary attributes are set.",
            Exception: "An unexpected error occurred. "
                       "Please check the logs for details and correct the issue.",
        }

    def handle_custom_error(self, error, json_file=None, custom_message=None, extra_info=None):
        logger.error(
            f"{error.__class__.__name__} | {error} {f'in file {json_file}' if json_file else ''}", extra=extra_info)

        message = custom_message if custom_message else self.get_user_message(error)
        self.display_message(message)
        raise CustomProcessingError(error) from error

    def get_user_message(self, error):
        """Return a user-friendly message for the given exception."""
        return self.user_error_map.get(type(error), "Unexpected error occurred.")

    def show_console_message(self, message):
        print(f"ERROR: {message}")

    def show_gui_message(self, message):
        pass

    def show_api_error(self, message):
        pass

    def display_message(self, message):
        if self.mode == 'gui':
            self.show_gui_message(message)
        elif self.mode == 'console':
            self.show_console_message(message)
        elif self.mode == 'api':
            return {'error': 'processing_error', 'message': message}
        else:
            pass


    @staticmethod
    def ehd(context="", json_file=None, custom_message=None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if ErrorHandler.DISABLE_DECORATORS:
                    return func(*args, **kwargs)

                if args and hasattr(args[0], '__self__'):
                    class_name = args[0].__class__.__name__
                else:
                    class_name = func.__qualname__.split('.')[0] if '.' in func.__qualname__ else 'UnknownClass'

                module_name = func.__module__
                func_name = func.__name__

                try:
                    return func(*args, **kwargs)
                except CustomProcessingError:
                    raise
                except Exception as e:
                    ErrorHandler().handle_custom_error(
                        e,
                        json_file=json_file,
                        custom_message=custom_message,
                        extra_info={
                            'ex_custom_module': module_name,
                            'ex_custom_className': class_name,
                            'ex_custom_funcName': func_name
                        }
                    )
            return wrapper

        return decorator


    # exception_handler_decorator_every_class_method#
    @staticmethod
    def ehdc(context="", json_file=None):
        def decorator(cls):
            for attr_name, attr_value in cls.__dict__.items():
                if callable(attr_value) and not attr_name.startswith("__"):

                    if isinstance(attr_value, staticmethod):
                        func = attr_value.__func__
                        decorated_func = ErrorHandler.ehd(context, json_file)(func)
                        setattr(cls, attr_name, staticmethod(decorated_func))

                    else:
                        decorated_method = ErrorHandler.ehd(context, json_file)(attr_value)
                        setattr(cls, attr_name, decorated_method)
            return cls

        return decorator