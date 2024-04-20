import logging
from datetime import datetime, timezone, timedelta
import time

class Custom_Logger:
    def __init__(
            self, logger_name=__name__, level=logging.DEBUG, file_level=logging.DEBUG,
            propagate=False, log_file=None, localtime=True,
            log_path=r'C:\Users\silvh\OneDrive\lighthouse\custom_python\files\logger_files'
            ):
        """
        Initialize the custom_logger with the specified parameters.

        Parameters:
            - logger_name (str): The name of the logger (default is 'custom_logger').
            - level (int): The logging level (default is logging.DEBUG).
            - propagate (bool): Whether the logs should be propagated to parent loggers (default is False).
            - log_file (str): The name of the log file (default is None).
            - log_path (str): The path to store log files ]

        Returns:
            None

        Documentation: https://docs.python.org/3/howto/logging.html#handlers
        
        """
        # self.logger = logging.getLogger(logger_name if logger_name else __name__)
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(file_level)
        self.logger.propagate = propagate
        self.log_messages = []  # New attribute to store log messages
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s:\n%(message)s\n')
        if localtime:
            formatter.converter = time.localtime  # Use local time
        handler_messages = ''
        console_handler = None
        self.save = True if level == logging.DEBUG else False            
        if len(self.logger.handlers) > 0:
            handler_messages += f'Found existing handlers: {self.logger.handlers}. '
            for handler in self.logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    console_handler = handler
                    handler_messages += f'Found existing console handler: {console_handler}. '
                    break
        if console_handler == None:
            handler_messages += f'Creating new console handler. '
            console_handler = logging.StreamHandler() # https://docs.python.org/3/library/logging.handlers.html#logging.StreamHandler

        handler_messages += f'Setting console handler level to: {level}. '
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        self.console_handler = console_handler

        if log_file:
            file_handler = None
            log_path = convert_windows_path(log_path)
        
            for handler in self.logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    file_handler = handler
                    handler_messages += f'Found existing file handler: {file_handler}. '
                    break
            if file_handler == None:
                handler_messages += f'Creating new file handler. '
                file_handler = logging.FileHandler(f'{log_path}/{log_file}')
            file_handler.setLevel(file_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.file_handler = file_handler
        self.logger.debug(handler_messages)
        self.log_messages.append(handler_messages)

    def save_log_messages(self, level, message):
        """
        Format the log message in the specified format and append to the log_messages list

        Parameters:
        - level (str): The level of the log message
        - message (str): The message to be logged

        """
        log_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]  # Adjusted format for microseconds
        log_message = f"{log_time} - {self.logger.name} - {level.upper()} - {message}"
        self.log_messages.append(log_message)

    def get_log_messages(self):
        """
        Return the log messages associated with this object.
        """
        for message in self.log_messages:
            print(message)
        return self.log_messages
    
    def debug(self, message):
        """
        - Logs a debug message and optionally saves it to the log file.
        - Parameters:
            - message: The debug message to be logged
            - save: A boolean indicating whether to save the message to the log file (default is False)
        - Returns:
            - None
        """
        self.logger.debug(message)
        if self.save:
            self.save_log_messages('debug', message)

    def info(self, message):
        """
        Logs an informational message using the provided message. 
        Parameters:
            message (str): The message to be logged.
            save (bool): A flag indicating whether to save the log message. Defaults to False.
        """
        self.logger.info(message)
        if self.save:
            self.save_log_messages('info', message)

    def warning(self, message):
        """
        A method to log a warning message and optionally save it to a file.

        - message: The warning message to be logged.
        - save: A boolean indicating whether to save the warning message to a file.
        """
        self.logger.warning(message)
        if self.save:
            self.save_log_messages('warning', message)

    def error(self, message):
        """
        - A method to log an error message and optionally save it to the log file.
        - 
        - :param message: The error message to be logged
        - :param save: A boolean indicating whether to save the error message to the log file
        - :return: None
        """
        self.logger.error(message)
        if self.save:
            self.save_log_messages('error', message)

    def critical(self, message):
        """
        A function that logs a critical message and optionally saves it. 

        Parameters:
            message (str): The critical message to log.
            save (bool, optional): Whether to save the critical message. Defaults to False.
        """
        self.logger.critical(message)
        if self.save:
            self.save_log_messages('critical', message)

    def log(self, message):
        self.logger.log(self.logger.level, message)
        if self.save:
            self.save_log_messages(str(self.logger.level), message)

def convert_windows_path(path):
    path = f'{path}/'.replace('\\','/')
    return path
            
def test_logger(logger, messages_dict):
    """
    A function that logs messages at different levels using the provided logger.
    Parameters:
        logger: the logger object used for logging
        messages_dict: a dictionary with log messages for different levels
        save: a boolean indicating whether to save the log messages (default is True)
    """
    for level in messages_dict:
        message = messages_dict[level]
        if level == 'debug':
            logger.debug(message, save=save)
        elif level == 'info':
            logger.info(message, save=save)
        elif level == 'warning':
            logger.warning(message, save=save)
        elif level == 'error':
            logger.error(message, save=save)
        elif level == 'critical':
            logger.critical(message, save=save)

def create_function_logger(
    function_name, parent_logger, level=logging.INFO,
    log_file=None, **kwargs
    ):
    """
    Create a logger for a specific function.

    Args:
        function_name (str): The name of the function.
        parent_logger (Logger): The logger of the parent function or module.
            If None (default), a new logger will be created.
        level (int, optional): The logging level (default is logging.INFO).
        log_file (str, optional): The path to the log file (default is None).
        **kwargs: Additional keyword arguments to be passed to Custom_Logger.

    Returns:
        Logger: The logger for the specific function.
    """
    if parent_logger:
        propagate = True
        function_logger = parent_logger
    else:
        function_logger_name = f'{function_name}'
        propagate = False
        function_logger = Custom_Logger(
                logger_name=function_logger_name, level=level,
                log_file=log_file, propagate=propagate, **kwargs
                )
    return function_logger