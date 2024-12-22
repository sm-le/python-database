# Logger
# contributors: smlee

# History
# 2024-12-22 - v1.0.1 | removed dataclass and verbose correction. 
# 2024-04-20 - v1.0.0 | first commit

# Module
import logging
import functools
from io import StringIO
from typing import Union

# Main
class Logger:
    """Logger class to log messages
    """

    def __init__(self,
                 name:str,
                 verbose:int=10):
        """Instantiate Logger object

        Args:
            name: logger name
            verbose: debug information
                1: info
                10: debug
        """
        # default
        self.name = name
        self.verbose = verbose

        self.log_stream:StringIO = StringIO()
        formatter = logging.Formatter(fmt="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                                      datefmt='%Y-%m-%dT%H:%M:%S')
        # set logger
        self.logger = logging.getLogger(self.name)
        handler = logging.StreamHandler(self.log_stream)
        # set formatter
        handler.setFormatter(formatter)
        # add handler
        self.logger.addHandler(handler)
        # set level
        if self.verbose == 1:
            self.logger.setLevel(logging.INFO)
        elif self.verbose == 10:
            self.logger.setLevel(logging.DEBUG)
        else:
            raise ValueError("verbose should be 1 or 10")
        
    def get_logger(self) -> object:
        return self.logger

    def get_log_stream(self) -> object:
        return self.log_stream

    def get_log_content(self) -> str:
        return self.log_stream.getvalue()

    def clear_log_content(self):
        self.log_stream.truncate(0)
        self.log_stream.seek(0)

# Get decorator for logging
def log(_func=None,
        *,
        set_logger: Union[Logger, logging.Logger] = None,
        verbose:int=1):
    """decorator for logging
    reference: https://ankitbko.github.io/blog/2021/04/logging-in-python/

    Args:
        my_logger (Union[MyLogger, logging.Logger], optional): logger to use. Defaults to None.
        verbose (int, optional): verbose level. Defaults to 1.
    """
    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if set_logger is None:
                logger = Logger(func.__name__, verbose=verbose).get_logger()
            else:
                logger = set_logger 
            # collect debug information  
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()] ## !r string representation
            signature = ", ".join(args_repr + kwargs_repr)
            logger.debug(f"function {func.__name__} called with args {signature}")
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
                raise e
        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)