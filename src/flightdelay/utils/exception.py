import sys

from flightdelay.utils.logger import logger


def error_message_detail(error: Exception, error_detail=sys) -> str:
    _, _, exc_tb = error_detail.exc_info()
    if exc_tb is None:
        return str(error)
    file_name = exc_tb.tb_frame.f_code.co_filename
    return f"Error in [{file_name}] line [{exc_tb.tb_lineno}]: {error}"


class CustomException(Exception):
    def __init__(self, error: Exception, error_detail=sys):
        self.error_message = error_message_detail(error, error_detail)
        super().__init__(self.error_message)

    def __str__(self) -> str:
        return self.error_message