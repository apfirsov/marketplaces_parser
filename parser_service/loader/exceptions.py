class BaseParserException(Exception):

    def __str__(self) -> str:
        return self.message


class EmptyResponseError(BaseParserException):

    def __init__(self, message: str = None) -> None:
        self.message = 'Server returned null value'


class ResponseStatusCodeError(BaseParserException):

    def __init__(self, message: str = None) -> None:
        self.message = 'Unexpected response status code'
