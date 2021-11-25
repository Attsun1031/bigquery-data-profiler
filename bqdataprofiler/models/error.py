from typing import Any

from pydantic import BaseModel


class Error(BaseModel):
    """
    Error holder
    """
    message: str
    values: dict[str, Any]
    stacktrace: str


class ErrorCollection(BaseModel):
    """
    Error Collection
    """
    errors: list[Error] = []

    def append_error(self, error: Error):
        """
        Append new error
        """
        self.errors.append(error)

    def __len__(self):
        return len(self.errors)

    def __iter__(self):
        return iter(self.errors)
