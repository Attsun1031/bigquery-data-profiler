from typing import Any

from pydantic import BaseModel


class Error(BaseModel):
    """
    Error holder
    """
    message: str
    values: dict[str, Any]
    stacktrace: str
