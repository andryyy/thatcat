import random

from components.models.helpers import to_str, to_int, validate_uuid_str
from dataclasses import dataclass
from functools import cached_property


@dataclass
class TokenConfirmation:
    confirmation_code: int | str
    token: str

    def __post_init__(self) -> None:
        self.confirmation_code = "%06d" % to_int(self.confirmation_code)
        self.token = to_str(self.token.strip())
        if len(self.token) != 14:
            raise ValueError("token", "'token' has wrong length")


@dataclass
class Authentication:
    login: str
    id: str | None = None

    @cached_property
    def token(self) -> str:
        return "%04d-%04d-%04d" % (
            random.randint(0, 9999),
            random.randint(0, 9999),
            random.randint(0, 9999),
        )

    def __post_init__(self) -> None:
        self.login = to_str(self.login.strip())
        if len(self.login) < 3:
            raise ValueError("login", "'login' must be at least 3 characters long")

        if self.id is not None:
            self.id = validate_uuid_str(self.id)
