from dataclasses import dataclass, fields
from components.models.helpers import to_str


@dataclass
class Vault:
    public_key_pem: str
    wrapped_private_key: str
    iv: str
    salt: str

    def __post_init__(self) -> None:
        for f in fields(self):
            setattr(self, f, to_str(getattr(self, f)))
