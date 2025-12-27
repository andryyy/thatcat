import re
from abc import ABC, abstractmethod
from ..models import DataType, VINResult
from ..processor import VINProcessor


class VINExtractorPlugin(ABC):
    name: str  # Extractor identifier (e.g., "google_vision", "claude")
    handles: list[DataType]  # What types of data this plugin accepts
    priority: int  # Selection priority (0 = highest/first, 1+ = lower priority)

    @property
    def friendly_name(self) -> str:
        return self.name.replace("_", " ").title()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Define required attributes
        required_attrs = ["name", "handles", "priority"]

        # Check if all required attributes are defined in the class
        for attr in required_attrs:
            if not hasattr(cls, attr) or getattr(cls, attr) is None:
                raise TypeError(
                    f"VINExtractorPlugin missing required attribute '{attr}'"
                )

        if not re.match(r"^[a-z0-9_]+$", cls.name):
            raise ValueError(
                f"Plugin name '{cls.name}' is invalid. It must contain only lowercase alphanumeric characters and underscores."
            )

    @abstractmethod
    async def extract(self, data_bytes: bytes, **kwargs) -> VINResult:
        pass


__all__ = ["VINExtractorPlugin", "DataType", "VINProcessor", "VINResult"]
