from abc import ABC, abstractmethod
from ..models import DataType, VINResult
from ..processor import VINProcessor


class VINExtractorPlugin(ABC):
    name: str  # Extractor identifier (e.g., "google_vision", "claude")
    handles: list[DataType]  # What types of data this plugin accepts
    priority: int  # Selection priority (0 = highest/first, 1+ = lower priority)

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

    @abstractmethod
    async def extract(self, data_bytes: bytes, **kwargs) -> VINResult:
        pass


__all__ = ["VINExtractorPlugin", "DataType", "VINProcessor", "VINResult"]
