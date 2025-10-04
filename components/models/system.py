from components.utils import utc_now_as_str
from components.models.helpers import *
from dataclasses import dataclass, field, fields


@dataclass
class SystemSettingsBase:
    id: str
    updated: str
    doc_version: int | str


@dataclass
class SystemSettingsData:
    GOOGLE_VISION_API_KEY: str | None = None


@dataclass
class SystemSettingsPatch(SystemSettingsData):
    id: str = field(default=None, init=False, repr=False)
    updated: str = field(default_factory=utc_now_as_str, init=False)

    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }


@dataclass
class SystemSettings(SystemSettingsData, SystemSettingsBase):
    def __post_init__(self) -> None:
        if not self.id == "1":
            raise ValueError("id", "'id' must be '1'")

        if not isinstance(self.updated, str) or to_str(self.updated.strip()) == "":
            raise ValueError("updated", "'updated' must be a non-empty string")

        self.doc_version = to_int(self.doc_version)

        if self.GOOGLE_VISION_API_KEY is not None:
            self.GOOGLE_VISION_API_KEY = (
                to_str(self.GOOGLE_VISION_API_KEY.strip()) or None
            )
