from components.utils.datetimes import utc_now_as_str
from components.models.helpers import to_str, to_int
from dataclasses import dataclass, field, fields
from config.defaults import CLAUDE_DEFAULT_MODEL


@dataclass
class SystemSettingsBase:
    id: str
    updated: str
    doc_version: int | str


@dataclass
class SystemSettingsData:
    claude_model: str | None = CLAUDE_DEFAULT_MODEL
    claude_api_key: str | None = None
    google_vision_api_key: str | None = None
    extractor_priority_overrides: dict | None = None


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

        if self.claude_api_key is not None:
            self.claude_api_key = to_str(self.claude_api_key.strip()) or None

        if self.claude_model is not None:
            self.claude_model = (
                to_str(self.claude_model.strip()) or CLAUDE_DEFAULT_MODEL
            )
