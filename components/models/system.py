from components.utils import ensure_list
from components.utils.datetimes import utc_now_as_str
from components.models import *


class SystemSettings(BaseModel):
    @computed_field
    @property
    def _form_id(self) -> str:
        return f"form-{str(uuid4())}"

    GOOGLE_VISION_API_KEY: str = Field(
        default="",
        json_schema_extra={
            "title": "Google Vision API",
            "description": "API key for Google Vision",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )


class SystemSettingsBase(BaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return "1"

    details: SystemSettings = SystemSettings()


class UpdateSystemSettings(SystemSettingsBase):
    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()
