from components.models import *
from components.models.assets import Asset
from components.models.coords import Location
from components.utils import ensure_list, utc_now_as_str


class CompleteProcessingRequest(BaseModel):
    id: Annotated[str, AfterValidator(lambda v: str(UUID(v)))]
    reason: Literal["abort", "completed"]


class CreateProcessingData(BaseModel):
    assets: Annotated[
        Json[List[Asset] | Asset] | Asset | list[Asset],
        AfterValidator(lambda asset: ensure_list(asset)),
    ]
    location: Location | None = None
    vin: str | None = None
    user: Annotated[str, AfterValidator(lambda v: str(UUID(v)))]

    @field_validator("vin")
    def validate_vins(cls, v):
        from components.utils.vins import VinTool

        if v and not VinTool.verify_checksum(v):
            raise PydanticCustomError(
                "vin",
                "Die Checksumme der Fahrgestellnummer ist nicht korrekt",
                dict(),
            )
        return v

    @computed_field
    @cached_property
    def id(self) -> str:
        return str(uuid4())

    @computed_field
    @property
    def created(self) -> str:
        return utc_now_as_str()
