import os

from components.models import *
from components.utils.images import magic
from werkzeug.utils import secure_filename

mime = magic.Magic(mime=True)


class Asset(BaseModel):
    id: str
    filename: str | None = None
    mime_type: str | None = None

    @model_validator(mode="after")
    def asset_validator(self) -> "Asset":
        if not os.path.exists(f"assets/{self.id}"):
            raise ValueError("No such file")
        if not self.filename:
            self.filename = self.id
        else:
            self.filename = secure_filename(self.filename)
        if not self.mime_type:
            self.mime_type = mime.from_file(f"assets/{self.id}")
        return self
