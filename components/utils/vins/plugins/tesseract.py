import asyncio
import pytesseract
from io import BytesIO
from PIL import Image

from .base import VINExtractorPlugin, DataType, VINResult, VINProcessor
from components.models.assets import Asset
from components.cluster import cluster


class TesseractExtractor(VINExtractorPlugin):
    name = "tesseract"
    handles = [DataType.IMAGE]
    priority = 0

    def __init__(self, *args, **kwargs):
        pass

    async def extract(self, data_bytes: bytes, **kwargs) -> list[VINResult]:
        asset = await Asset.from_bytes(
            data_bytes,
            cluster=cluster,
            overlay=None,
            compress=False,
            filename=kwargs.get("filename"),
        )

        try:
            image = Image.open(BytesIO(data_bytes))
            text = await asyncio.to_thread(pytesseract.image_to_string, image)
        except Exception as e:
            return [
                VINResult(
                    vin=None,
                    raw_response=None,
                    metadata={
                        "errors": f"Tesseract Error: {e}",
                    },
                    asset=asset,
                )
            ]

        if not text:
            return [
                VINResult(
                    vin=None,
                    raw_response=text,
                    metadata={
                        "errors": "No text detected",
                    },
                    asset=asset,
                )
            ]

        results = []
        vins, corrections = VINProcessor.extract_from_text(text)
        for vin in vins:
            results.append(
                VINResult(
                    vin=vin,
                    raw_response=text,
                    metadata={"corrections": corrections, "text": text},
                    asset=asset,
                )
            )

        return results or [
            VINResult(
                vin=None,
                raw_response=text,
                metadata={"text": text},
                asset=asset,
            )
        ]
