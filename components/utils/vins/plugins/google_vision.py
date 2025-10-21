import base64
import json

from .base import VINExtractorPlugin, DataType, VINResult, VINProcessor

from PIL import Image
from components.utils.requests import async_request
from components.models.system import SystemSettings
from components.models.assets import Asset
from components.cluster import cluster


class GoogleVisionExtractor(VINExtractorPlugin):
    name = "google_vision"
    handles = [DataType.IMAGE]
    priority = 0

    def __init__(self, settings: SystemSettings, feature_types: list = None):
        if not isinstance(settings, SystemSettings):
            raise ValueError("'settings' must be SystemSettings")

        if not settings.google_vision_api_key:
            raise ValueError("'google_vision_api_key' must not be empty")

        self.api_key = settings.google_vision_api_key
        self.feature_types = feature_types or ["TEXT_DETECTION"]

    async def extract(self, data_bytes: bytes, **kwargs) -> VINResult:
        asset = await Asset.create_from_bytes(
            data_bytes,
            compress=True,
            cluster=cluster,
            overlay=None,
        )
        img = Image.open(f"assets/{asset.filename}")
        image_width, image_height = img.size
        image_data = base64.standard_b64encode(data_bytes).decode("utf-8")

        request_response = await async_request(
            f"https://vision.googleapis.com/v1/images:annotate?key={self.api_key}",
            method="POST",
            data={
                "requests": [
                    {
                        "image": {"content": image_data},
                        "features": [{"type": ft} for ft in self.feature_types],
                    }
                ]
            },
            headers={"Content-Type": "application/json"},
        )
        response_status, response_text = request_response

        if response_status >= 400:
            try:
                response_text = json.loads(response_text)
                error = response_text.get("error")
                error = json.dumps(error)
            except Exception:
                error = response_text
            finally:
                return VINResult(
                    vins=[None],
                    raw_response=response_text,
                    metadata={
                        "notes": f"API Error: {error}",
                    },
                    asset=asset,
                )

        response_text = json.loads(response_text)
        try:
            text = response_text["responses"][0]["fullTextAnnotation"]["text"]
        except (KeyError, IndexError):
            return VINResult(
                vins=[None],
                raw_response=response_text,
                metadata={
                    "notes": "No text detected in image",
                },
                asset=asset,
            )

        asset.overlay = self._generate_svg_overlay(
            response_text, image_width, image_height
        )
        candidates = VINProcessor.extract_candidates(text)
        processed_vins, notes = VINProcessor.process_vin_candidates(candidates)

        return VINResult(
            vins=processed_vins or [None],
            raw_response=response_text,
            metadata={"notes": notes},
            asset=asset,
        )

    @staticmethod
    def _generate_svg_overlay(
        response_data: dict, image_width: int, image_height: int
    ) -> str:
        try:
            text_annotations = response_data["responses"][0].get("textAnnotations", [])
            if not text_annotations:
                return None

            svg_parts = [
                f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {image_width} {image_height}" '
                f'width="100%" height="100%" style="position: absolute; top: 0; left: 0; pointer-events: none;">'
            ]

            for annotation in text_annotations[1:]:
                vertices = annotation.get("boundingPoly", {}).get("vertices", [])
                if len(vertices) >= 4:
                    points = " ".join(
                        f"{v.get('x', 0)},{v.get('y', 0)}" for v in vertices
                    )
                    svg_parts.append(
                        f'<polygon points="{points}" '
                        f'fill="rgba(0, 255, 0, 0.2)" '
                        f'stroke="rgba(0, 255, 0, 1.0)" '
                        f'stroke-width="2"/>'
                    )

            svg_parts.append("</svg>")
            return "".join(svg_parts)
        except (KeyError, IndexError, TypeError):
            return None
