import importlib
import inspect

from .base import VINExtractorPlugin
from components.logs import logger
from pathlib import Path

EXTRACTORS: set[type[VINExtractorPlugin]] = set()


def load_plugins():
    global EXTRACTORS
    plugins_dir = Path(__file__).parent

    for file_path in plugins_dir.glob("*.py"):
        if file_path.name.startswith("_") or file_path.name == "base.py":
            continue

        try:
            module = importlib.import_module(f".{file_path.stem}", package=__package__)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, VINExtractorPlugin)
                    and obj is not VINExtractorPlugin
                    and obj.__module__ == module.__name__
                ):
                    logger.info(f"Loaded extractor plugin {name}")
                    EXTRACTORS.add(obj)
        except Exception as e:
            logger.error(f"Error loading extractor plugin from {file_path}: {e}")


load_plugins()

__all__ = ["EXTRACTORS"]
