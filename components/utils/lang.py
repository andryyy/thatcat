import json

from config.defaults import ACCEPT_LANGUAGES


class LangDict(dict):
    def __init__(self, lang="en", **kwargs):
        if lang != "en":
            with open(f"lang/{lang}.json", encoding="utf-8") as f:
                data = json.load(f)
            super().__init__(data, **kwargs)
        else:
            super().__init__(**kwargs)

    def __missing__(self, key):
        return key


LANG = {k: LangDict(lang=k) for k in ACCEPT_LANGUAGES}
