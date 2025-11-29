import json
import os
import sys

from config.defaults import ACCEPT_LANGUAGES

_main_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

class LangDict(dict):
    def __init__(self, lang="en", **kwargs):
        if lang != "en":
            with open(f"{_main_dir}/lang/{lang}.json", encoding="utf-8") as f:
                data = json.load(f)
            super().__init__(data, **kwargs)
        else:
            super().__init__(**kwargs)

    def __missing__(self, key):
        return key


LANG = {k: LangDict(lang=k) for k in ACCEPT_LANGUAGES}
