from dataclasses import dataclass, field
from threading import RLock


class LockedDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = RLock()

    def __getitem__(self, key):
        with self._lock:
            return super().__getitem__(key)

    def __setitem__(self, key, value):
        with self._lock:
            return super().__setitem__(key, value)

    def __delitem__(self, key):
        with self._lock:
            return super().__delitem__(key)

    def update(self, *args, **kwargs):
        with self._lock:
            return super().update(*args, **kwargs)

    def get(self, key, default=None):
        with self._lock:
            return super().get(key, default)

    # etc. — you can override more as needed


class LockedSet(set):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = RLock()

    def add(self, elem):
        with self._lock:
            return super().add(elem)

    def discard(self, elem):
        with self._lock:
            return super().discard(elem)

    def remove(self, elem):
        with self._lock:
            return super().remove(elem)

    def __contains__(self, elem):
        with self._lock:
            return super().__contains__(elem)


@dataclass
class GlobalState:
    _lock: RLock = field(default_factory=RLock, repr=False)
    _challenge_options: LockedDict = field(default_factory=LockedDict)
    locations: LockedDict = field(default_factory=LockedDict)
    promote_users: LockedSet[str] = field(default_factory=LockedSet)
    queued_user_tasks: LockedDict[str, object] = field(default_factory=LockedDict)
    query_cache: LockedDict[str, object] = field(default_factory=LockedDict)
    session_validated: LockedDict[str, bool] = field(default_factory=LockedDict)
    sign_in_tokens: LockedDict[str, str] = field(default_factory=LockedDict)
    terminal_tokens: LockedDict[str, str] = field(default_factory=LockedDict)
    ws_connections: LockedDict[str, object] = field(default_factory=LockedDict)


STATE = GlobalState()
