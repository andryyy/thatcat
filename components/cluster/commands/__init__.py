from .plugin import CommandPlugin


class CommandRegistry:
    def __init__(self):
        self._commands = {}

    def register(self, plugin: "CommandPlugin"):
        if plugin.name in self._commands:
            raise ValueError(f"Command '{plugin.name}' already registered.")
        self._commands[plugin.name] = plugin

    def get(self, name: str) -> CommandPlugin | None:
        return self._commands.get(name)

    def all(self):
        return self._commands.values()
