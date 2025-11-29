from .plugin import CommandPlugin


class CommandRegistry:
    def __init__(self):
        self._handlers = {}
        self.callback_commands = set()
        self.requires_callback = set()
        self.commands = set()

    def register(self, plugin: "CommandPlugin"):
        if plugin.name in self._handlers:
            raise ValueError(f"Command '{plugin.name}' already registered.")
        self._handlers[plugin.name] = plugin

        if plugin.is_callback:
            self.callback_commands.add(plugin.name)
        elif plugin.requires_callback:  # a callback cmd can never require a callback
            self.requires_callback.add(plugin.name)

        self.commands.add(plugin.name)

    def get(self, name: str) -> CommandPlugin | None:
        return self._handlers.get(name)

    def all(self):
        return self._handlers.values()
