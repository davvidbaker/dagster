from dagster_components import Component, ComponentLoadContext, component_type
from pydantic import BaseModel

from dagster import Definitions


@component_type(name="shell_command")
class ShellCommand(Component):
    @classmethod
    def get_schema(cls) -> type[BaseModel]: ...

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions: ...
