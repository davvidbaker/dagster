from collections.abc import Sequence
from typing import Optional

from dagster_components import ComponentSchema, ResolutionContext, Resolver, resolver
from dagster_components.core.schema.objects import AssetAttributesSchema, OpSpecSchema


class ShellScriptSchema(ComponentSchema):
    script_path: str
    asset_attributes: Sequence[AssetAttributesSchema]
    op: Optional[OpSpecSchema] = None


@resolver(fromtype=ShellScriptSchema)
class ShellScriptResolver(Resolver[ShellScriptSchema]):
    def resolve_my_object(self, context: ResolutionContext): ...
