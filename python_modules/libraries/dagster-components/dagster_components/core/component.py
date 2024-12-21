import copy
import dataclasses
import importlib
import importlib.metadata
import inspect
import sys
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
)

import click
from dagster import _check as check
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.utils import is_valid_name
from dagster._core.errors import DagsterError
from dagster._record import IHaveNew, record, record_custom
from dagster._utils import pushd, snakecase
from pydantic import TypeAdapter
from typing_extensions import Self

from dagster_components.core.component_rendering import TemplatedValueResolver, preprocess_value

if TYPE_CHECKING:
    from dagster_components.core.component_decl_builder import YamlComponentDecl


class ComponentDeclNode: ...


@record
class ComponentGenerateRequest:
    component_type_name: str
    component_instance_root_path: Path


class Component(ABC):
    name: ClassVar[Optional[str]] = None
    params_schema: ClassVar = None
    generate_params_schema: ClassVar = None

    @classmethod
    def get_rendering_scope(cls) -> Mapping[str, Any]:
        return {}

    @classmethod
    def generate_files(cls, request: ComponentGenerateRequest, params: Any) -> None:
        from dagster_components.generate import generate_component_yaml

        generate_component_yaml(request, {})

    @abstractmethod
    def build_defs(self, context: "ComponentLoadContext") -> Definitions: ...

    @classmethod
    @abstractmethod
    def load(cls, context: "ComponentLoadContext") -> Self: ...

    @classmethod
    def get_metadata(cls) -> "ComponentInternalMetadata":
        docstring = cls.__doc__
        clean_docstring = _clean_docstring(docstring) if docstring else None

        return {
            "summary": clean_docstring.split("\n\n")[0] if clean_docstring else None,
            "description": clean_docstring if clean_docstring else None,
            "generate_params_schema": cls.generate_params_schema.schema()
            if cls.generate_params_schema
            else None,
            "component_params_schema": cls.params_schema.schema() if cls.params_schema else None,
        }

    @classmethod
    def get_description(cls) -> Optional[str]:
        return inspect.getdoc(cls)


def _clean_docstring(docstring: str) -> str:
    lines = docstring.strip().splitlines()
    first_line = lines[0]
    if len(lines) == 1:
        return first_line
    else:
        rest = textwrap.dedent("\n".join(lines[1:]))
        return f"{first_line}\n{rest}"


def _get_click_cli_help(command: click.Command) -> str:
    with click.Context(command) as ctx:
        formatter = click.formatting.HelpFormatter()
        param_records = [
            p.get_help_record(ctx) for p in command.get_params(ctx) if p.name != "help"
        ]
        formatter.write_dl([pr for pr in param_records if pr])
        return formatter.getvalue()


class ComponentInternalMetadata(TypedDict):
    summary: Optional[str]
    description: Optional[str]
    generate_params_schema: Optional[Any]  # json schema
    component_params_schema: Optional[Any]  # json schema


class ComponentMetadata(ComponentInternalMetadata):
    name: str
    package: str


def get_entry_points_from_python_environment(group: str) -> Sequence[importlib.metadata.EntryPoint]:
    if sys.version_info >= (3, 10):
        return importlib.metadata.entry_points(group=group)
    else:
        return importlib.metadata.entry_points().get(group, [])


COMPONENTS_ENTRY_POINT_GROUP = "dagster.components"
BUILTIN_COMPONENTS_ENTRY_POINT_BASE = "dagster_components"
BUILTIN_MAIN_COMPONENT_ENTRY_POINT = BUILTIN_COMPONENTS_ENTRY_POINT_BASE
BUILTIN_TEST_COMPONENT_ENTRY_POINT = ".".join([BUILTIN_COMPONENTS_ENTRY_POINT_BASE, "test"])


class ComponentRegistry:
    @classmethod
    def from_entry_point_discovery(
        cls, builtin_component_lib: str = BUILTIN_MAIN_COMPONENT_ENTRY_POINT
    ) -> "ComponentRegistry":
        """Discover components registered in the Python environment via the `dagster_components` entry point group.

        `dagster-components` itself registers multiple component entry points. We call these
        "builtin" component libraries. The `dagster_components` entry point resolves to published
        components and is loaded by default. Other entry points resolve to various sets of test
        components. This method will only ever load one builtin component library.

        Args:
            builtin-component-lib (str): Specifies the builtin components library to load. Builtin
            copmonents libraries are defined under entry points with names matching the pattern
            `dagster_components*`. Only one builtin component library can be loaded at a time.
            Defaults to `dagster_components`, the standard set of published components.
        """
        components: Dict[str, Type[Component]] = {}
        for entry_point in get_entry_points_from_python_environment(COMPONENTS_ENTRY_POINT_GROUP):
            # Skip builtin entry points that are not the specified builtin component library.
            if (
                entry_point.name.startswith(BUILTIN_COMPONENTS_ENTRY_POINT_BASE)
                and not entry_point.name == builtin_component_lib
            ):
                continue

            root_module = entry_point.load()
            if not isinstance(root_module, ModuleType):
                raise DagsterError(
                    f"Invalid entry point {entry_point.name} in group {COMPONENTS_ENTRY_POINT_GROUP}. "
                    f"Value expected to be a module, got {root_module}."
                )
            for component in get_registered_components_in_module(root_module):
                key = f"{entry_point.name}.{get_component_name(component)}"
                components[key] = component

        return cls(components)

    def __init__(self, components: Dict[str, Type[Component]]):
        self._components: Dict[str, Type[Component]] = copy.copy(components)

    @staticmethod
    def empty() -> "ComponentRegistry":
        return ComponentRegistry({})

    def register(self, name: str, component: Type[Component]) -> None:
        if name in self._components:
            raise DagsterError(f"There is an existing component registered under {name}")
        self._components[name] = component

    def has(self, name: str) -> bool:
        return name in self._components

    def get(self, name: str) -> Type[Component]:
        return self._components[name]

    def keys(self) -> Iterable[str]:
        return self._components.keys()

    def __repr__(self) -> str:
        return f"<ComponentRegistry {list(self._components.keys())}>"


def get_registered_components_in_module(module: ModuleType) -> Iterable[Type[Component]]:
    from dagster._core.definitions.module_loaders.load_assets_from_modules import (
        find_subclasses_in_module,
    )

    for component in find_subclasses_in_module(module, (Component,)):
        if is_registered_component(component):
            yield component


T = TypeVar("T")


@record_custom
class ComponentKey(IHaveNew):
    """Uniquely identifies a component instance within a component hierarchy. Parts
    typically correspond to the folder structure of the component hierarchy.
    """

    def __new__(cls, parts: Iterable[str]):
        for part in parts:
            if not is_valid_name(part):
                check.param_invariant(False, "parts", f"Invalid key part {part}")

        return super().__new__(cls, parts=parts)

    parts: List[str]

    @staticmethod
    def root() -> "ComponentKey":
        return ComponentKey(parts=[])

    def child(self, part: str) -> "ComponentKey":
        return ComponentKey(parts=self.parts + [part])

    @property
    def dot_path(self) -> str:
        return ".".join(self.parts)


@dataclass
class ComponentLoadContext:
    resources: Mapping[str, object]
    registry: ComponentRegistry
    decl_node: Optional[ComponentDeclNode]
    templated_value_resolver: TemplatedValueResolver
    code_location_name: str

    @staticmethod
    def for_test(
        *,
        resources: Optional[Mapping[str, object]] = None,
        registry: Optional[ComponentRegistry] = None,
        decl_node: Optional[ComponentDeclNode] = None,
        code_location_name: Optional[str] = None,
    ) -> "ComponentLoadContext":
        return ComponentLoadContext(
            resources=resources or {},
            registry=registry or ComponentRegistry.empty(),
            decl_node=decl_node,
            templated_value_resolver=TemplatedValueResolver.default(),
            code_location_name=code_location_name or "test",
        )

    @cached_property
    def yaml_decl_node(self) -> "YamlComponentDecl":
        from dagster_components.core.component_decl_builder import YamlComponentDecl

        if not isinstance(self.decl_node, YamlComponentDecl):
            check.failed(f"Unsupported decl_node type {type(self.decl_node)}")
        return self.decl_node

    @property
    def path(self) -> Path:
        return self.yaml_decl_node.path

    @property
    def component_instance_key(self) -> ComponentKey:
        return self.yaml_decl_node.key

    @property
    def component_type(self) -> str:
        return self.yaml_decl_node.component_file_model.type

    def with_rendering_scope(self, rendering_scope: Mapping[str, Any]) -> "ComponentLoadContext":
        return dataclasses.replace(
            self,
            templated_value_resolver=self.templated_value_resolver.with_context(**rendering_scope),
        )

    def for_decl_node(self, decl_node: ComponentDeclNode) -> "ComponentLoadContext":
        return dataclasses.replace(self, decl_node=decl_node)

    def _raw_params(self) -> Optional[Mapping[str, Any]]:
        from dagster_components.core.component_decl_builder import YamlComponentDecl

        if not isinstance(self.decl_node, YamlComponentDecl):
            check.failed(f"Unsupported decl_node type {type(self.decl_node)}")
        return self.decl_node.component_file_model.params

    def load_params(self, params_schema: Type[T]) -> T:
        with pushd(str(self.path)):
            preprocessed_params = preprocess_value(
                self.templated_value_resolver, self._raw_params(), params_schema
            )
            return TypeAdapter(params_schema).validate_python(preprocessed_params)


def get_python_module_name(context: ComponentLoadContext, subkey: str) -> str:
    """Utility function to generate a unique name for a Python module that is being dynamically
    loaded for a particular component instance.
    """
    return (
        f"__dagster_code_location__.{context.code_location_name}."
        f"__component_instance__.{context.component_instance_key.dot_path}."
        f"__{context.component_type}__.{subkey}"
    )


COMPONENT_REGISTRY_KEY_ATTR = "__dagster_component_registry_key"


def component_type(cls: Optional[Type[Component]] = None, *, name: Optional[str] = None) -> Any:
    """Decorator for registering a component type. You must annotate a component
    type with this decorator in order for it to be inspectable and loaded by tools.

    Args:
        cls (Optional[Type[Component]]): The target of the decorator: the component class
            to register. The class must inherit from Component.
        name (Optional[str]): The name to register the component type under. If not
            provided, the name will be the snake-cased version of the class name. The
            name is used as a key in operations like scaffolding and loading.
    """
    if cls is None:

        def wrapper(actual_cls: Type[Component]) -> Type[Component]:
            check.inst_param(actual_cls, "actual_cls", type)
            setattr(
                actual_cls,
                COMPONENT_REGISTRY_KEY_ATTR,
                name or snakecase(actual_cls.__name__),
            )
            return actual_cls

        return wrapper
    else:
        # called without params
        check.inst_param(cls, "cls", type)
        setattr(cls, COMPONENT_REGISTRY_KEY_ATTR, name or snakecase(cls.__name__))
        return cls


def is_registered_component(cls: Type) -> bool:
    return hasattr(cls, COMPONENT_REGISTRY_KEY_ATTR)


def get_component_name(component_type: Type[Component]) -> str:
    check.param_invariant(
        is_registered_component(component_type),
        "component_type",
        "Expected a registered component. Use @component to register a component.",
    )
    return getattr(component_type, COMPONENT_REGISTRY_KEY_ATTR)
