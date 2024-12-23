import functools
from collections.abc import Iterator, Mapping, Sequence
from enum import Enum
from hashlib import sha256
from typing import TYPE_CHECKING, Callable, Final, NamedTuple, Optional, Union

from dagster import _check as check
from dagster._annotations import beta, confirmed_deprecated
from dagster._core.loader import LoadingContext
from dagster._utils.cached_method import cached_method

if TYPE_CHECKING:
    from dagster._core.definitions.base_asset_graph import BaseAssetGraph
    from dagster._core.definitions.events import (
        AssetKey,
        AssetKeyPartitionKey,
        AssetMaterialization,
        AssetObservation,
    )
    from dagster._core.event_api import EventLogRecord
    from dagster._core.events.log import EventLogEntry
    from dagster._core.instance import DagsterInstance
    from dagster._utils.caching_instance_queryer import CachingInstanceQueryer


class UnknownValue:
    pass


UNKNOWN_VALUE: Final[UnknownValue] = UnknownValue()


class DataVersion(
    NamedTuple(
        "_DataVersion",
        [("value", str)],
    )
):
    """(Experimental) Represents a data version for an asset.

    Args:
        value (str): An arbitrary string representing a data version.
    """

    def __new__(
        cls,
        value: str,
    ):
        return super().__new__(
            cls,
            value=check.str_param(value, "value"),
        )


@beta
class DataVersionsByPartition(
    NamedTuple(
        "_DataVersionsByPartition", [("data_versions_by_partition", Mapping[str, DataVersion])]
    )
):
    def __new__(
        cls,
        data_versions_by_partition: Mapping[str, Union[str, DataVersion]],
    ):
        check.dict_param(
            data_versions_by_partition,
            "data_versions_by_partition",
            key_type=str,
            value_type=(str, DataVersion),
        )
        return super().__new__(
            cls,
            data_versions_by_partition={
                partition: DataVersion(version) if isinstance(version, str) else version
                for partition, version in data_versions_by_partition.items()
            },
        )


DEFAULT_DATA_VERSION: Final[DataVersion] = DataVersion("INITIAL")
NULL_DATA_VERSION: Final[DataVersion] = DataVersion("NULL")
UNKNOWN_DATA_VERSION: Final[DataVersion] = DataVersion("UNKNOWN")
NULL_EVENT_POINTER: Final[str] = "NULL"


class DataProvenance(
    NamedTuple(
        "_DataProvenance",
        [
            ("code_version", str),
            ("input_data_versions", Mapping["AssetKey", DataVersion]),
            ("input_storage_ids", Mapping["AssetKey", Optional[int]]),
            ("is_user_provided", bool),
        ],
    )
):
    """(Experimental) Provenance information for an asset materialization.

    Args:
        code_version (str): The code version of the op that generated a materialization.
        input_data_versions (Mapping[AssetKey, DataVersion]): The data versions of the
            inputs used to generate a materialization.
        is_user_provided (bool): True if the data version was provided directly by the user, false
            if it was generated by Dagster.
    """

    def __new__(
        cls,
        code_version: str,
        input_data_versions: Mapping["AssetKey", DataVersion],
        input_storage_ids: Mapping["AssetKey", Optional[int]],
        is_user_provided: bool,
    ):
        from dagster._core.definitions.events import AssetKey

        return super().__new__(
            cls,
            code_version=check.str_param(code_version, "code_version"),
            input_data_versions=check.mapping_param(
                input_data_versions,
                "input_data_versions",
                key_type=AssetKey,
                value_type=DataVersion,
            ),
            input_storage_ids=check.mapping_param(
                input_storage_ids,
                "input_storage_ids",
                key_type=AssetKey,
            ),
            is_user_provided=check.bool_param(is_user_provided, "is_user_provided"),
        )

    # In theory an asset key should always be absent/present in both input_data_versions and
    # input_storage_ids, but in the wild we've observed at least one instance
    # where this was not true, resulting in an error. This method can be used
    # as a defensive check.
    def has_input_asset(self, key: "AssetKey") -> bool:
        return key in self.input_data_versions and key in self.input_storage_ids

    @staticmethod
    def from_tags(tags: Mapping[str, str]) -> Optional["DataProvenance"]:
        code_version = tags.get(CODE_VERSION_TAG)
        if code_version is None:
            return None
        input_data_versions = {
            _asset_key_from_tag(k): DataVersion(v)
            for k, v in tags.items()
            if k.startswith(INPUT_DATA_VERSION_TAG_PREFIX)
            or k.startswith(_OLD_INPUT_DATA_VERSION_TAG_PREFIX)
        }
        input_storage_ids = {
            _asset_key_from_tag(k): int(v) if v != NULL_EVENT_POINTER else None
            for k, v in tags.items()
            if k.startswith(INPUT_EVENT_POINTER_TAG_PREFIX)
        }
        is_user_provided = tags.get(DATA_VERSION_IS_USER_PROVIDED_TAG) == "true"
        return DataProvenance(
            code_version, input_data_versions, input_storage_ids, is_user_provided
        )

    @property
    @confirmed_deprecated(
        breaking_version="2.0", additional_warn_text="Use `input_data_versions` instead."
    )
    def input_logical_versions(self) -> Mapping["AssetKey", DataVersion]:
        return self.input_data_versions


def _asset_key_from_tag(tag: str) -> "AssetKey":
    from dagster._core.definitions.events import AssetKey

    # Everything after the 2nd slash is the asset key
    return AssetKey.from_user_string(tag.split("/", maxsplit=2)[-1])


# ########################
# ##### TAG KEYS
# ########################

DATA_VERSION_TAG: Final[str] = "dagster/data_version"
_OLD_DATA_VERSION_TAG: Final[str] = "dagster/logical_version"
CODE_VERSION_TAG: Final[str] = "dagster/code_version"
INPUT_DATA_VERSION_TAG_PREFIX: Final[str] = "dagster/input_data_version"
_OLD_INPUT_DATA_VERSION_TAG_PREFIX: Final[str] = "dagster/input_logical_version"
INPUT_EVENT_POINTER_TAG_PREFIX: Final[str] = "dagster/input_event_pointer"
DATA_VERSION_IS_USER_PROVIDED_TAG = "dagster/data_version_is_user_provided"


def read_input_data_version_from_tags(
    tags: Mapping[str, str], input_key: "AssetKey"
) -> Optional[DataVersion]:
    value = tags.get(
        get_input_data_version_tag(input_key, prefix=INPUT_DATA_VERSION_TAG_PREFIX)
    ) or tags.get(get_input_data_version_tag(input_key, prefix=_OLD_INPUT_DATA_VERSION_TAG_PREFIX))
    return DataVersion(value) if value is not None else None


def get_input_data_version_tag(
    input_key: "AssetKey", prefix: str = INPUT_DATA_VERSION_TAG_PREFIX
) -> str:
    return f"{prefix}/{input_key.to_user_string()}"


def get_input_event_pointer_tag(input_key: "AssetKey") -> str:
    return f"{INPUT_EVENT_POINTER_TAG_PREFIX}/{input_key.to_user_string()}"


# ########################
# ##### COMPUTE / EXTRACT
# ########################


def compute_logical_data_version(
    code_version: Union[str, UnknownValue],
    input_data_versions: Mapping["AssetKey", DataVersion],
) -> DataVersion:
    """Compute a data version for a value as a hash of input data versions and code version.

    Args:
        code_version (str): The code version of the computation.
        input_data_versions (Mapping[AssetKey, DataVersion]): The data versions of the inputs.

    Returns:
        DataVersion: The computed version as a `DataVersion`.
    """
    from dagster._core.definitions.events import AssetKey

    check.inst_param(code_version, "code_version", (str, UnknownValue))
    check.mapping_param(
        input_data_versions, "input_versions", key_type=AssetKey, value_type=DataVersion
    )

    if (
        isinstance(code_version, UnknownValue)
        or UNKNOWN_DATA_VERSION in input_data_versions.values()
    ):
        return UNKNOWN_DATA_VERSION

    ordered_input_versions = [
        input_data_versions[k] for k in sorted(input_data_versions.keys(), key=str)
    ]
    all_inputs = (code_version, *(v.value for v in ordered_input_versions))

    hash_sig = sha256()
    hash_sig.update(bytearray("".join(all_inputs), "utf8"))
    return DataVersion(hash_sig.hexdigest())


def extract_data_version_from_entry(
    entry: "EventLogEntry",
) -> Optional[DataVersion]:
    tags = entry.tags or {}
    value = tags.get(DATA_VERSION_TAG) or tags.get(_OLD_DATA_VERSION_TAG)
    return None if value is None else DataVersion(value)


def extract_data_provenance_from_entry(
    entry: "EventLogEntry",
) -> Optional[DataProvenance]:
    tags = entry.tags or {}
    return DataProvenance.from_tags(tags)


# ########################
# ##### STALENESS OPERATIONS
# ########################


class StaleStatus(Enum):
    MISSING = "MISSING"
    STALE = "STALE"
    FRESH = "FRESH"


@functools.total_ordering
class StaleCauseCategory(Enum):
    CODE = "CODE"
    DATA = "DATA"
    DEPENDENCIES = "DEPENDENCIES"

    def __lt__(self, other: "StaleCauseCategory"):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class StaleCause(
    NamedTuple(
        "_StaleCause",
        [
            ("key", "AssetKeyPartitionKey"),
            ("category", StaleCauseCategory),
            ("reason", str),
            ("dependency", Optional["AssetKeyPartitionKey"]),
            ("children", Optional[Sequence["StaleCause"]]),
        ],
    )
):
    def __new__(
        cls,
        key: Union["AssetKey", "AssetKeyPartitionKey"],
        category: StaleCauseCategory,
        reason: str,
        dependency: Optional[Union["AssetKey", "AssetKeyPartitionKey"]] = None,
        children: Optional[Sequence["StaleCause"]] = None,
    ):
        from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey

        return super().__new__(
            cls,
            AssetKeyPartitionKey(key) if isinstance(key, AssetKey) else key,
            category,
            reason,
            AssetKeyPartitionKey(dependency) if isinstance(dependency, AssetKey) else dependency,
            children,
        )

    @property
    def asset_key(self) -> "AssetKey":
        return self.key.asset_key

    @property
    def partition_key(self) -> Optional[str]:
        return self.key.partition_key

    @property
    def dependency_asset_key(self) -> Optional["AssetKey"]:
        return self.dependency.asset_key if self.dependency else None

    @property
    def dependency_partition_key(self) -> Optional[str]:
        return self.dependency.partition_key if self.dependency else None

    @property
    def sort_key(self) -> str:
        if not hasattr(self, "_sort_key"):
            self._sort_key = f"{self.key}/{self.dependency}" if self.dependency else str(self.key)
        return self._sort_key

    @property
    def dedupe_key(self) -> int:
        # subset of properties that are safe to hash
        safe_tup = (self.key, self.category, self.reason, self.dependency)
        return hash(safe_tup)


# If a partition has greater than this number of dependencies, we don't check
# this edge for updated data or propagate other stale causes through this edge.
# This constraint can be removed when we have thoroughly tested performance for
# large upstream partition counts.
SKIP_PARTITION_DATA_VERSION_DEPENDENCY_THRESHOLD = 100

# If an asset is self-dependent and has  greater than this number of partitions, we don't check the
# self-edge for updated data or propagate other stale causes through the edge. That is because the
# current logic will recurse to the first partition, potentially throwing a recursion error. This
# constraint can be removed when we have thoroughly tested performance for large partition counts on
# self-dependent assets.
SKIP_PARTITION_DATA_VERSION_SELF_DEPENDENCY_THRESHOLD = 100


class CachingStaleStatusResolver:
    """Used to resolve data version information. Avoids redundant database
    calls that would otherwise occur. Intended for use within the scope of a
    single "request" (e.g. GQL request, RunRequest resolution).
    """

    _instance: "DagsterInstance"
    _instance_queryer: Optional["CachingInstanceQueryer"]
    _asset_graph: Optional["BaseAssetGraph"]
    _asset_graph_load_fn: Optional[Callable[[], "BaseAssetGraph"]]

    def __init__(
        self,
        instance: "DagsterInstance",
        asset_graph: Union["BaseAssetGraph", Callable[[], "BaseAssetGraph"]],
        loading_context: LoadingContext,
        instance_queryer: Optional["CachingInstanceQueryer"] = None,
    ):
        from dagster._core.definitions.base_asset_graph import BaseAssetGraph

        self._instance = instance
        self._instance_queryer = instance_queryer
        self._loading_context = loading_context
        if isinstance(asset_graph, BaseAssetGraph):
            self._asset_graph = asset_graph
            self._asset_graph_load_fn = None
        else:
            self._asset_graph = None
            self._asset_graph_load_fn = asset_graph

    def get_status(self, key: "AssetKey", partition_key: Optional[str] = None) -> StaleStatus:
        from dagster._core.definitions.events import AssetKeyPartitionKey

        return self._get_status(key=AssetKeyPartitionKey(key, partition_key))

    def get_stale_causes(
        self, key: "AssetKey", partition_key: Optional[str] = None
    ) -> Sequence[StaleCause]:
        from dagster._core.definitions.events import AssetKeyPartitionKey

        return self._get_stale_causes(key=AssetKeyPartitionKey(key, partition_key))

    def get_stale_root_causes(
        self, key: "AssetKey", partition_key: Optional[str] = None
    ) -> Sequence[StaleCause]:
        from dagster._core.definitions.events import AssetKeyPartitionKey

        return self._get_stale_root_causes(key=AssetKeyPartitionKey(key, partition_key))

    def get_current_data_version(
        self, key: "AssetKey", partition_key: Optional[str] = None
    ) -> DataVersion:
        from dagster._core.definitions.events import AssetKeyPartitionKey

        return self._get_current_data_version(key=AssetKeyPartitionKey(key, partition_key))

    @cached_method
    def _get_status(self, key: "AssetKeyPartitionKey") -> StaleStatus:
        # The status loader does not support querying for the stale status of a
        # partitioned asset without specifying a partition, so we return here.
        asset = self.asset_graph.get(key.asset_key)
        if asset.is_partitioned and not key.partition_key:
            return StaleStatus.FRESH
        else:
            current_version = self._get_current_data_version(key=key)
            if current_version == NULL_DATA_VERSION:
                return StaleStatus.MISSING
            elif asset.is_external:
                return StaleStatus.FRESH
            else:
                causes = self._get_stale_causes(key=key)
                return StaleStatus.FRESH if len(causes) == 0 else StaleStatus.STALE

    @cached_method
    def _get_stale_causes(self, key: "AssetKeyPartitionKey") -> Sequence[StaleCause]:
        # Querying for the stale status of a partitioned asset without specifying a partition key
        # is strictly speaking undefined, but we return an empty list here (from which FRESH status
        # is inferred) for backcompat.
        asset = self.asset_graph.get(key.asset_key)
        if asset.is_partitioned and not key.partition_key:
            return []
        elif asset.is_external:
            return []
        else:
            current_version = self._get_current_data_version(key=key)
            if current_version == NULL_DATA_VERSION:
                return []
            else:
                return sorted(
                    self._get_stale_causes_materialized(key=key), key=lambda cause: cause.sort_key
                )

    def _is_dep_updated(self, provenance: DataProvenance, dep_key: "AssetKeyPartitionKey") -> bool:
        dep_asset = self.asset_graph.get(dep_key.asset_key)
        if dep_key.partition_key is None:
            current_data_version = self._get_current_data_version(key=dep_key)
            return provenance.input_data_versions[dep_key.asset_key] != current_data_version
        else:
            cursor = provenance.input_storage_ids[dep_key.asset_key]
            updated_record = self._instance.get_latest_data_version_record(
                dep_key.asset_key,
                dep_asset.is_external,
                dep_key.partition_key,
                after_cursor=cursor,
            )
            if updated_record:
                previous_record = self._instance.get_latest_data_version_record(
                    dep_key.asset_key,
                    dep_asset.is_external,
                    dep_key.partition_key,
                    before_cursor=cursor + 1 if cursor else None,
                )
                previous_version = (
                    extract_data_version_from_entry(previous_record.event_log_entry)
                    if previous_record
                    else None
                )
                updated_version = extract_data_version_from_entry(updated_record.event_log_entry)
                return previous_version != updated_version
            else:
                return False

    def _get_stale_causes_materialized(self, key: "AssetKeyPartitionKey") -> Iterator[StaleCause]:
        from dagster._core.definitions.events import AssetKeyPartitionKey

        code_version = self.asset_graph.get(key.asset_key).code_version
        provenance = self._get_current_data_provenance(key=key)

        asset_deps = self.asset_graph.get(key.asset_key).parent_keys

        # only used if no provenance available
        materialization = check.not_none(self._get_latest_data_version_record(key=key))
        materialization_time = materialization.timestamp

        if provenance:
            if code_version and code_version != provenance.code_version:
                yield StaleCause(key, StaleCauseCategory.CODE, "has a new code version")

            removed_deps = set(provenance.input_data_versions.keys()) - set(asset_deps)
            for dep_key in removed_deps:
                yield StaleCause(
                    key,
                    StaleCauseCategory.DEPENDENCIES,
                    f"removed dependency on {dep_key.to_user_string()}",
                    AssetKeyPartitionKey(dep_key, None),
                )

        # If a partition has greater than or equal to SKIP_PARTITION_DATA_VERSION_DEPENDENCY_THRESHOLD of
        # dependencies, it is not included in partition_deps. This is for performance reasons. This
        # constraint can be removed when we have thoroughly tested performance for large upstream
        # partition counts.
        partition_deps = self._get_partition_dependencies(key=key)
        for dep_key in sorted(partition_deps):
            dep_asset = self.asset_graph.get(dep_key.asset_key)
            if provenance:
                if not provenance.has_input_asset(dep_key.asset_key):
                    yield StaleCause(
                        key,
                        StaleCauseCategory.DEPENDENCIES,
                        f"has a new dependency on {dep_key.asset_key.to_user_string()}",
                        dep_key,
                    )
                elif self._is_dep_updated(provenance, dep_key):
                    report_data_version = (
                        dep_asset.code_version is not None
                        or self._is_current_data_version_user_provided(key=dep_key)
                    )
                    yield StaleCause(
                        key,
                        StaleCauseCategory.DATA,
                        (
                            "has a new dependency data version"
                            if report_data_version
                            else "has a new dependency materialization"
                        ),
                        dep_key,
                        [
                            StaleCause(
                                dep_key,
                                StaleCauseCategory.DATA,
                                (
                                    # Assets with no defined code version will get a new data version on
                                    # every materialization, so we just report new materialization for
                                    # this case since the user likely hasn't engaged with data versions.
                                    "has a new data version"
                                    if report_data_version
                                    else "has a new materialization"
                                ),
                            )
                        ],
                    )
            # If no provenance and dep is a materializable asset, then use materialization
            # timestamps instead of versions this should be removable eventually since
            # provenance is on all newer materializations. If dep is a source, then we'll never
            # provide a stale reason here.
            elif not dep_asset.is_external:
                dep_materialization = self._get_latest_data_version_record(key=dep_key)
                if dep_materialization is None:
                    # The input must be new if it has no materialization
                    yield StaleCause(key, StaleCauseCategory.DATA, "has a new input", dep_key)
                elif dep_materialization.timestamp > materialization_time:
                    yield StaleCause(
                        key,
                        StaleCauseCategory.DATA,
                        "has a new dependency materialization",
                        dep_key,
                        [
                            StaleCause(
                                dep_key,
                                StaleCauseCategory.DATA,
                                "has a new materialization",
                            )
                        ],
                    )

    @cached_method
    def _get_stale_root_causes(self, key: "AssetKeyPartitionKey") -> Sequence[StaleCause]:
        candidates = self._get_stale_causes(key=key)
        visited = set()
        root_causes = []
        while candidates:
            next_candidates: list[StaleCause] = []
            for cause in candidates:
                if cause.dedupe_key not in visited:
                    if cause.children is None:
                        root_causes.append(cause)
                    else:
                        next_candidates.extend(cause.children)
                    visited.add(cause.dedupe_key)

            candidates = next_candidates
        return root_causes

    @property
    def asset_graph(self) -> "BaseAssetGraph":
        if self._asset_graph is None:
            self._asset_graph = check.not_none(self._asset_graph_load_fn)()
        return self._asset_graph

    # This is lazily constructed because it depends on the asset graph, which needs to be lazily
    # constructed for GQL performance reasons.
    @property
    def instance_queryer(self) -> "CachingInstanceQueryer":
        from dagster._utils.caching_instance_queryer import CachingInstanceQueryer

        if self._instance_queryer is None:
            self._instance_queryer = CachingInstanceQueryer(
                self._instance, self.asset_graph, self._loading_context
            )
        return self._instance_queryer

    @cached_method
    def _get_current_data_version(self, *, key: "AssetKeyPartitionKey") -> DataVersion:
        # Currently we can only use asset records, which are fetched in one shot, for non-source
        # assets. This is because the most recent AssetObservation is not stored on the AssetRecord.
        record = self._get_latest_data_version_record(key=key)
        if self.asset_graph.get(key.asset_key).is_external and record is None:
            return DEFAULT_DATA_VERSION
        elif record is None:
            return NULL_DATA_VERSION
        else:
            data_version = extract_data_version_from_entry(record.event_log_entry)
            return data_version or DEFAULT_DATA_VERSION

    @cached_method
    def _is_current_data_version_user_provided(self, *, key: "AssetKeyPartitionKey") -> bool:
        if self.asset_graph.get(key.asset_key).is_external:
            return True
        else:
            provenance = self._get_current_data_provenance(key=key)
            return provenance is not None and provenance.is_user_provided

    @cached_method
    def _get_current_data_provenance(
        self, *, key: "AssetKeyPartitionKey"
    ) -> Optional[DataProvenance]:
        record = self._get_latest_data_version_record(key=key)
        if record is None:
            return None
        else:
            return extract_data_provenance_from_entry(record.event_log_entry)

    # Volatility means that an asset is assumed to be constantly changing. We assume that observable
    # source assets are non-volatile, since the primary purpose of the observation function is to
    # determine if a source asset has changed. We assume that regular assets are volatile if they
    # are at the root of the graph (have no dependencies) or are downstream of a volatile asset.
    @cached_method
    def _is_volatile(self, *, key: "AssetKey") -> bool:
        asset = self.asset_graph.get(key)
        if asset.is_external:
            return asset.is_observable
        else:
            deps = asset.get(key).parent_keys
            return len(deps) == 0 or any(self._is_volatile(key=dep_key) for dep_key in deps)

    @cached_method
    def _get_latest_data_version_event(
        self, *, key: "AssetKeyPartitionKey"
    ) -> Optional[Union["AssetMaterialization", "AssetObservation"]]:
        record = self._get_latest_data_version_record(key=key)
        if record:
            entry = record.event_log_entry
            return entry.asset_materialization or entry.asset_observation
        else:
            return None

    @cached_method
    def _get_latest_data_version_record(
        self, key: "AssetKeyPartitionKey"
    ) -> Optional["EventLogRecord"]:
        return self.instance_queryer.get_latest_materialization_or_observation_record(
            asset_partition=key
        )

    # If a partition has greater than or equal to SKIP_PARTITION_DATA_VERSION_DEPENDENCY_THRESHOLD
    # of dependencies, or is downstream of a time window partition with an AllPartitionsMapping,
    # it is not included in partition_deps. This is for performance reasons. Besides this, the
    # logic here largely replicates `asset_graph.get_parents_partitions`.
    #
    # Similarly, If an asset is self-dependent and has greater than or equal to
    # SKIP_PARTITION_DATA_VERSION_SELF_DEPENDENCY_THRESHOLD partitions, we don't check the
    # self-edge for updated data or propagate other stale causes through the edge. That is because
    # the current logic will recurse to the first partition, potentially throwing a recursion error.
    @cached_method
    def _get_partition_dependencies(
        self, *, key: "AssetKeyPartitionKey"
    ) -> Sequence["AssetKeyPartitionKey"]:
        from dagster import AllPartitionMapping
        from dagster._core.definitions.events import AssetKeyPartitionKey
        from dagster._core.definitions.time_window_partitions import TimeWindowPartitionsDefinition

        asset_deps = self.asset_graph.get(key.asset_key).parent_keys

        deps = []
        for dep_asset_key in asset_deps:
            dep_asset = self.asset_graph.get(dep_asset_key)
            if not dep_asset.is_partitioned:
                deps.append(AssetKeyPartitionKey(dep_asset_key, None))
            elif key.asset_key == dep_asset_key and self._exceeds_self_partition_limit(
                key.asset_key
            ):
                continue
            elif isinstance(
                dep_asset.partitions_def, TimeWindowPartitionsDefinition
            ) and isinstance(
                self.asset_graph.get_partition_mapping(key.asset_key, dep_asset_key),
                AllPartitionMapping,
            ):
                continue
            else:
                upstream_partition_keys = list(
                    self.asset_graph.get_parent_partition_keys_for_child(
                        key.partition_key,
                        dep_asset_key,
                        key.asset_key,
                        dynamic_partitions_store=self._instance,
                        current_time=self.instance_queryer.evaluation_time,
                    ).partitions_subset.get_partition_keys()
                )
                if len(upstream_partition_keys) < SKIP_PARTITION_DATA_VERSION_DEPENDENCY_THRESHOLD:
                    deps.extend(
                        [
                            AssetKeyPartitionKey(dep_asset_key, partition_key)
                            for partition_key in upstream_partition_keys
                        ]
                    )
        return deps

    def _exceeds_self_partition_limit(self, asset_key: "AssetKey") -> bool:
        return (
            check.not_none(self.asset_graph.get(asset_key).partitions_def).get_num_partitions(
                dynamic_partitions_store=self._instance
            )
            >= SKIP_PARTITION_DATA_VERSION_SELF_DEPENDENCY_THRESHOLD
        )
