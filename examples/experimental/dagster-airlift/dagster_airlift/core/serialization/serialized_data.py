from functools import cached_property
from typing import AbstractSet, Any, Dict, List, Mapping, NamedTuple, Optional, Set

from dagster import (
    AssetKey,
    _check as check,
)
from dagster._record import record
from dagster._serdes import whitelist_for_serdes


@whitelist_for_serdes
@record
class TaskInfo:
    webserver_url: str
    dag_id: str
    task_id: str
    metadata: Dict[str, Any]

    @property
    def dag_url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}"

    @cached_property
    def downstream_task_ids(self) -> List[str]:
        return check.is_list(self.metadata["downstream_task_ids"], str)


@whitelist_for_serdes
@record
class DagInfo:
    webserver_url: str
    dag_id: str
    metadata: Dict[str, Any]

    @property
    def url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}"

    @property
    def file_token(self) -> str:
        return self.metadata["file_token"]


@whitelist_for_serdes
class TaskHandle(NamedTuple):
    dag_id: str
    task_id: str


@whitelist_for_serdes
@record
class MappedAirflowTaskData:
    # remove if we keep it in SerializedDataData
    task_info: TaskInfo
    task_handle: TaskHandle
    proxied: Optional[bool]


@whitelist_for_serdes
@record
class MappedAirflowDagData:
    dag_info: DagInfo
    dag_id: str
    proxied: Optional[bool]


###################################################################################################
# Serialized data that scopes to airflow DAGs and tasks.
###################################################################################################
# History:
# - created
@whitelist_for_serdes
@record
class SerializedDagData:
    """A record containing pre-computed data about a given airflow dag."""

    dag_id: str
    task_handle_data: Mapping[str, "SerializedTaskHandleData"]
    dag_info: DagInfo
    source_code: str
    leaf_asset_keys: Set[AssetKey]
    task_infos: Mapping[str, TaskInfo]
    proxied: Optional[bool]


@whitelist_for_serdes
@record
class KeyScopedTaskItem:
    asset_key: AssetKey
    mapped_tasks: List[MappedAirflowTaskData]


@whitelist_for_serdes
@record
class KeyScopedDagItem:
    asset_key: AssetKey
    mapped_dags: List[MappedAirflowDagData]


###################################################################################################
# Serializable data that will be cached to avoid repeated calls to the Airflow API, and to avoid
# repeated scans of passed-in Definitions objects.
###################################################################################################
# History:
# - created
# - removed existing_asset_data
# - added key_scope_data_items
# - added instance_name
@whitelist_for_serdes
@record
class SerializedAirflowDefinitionsData:
    instance_name: str
    key_scoped_task_items: List[KeyScopedTaskItem]
    key_scoped_dag_items: List[KeyScopedDagItem]
    dag_datas: Mapping[str, SerializedDagData]

    @cached_property
    def all_mapped_tasks(self) -> Dict[AssetKey, List[MappedAirflowTaskData]]:
        return {item.asset_key: item.mapped_tasks for item in self.key_scoped_task_items}

    @cached_property
    def all_mapped_dags(self) -> Dict[AssetKey, List[MappedAirflowDagData]]:
        return {item.asset_key: item.mapped_dags for item in self.key_scoped_dag_items}

    @cached_property
    def mapped_dag_ids(self) -> AbstractSet[str]:
        res = set()
        for item in self.key_scoped_dag_items:
            for dag_data in item.mapped_dags:
                res.add(dag_data.dag_id)
        return res

    def task_ids_in_dag(self, dag_id: str) -> AbstractSet[str]:
        return set(self.dag_datas[dag_id].task_handle_data.keys())

    def proxied_state_for_task(self, dag_id: str, task_id: str) -> Optional[bool]:
        return self.dag_datas[dag_id].task_handle_data[task_id].proxied_state


# History:
# - created
@whitelist_for_serdes
@record
class SerializedTaskHandleData:
    """A record containing known data about a given airflow task handle."""

    proxied_state: Optional[bool]
    asset_keys_in_task: AbstractSet[AssetKey]
