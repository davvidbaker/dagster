from typing import NamedTuple, Optional

import pendulum

from dagster._core.reactive_scheduling.asset_graph_view import AssetSlice
from dagster._core.reactive_scheduling.scheduling_policy import (
    ScheduleLaunchResult,
    SchedulingExecutionContext,
    SchedulingPolicy,
)
from dagster._core.reactive_scheduling.scheduling_sensor import SensorSpec
from dagster._serdes.serdes import deserialize_value, serialize_value, whitelist_for_serdes


@whitelist_for_serdes
class CronCursor(NamedTuple):
    previous_launch_timestamp: Optional[float]

    def serialize(self) -> str:
        return serialize_value(self)

    @staticmethod
    def deserialize(cursor: Optional[str]) -> Optional["CronCursor"]:
        return deserialize_value(cursor, as_type=CronCursor) if cursor else None


class Cron(SchedulingPolicy):
    def __init__(self, cron_schedule: str, cron_timezone: str, sensor_spec: SensorSpec) -> None:
        super().__init__(sensor_spec=sensor_spec)
        self.cron_schedule = cron_schedule
        self.cron_timezone = cron_timezone

    def schedule_launch(
        self, context: SchedulingExecutionContext, asset_slice: AssetSlice
    ) -> ScheduleLaunchResult:
        cron_cursor = CronCursor.deserialize(context.previous_cursor)
        previous_launch_dt = (
            pendulum.from_timestamp(cron_cursor.previous_launch_timestamp)
            if cron_cursor and cron_cursor.previous_launch_timestamp
            else None
        )

        asset_slice_since_cron = asset_slice.compute_since_cron(
            cron_schedule=self.cron_schedule,
            cron_timezone=self.cron_timezone,
            previous_dt=previous_launch_dt,
        )

        if asset_slice_since_cron.is_empty:
            return ScheduleLaunchResult(
                launch=False, cursor=cron_cursor.serialize() if cron_cursor else None
            )

        return ScheduleLaunchResult(
            launch=True,
            explicit_launching_slice=asset_slice_since_cron,
            cursor=CronCursor(
                previous_launch_timestamp=context.effective_dt.timestamp()
            ).serialize(),
        )
