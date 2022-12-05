from typing import Any, Dict

import arrow

from calculations import (
    calculate_availability,
    calculate_count,
    calculate_off_spec,
    calculate_performance,
    calculate_quality,
    calculate_theoretical_runtime,
    get_timeseries_by_site_and_type,
    get_uptime,
    insert_datapoints,
)
from cognite.client import CogniteClient


CYCLE_TIME = 3


def handle(client: CogniteClient, data: Dict[str, Any]) -> None:
    print(f"Input data = {data}")
    latest_ts = int(arrow.utcnow().shift(hours=-1).timestamp() * 1000)
    print(f"Current time in ms = {latest_ts}")

    # Input data
    lookback_minutes = data.get("lookback_minutes", 60)
    data_set_external_id = data.get("data_set_external_id", "uc:001:oee:ds")
    sites = data.get("sites")
    latest_timestamp_ms = data.get("latest_timestamp_ms", latest_ts)

    data_set = client.data_sets.retrieve(external_id=data_set_external_id)

    counts = get_timeseries_by_site_and_type(client, "count", sites)
    goods = get_timeseries_by_site_and_type(client, "good", sites)
    status = get_timeseries_by_site_and_type(client, "status", sites)
    planned_status = get_timeseries_by_site_and_type(client, "planned_status", sites)

    total_counts = calculate_count(client, counts, latest_timestamp_ms, lookback_minutes)
    total_good = calculate_count(client, goods, latest_timestamp_ms, lookback_minutes)

    actual_run_time = get_uptime(client, status, latest_timestamp_ms, lookback_minutes)
    planned_run_time = get_uptime(client, planned_status, latest_timestamp_ms, lookback_minutes)

    off_spec_dps = []
    quality_dps = []
    performance_dps = []
    availability_dps = []

    for idx, val in total_counts.items():
        prefix = idx.split(":")[0]
        good_count = total_good[f"{prefix}:good"]

        # Calculate OFF_SPEC and QUALITY for all equipment
        off_spec = calculate_off_spec(good_count, val)
        quality = calculate_quality(good_count, val)

        off_spec_dps.append({"externalId": f"{prefix}:off_spec", "datapoints": [(latest_timestamp_ms, off_spec)]})
        quality_dps.append({"externalId": f"{prefix}:quality", "datapoints": [(latest_timestamp_ms, quality)]})

        # Calculate the PERFORMANCE
        theory_cycle_time = calculate_theoretical_runtime(CYCLE_TIME, val)
        performance = calculate_performance(
            actual_runtime=actual_run_time[f"{prefix}:status"], theoretical_runtime=theory_cycle_time
        )
        performance_dps.append(
            {"externalId": f"{prefix}:performance", "datapoints": [(latest_timestamp_ms, performance)]}
        )

        # Calculate the AVAILABILITY
        availability = calculate_availability(
            actual_runtime=actual_run_time[f"{prefix}:status"],
            planned_runtime=planned_run_time[f"{prefix}:planned_status"],
        )
        availability_dps.append(
            {"externalId": f"{prefix}:availability", "datapoints": [(latest_timestamp_ms, availability)]}
        )

    insert_datapoints(client, off_spec_dps, "off_spec", data_set)
    insert_datapoints(client, quality_dps, "quality", data_set)
    insert_datapoints(client, performance_dps, "performance", data_set)
    insert_datapoints(client, availability_dps, "availability", data_set)

    print("Calculation complete.")
