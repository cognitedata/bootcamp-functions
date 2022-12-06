from typing import Any, Dict

import arrow

from calculations import (
    calculate_availability,
    calculate_count,
    calculate_off_spec,
    calculate_performance,
    calculate_quality,
    calculate_theoretical_runtime,
    calculate_uptime,
    get_state,
    get_timeseries_by_site_and_type,
    insert_datapoints,
)
from cognite.client import CogniteClient


CYCLE_TIME = 3


def handle(client: CogniteClient, data: Dict[str, Any]) -> None:
    print(f"Input data of function: {data}")
    low, high = get_state(client, db_name="src:002:opcua:db:state", table_name="timeseries_datapoints_states")
    latest_ts = high
    print(f"Latest available datapoint from {arrow.get(latest_ts).humanize()}. {arrow.get(latest_ts).format()}")

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

    total_actual_run_time = calculate_uptime(client, status, latest_timestamp_ms, lookback_minutes)
    total_planned_run_time = calculate_uptime(client, planned_status, latest_timestamp_ms, lookback_minutes)

    off_spec_dps = []
    quality_dps = []
    performance_dps = []
    availability_dps = []

    all_prefix = []
    all_prefix += [tag.split(":")[0] for tag in total_counts.index]
    all_prefix += [tag.split(":")[0] for tag in total_good.index]
    all_prefix += [tag.split(":")[0] for tag in total_actual_run_time.index]
    all_prefix += [tag.split(":")[0] for tag in total_planned_run_time.index]
    all_prefix = set(all_prefix)

    for prefix in all_prefix:

        try:
            count = total_counts[f"{prefix}:count|sum"]
            good_count = total_good[f"{prefix}:good|sum"]

            # Calculate OFF_SPEC and QUALITY for all equipment
            off_spec = calculate_off_spec(good_count, count)
            quality = calculate_quality(good_count, count)

            if off_spec:
                off_spec_dps.append(
                    {"externalId": f"{prefix}:off_spec", "datapoints": [(latest_timestamp_ms, off_spec)]}
                )

            if quality:
                quality_dps.append({"externalId": f"{prefix}:quality", "datapoints": [(latest_timestamp_ms, quality)]})

            # Calculate the PERFORMANCE
            theory_cycle_time = calculate_theoretical_runtime(CYCLE_TIME, count)
            performance = calculate_performance(
                actual_runtime=total_actual_run_time[f"{prefix}:status"], theoretical_runtime=theory_cycle_time
            )

            if performance:
                performance_dps.append(
                    {"externalId": f"{prefix}:performance", "datapoints": [(latest_timestamp_ms, performance)]}
                )

            # Calculate the AVAILABILITY
            availability = calculate_availability(
                actual_runtime=total_actual_run_time[f"{prefix}:status"],
                planned_runtime=total_planned_run_time[f"{prefix}:planned_status"],
            )
            if availability:
                availability_dps.append(
                    {"externalId": f"{prefix}:availability", "datapoints": [(latest_timestamp_ms, availability)]}
                )

        except KeyError:
            print(f"Failed to calculated values for {prefix}.")

    print("Uploading datapoints to CDF.")

    insert_datapoints(client, off_spec_dps, "off_spec", data_set)
    insert_datapoints(client, quality_dps, "quality", data_set)
    insert_datapoints(client, performance_dps, "performance", data_set)
    insert_datapoints(client, availability_dps, "availability", data_set)

    print("Calculation complete.")
