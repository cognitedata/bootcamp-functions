from typing import Any, Dict

import arrow

from calculations import (  # calculate_availability,; calculate_performance,; calculate_theoretical_runtime,
    calculate_count,
    calculate_off_spec,
    calculate_quality,
    get_timeseries_by_site_and_type,
    insert_datapoints,
)
from cognite.client import CogniteClient


def handle(client: CogniteClient, data: Dict[str, Any]) -> None:
    print(f"Input data = {data}")
    now = int(arrow.utcnow().timestamp() * 1000)
    print(f"Current time in ms = {now}")

    # Input data
    lookback_minutes = data["lookback_minutes"]  # Required
    sites = data.get("sites")
    latest_timestamp_ms = data.get("latest_timestamp_ms", now)

    counts = get_timeseries_by_site_and_type(client, "count", sites)
    goods = get_timeseries_by_site_and_type(client, "good", sites)
    status = get_timeseries_by_site_and_type(client, "status", sites)
    planned_status = get_timeseries_by_site_and_type(client, "planned_status", sites)

    # NOTE: It is almost not worth it to expose the intermediate calculations.
    # We should only expose OEE as all of these calculations are dependent on lookback_minutes

    # Calculate OFF_SPEC and QUALITY for all equipment
    total_counts = calculate_count(
        client, counts, latest_timestamp_ms, lookback_minutes
    )  # TODO: Change latest_timestamp_ms to now for live
    total_good = calculate_count(
        client, goods, latest_timestamp_ms, lookback_minutes
    )  # TODO: Change latest_timestamp_ms to now for live

    off_spec_dps = []
    quality_dps = []
    for idx, val in total_counts.items():
        prefix = idx.split(":")[0]
        good_count = total_good[f"{prefix}:good"]

        off_spec = calculate_off_spec(good_count, val)
        quality = calculate_quality(good_count, val)

        off_spec_dps.append({"externalId": f"{prefix}:off_spec", "datapoints": [(now, off_spec)]})
        quality_dps.append({"externalId": f"{prefix}:quality", "datapoints": [(now, quality)]})

    insert_datapoints(client, off_spec_dps, "off_spec")
    insert_datapoints(client, quality_dps, "quality")

    # Calculate the PERFORMANCE
    # calculate_performance(actual_runtime=, theoretical_runtime=)
    # TODO: PLACEHOLDER FOR ACTUAL CALCULATIONS
    performance_dps = [
        {"externalId": f"{ts.external_id.split(':')[0]}:performance", "datapoints": [(now, 1)]} for ts in status
    ]
    insert_datapoints(client, performance_dps, "performance")

    # Calculate the AVAILABILITY
    # calculate_availability(actual_runtime=, planned_runtime=)
    # TODO: PLACEHOLDER FOR ACTUAL CALCULATIONS
    availability_dps = [
        {"externalId": f"{ts.external_id.split(':')[0]}:performance", "datapoints": [(now, 1)]} for ts in planned_status
    ]
    insert_datapoints(client, availability_dps, "availability")

    print("Calculation complete.")
