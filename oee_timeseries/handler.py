from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from math import floor
from typing import Any
from typing import Dict

import arrow
import numpy as np
from arrow import Arrow
from cognite.client import CogniteClient
from tools import discover_datapoints
from tools import get_timeseries_for_site
from tools import insert_datapoints

CYCLE_TIME = 3


def get_payload(collection: np.array, lookback_minutes: int, now: Arrow):
    return [
        (_timestamp, collection[i])
        for i, _timestamp in enumerate(
            range(
                floor(now.shift(minutes=-lookback_minutes + 1).floor("minutes").float_timestamp * 1000),
                floor(now.floor("minutes").shift(minutes=1).float_timestamp * 1000),
                60 * 1000,
            )
        )
    ]


def get_state(client, db_name, table_name):
    state = client.raw.rows.list(db_name, table_name, limit=None).to_pandas()
    return max(state["high"])


def handle(client: CogniteClient, data: Dict[str, Any]) -> None:
    print(f"Input data of function: {data}")

    # Input data
    lookback_minutes = data.get("lookback_minutes", 1440)
    window_size = data.get("window_size_minutes", 60)
    data_set_external_id = data.get("data_set_external_id", "uc:001:oee:ds")
    sites = data.get("sites")
    # now specifies the time upto which the OEE numbers will be calculated
    # We want to balance the data freshness here
    the_latest = get_state(client, db_name="src:002:opcua:db:state", table_name="timeseries_datapoints_states")
    now = arrow.get(the_latest).floor("minutes").shift(minutes=-10)  # -10 minutes as a safety margin
    data_set = client.data_sets.retrieve(external_id=data_set_external_id)
    futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for _range in Arrow.span_range("days", now.shift(minutes=-lookback_minutes), now, exact=True):
            for site in sites:
                process_range = (_range[0].shift(minutes=-window_size), _range[1])
                futures.append(
                    executor.submit(
                        process_site,
                        client,
                        data_set,
                        round((process_range[1] - process_range[0]).total_seconds() / 60) - window_size,
                        now,
                        site,
                        process_range,
                        window_size,
                    )
                )
    for future in futures:
        future.result()


def process_site(client, data_set, lookback_minutes, now, site, window, window_size):
    discovered_ts = get_timeseries_for_site(client, site)
    discovered_points = discover_datapoints(client, discovered_ts, window)
    equipment = {p.split(":")[0] for p in discovered_points.keys()}
    off_spec_dps = []
    performance_dps = []
    availability_dps = []
    quality_dps = []
    oee_dps = []
    for item in equipment:
        total_items = np.array(discovered_points.get(f"{item}:count"))[:, 1]
        good_items = np.array(discovered_points.get(f"{item}:good"))[:, 1]
        uptime_points = np.array(discovered_points.get(f"{item}:status"))[:, 1]
        planned_uptime_points = np.array(discovered_points.get(f"{item}:planned_status"))[:, 1]

        if (
            len(total_items) != len(good_items)
            or len(total_items) != len(uptime_points)
            or len(total_items) != len(planned_uptime_points)
        ):
            raise RuntimeError(f"CDF returned different amount of aggregations for {window}")

        if len(total_items) != lookback_minutes + window_size:
            raise RuntimeError(
                f"CDF returned inaccurate amount of aggregations for {window}. "
                f"Got {len(total_items)}, expected {lookback_minutes + window_size}"
            )

        bad_items = np.subtract(total_items, good_items)
        quality = np.divide(good_items, total_items, out=np.zeros_like(good_items), where=total_items != 0)
        uptime = np.convolve(uptime_points, np.ones(window_size, dtype=int), "valid")[1:]
        produced = np.convolve(total_items, np.ones(window_size, dtype=int), "valid")[1:]
        ideal_rate = np.full(lookback_minutes, 60.0 / 3.0)  # we know that ideal production should be 3 per sec
        planned_uptime = np.convolve(planned_uptime_points, np.ones(window_size, dtype=int), "valid")[1:]

        off_spec_dps.extend(
            [
                {
                    "externalId": f"{item}:off_spec",
                    "datapoints": get_payload(bad_items[window_size:], lookback_minutes, now),
                }
            ]
        )

        quality_dps.extend(
            [
                {
                    "externalId": f"{item}:quality",
                    "datapoints": get_payload(quality[window_size:], lookback_minutes, now),
                }
            ]
        )

        perf1 = np.divide(produced, uptime, out=np.zeros_like(produced), where=uptime != 0)
        performance = np.divide(perf1, ideal_rate, out=np.zeros_like(perf1), where=ideal_rate != 0)
        performance_dps.extend(
            [
                {
                    "externalId": f"{item}:performance",
                    "datapoints": get_payload(performance, lookback_minutes, now),
                }
            ]
        )

        availability = np.divide(uptime, planned_uptime, out=np.zeros_like(uptime), where=planned_uptime != 0)
        availability_dps.extend(
            [
                {
                    "externalId": f"{item}:availability",
                    "datapoints": get_payload(availability, lookback_minutes, now),
                }
            ]
        )

        oee = np.multiply(np.multiply(performance, availability), quality[window_size:])
        oee_dps.extend(
            [
                {
                    "externalId": f"{item}:oee",
                    "datapoints": get_payload(oee, lookback_minutes, now),
                }
            ]
        )
    insert_datapoints(client, performance_dps, "performance", data_set)
    insert_datapoints(client, quality_dps, "quality", data_set)
    insert_datapoints(client, availability_dps, "availability", data_set)
    insert_datapoints(client, off_spec_dps, "off_spec", data_set)
    insert_datapoints(client, oee_dps, "oee", data_set)
