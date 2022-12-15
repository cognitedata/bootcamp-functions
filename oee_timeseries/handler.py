from __future__ import annotations

import random
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from math import floor
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import arrow
import numpy as np
from arrow import Arrow
from cognite.client import CogniteClient
from retry import retry
from tools import discover_datapoints
from tools import get_timeseries_for_site
from tools import insert_datapoints

CYCLE_TIME = 3


def get_payload(collection: np.array, window: Tuple[Arrow, Arrow]):
    return [
        (_timestamp, collection[i])
        for i, _timestamp in enumerate(
            range(floor(window[0].float_timestamp * 1000), floor(window[1].float_timestamp * 1000), 60_000)
        )
    ]


def get_state(client, db_name, table_name):
    state = client.raw.rows.list(db_name, table_name, limit=None).to_pandas().dropna()
    return max(state["high"])


def handle(client: CogniteClient, data: Dict[str, Any]) -> None:
    print(f"Input data of function: {data}")

    # Input data
    lookback_minutes = data.get("lookback_minutes", 1440)
    data_set_external_id = data.get("data_set_external_id", "uc:001:oee:ds")
    sites = data.get("sites")
    # "now" variable specifies the time upto which the OEE numbers will be calculated
    # We want to balance the data freshness here
    the_latest = get_state(client, db_name="src:002:opcua:db:state", table_name="timeseries_datapoints_states")
    now = arrow.get(the_latest, tzinfo="UTC").floor("minutes").shift(minutes=-10)  # -10 minutes as a safety margin
    data_set = client.data_sets.retrieve(external_id=data_set_external_id)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures: List[Future] = []
        for _range in Arrow.span_range("day", now.shift(minutes=-lookback_minutes), now, exact=True):
            for site in sites:
                futures.append(
                    executor.submit(
                        process_site,
                        client,
                        data_set,
                        round((_range[1] - _range[0]).total_seconds() / 60),
                        site,
                        _range,
                    )
                )

        for f in futures:
            f.result()


@retry(tries=5, jitter=random.randint(5, 10), delay=random.randint(5, 15))
def process_site(client, data_set, lookback_minutes, site, window):
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
        uptime = np.array(discovered_points.get(f"{item}:status"))[:, 1]
        planned_uptime = np.array(discovered_points.get(f"{item}:planned_status"))[:, 1]

        if (
            len(total_items) != len(good_items)
            or len(total_items) != len(uptime)
            or len(total_items) != len(planned_uptime)
        ):
            raise RuntimeError(f"CDF returned different amount of aggregations for {window}")

        bad_items = np.subtract(total_items, good_items)
        quality = np.divide(good_items, total_items, out=np.zeros_like(good_items), where=total_items != 0)
        ideal_rate = np.full(lookback_minutes, 60.0 / 3.0)  # we know that ideal production should be 3 per sec.

        off_spec_dps.extend(
            [
                {
                    "externalId": f"{item}:off_spec",
                    "datapoints": get_payload(bad_items, window),
                }
            ]
        )

        quality_dps.extend(
            [
                {
                    "externalId": f"{item}:quality",
                    "datapoints": get_payload(quality, window),
                }
            ]
        )

        perf1 = np.divide(total_items, uptime, out=np.zeros_like(total_items), where=uptime != 0)
        performance = np.divide(perf1, ideal_rate, out=np.zeros_like(perf1), where=ideal_rate != 0)
        performance_dps.extend(
            [
                {
                    "externalId": f"{item}:performance",
                    "datapoints": get_payload(performance, window),
                }
            ]
        )

        availability = np.divide(uptime, planned_uptime, out=np.zeros_like(uptime), where=planned_uptime != 0)
        availability_dps.extend(
            [
                {
                    "externalId": f"{item}:availability",
                    "datapoints": get_payload(availability, window),
                }
            ]
        )

        oee = np.multiply(np.multiply(performance, availability), quality)
        oee_dps.extend(
            [
                {
                    "externalId": f"{item}:oee",
                    "datapoints": get_payload(oee, window),
                }
            ]
        )
    insert_datapoints(client, performance_dps, "performance", data_set)
    insert_datapoints(client, quality_dps, "quality", data_set)
    insert_datapoints(client, availability_dps, "availability", data_set)
    insert_datapoints(client, off_spec_dps, "off_spec", data_set)
    insert_datapoints(client, oee_dps, "oee", data_set)
