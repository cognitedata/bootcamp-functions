from __future__ import annotations

from math import floor
from typing import Any
from typing import Callable
from typing import Dict

import arrow
import numpy as np
from arrow import Arrow
from cognite.client import CogniteClient

from oee_timeseries.tools import discover_datapoints
from oee_timeseries.tools import get_timeseries_for_site
from tools import insert_datapoints

CYCLE_TIME = 3


def get_payload(func: Callable[[int], float], lookback_minutes: int, now: Arrow):
    return [
        (_timestamp, func(i))
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
    lookback_minutes = data.get("lookback_minutes", 60)
    data_set_external_id = data.get("data_set_external_id", "uc:001:oee:ds")
    sites = data.get("sites")
    # now specifies the time upto which the OEE numbers will be calculated
    # We want to balance the data freshness here
    the_latest = get_state(client, db_name="src:002:opcua:db:state", table_name="timeseries_datapoints_states")
    now = arrow.get(the_latest).shift(minutes=-10)

    window = (now.shift(minutes=-2 * lookback_minutes), now)

    data_set = client.data_sets.retrieve(external_id=data_set_external_id)
    for site in sites:
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

            if len(total_items) != 2 * lookback_minutes:
                raise RuntimeError(
                    f"CDF returned inaccurate amount of aggregations for {window}. "
                    f"Got {len(total_items)}, expected {2 * lookback_minutes}"
                )

            bad_items = np.subtract(total_items, good_items)
            quality = np.divide(good_items, total_items, out=np.zeros_like(good_items), where=total_items != 0)
            uptime = np.convolve(uptime_points, np.ones(lookback_minutes, dtype=int), "valid")[1:]
            produced = np.convolve(total_items, np.ones(lookback_minutes, dtype=int), "valid")[1:]
            ideal_rate = np.full(lookback_minutes, 60.0 / 3.0)  # we know that ideal production should be 3 per sec
            planned_uptime = np.convolve(planned_uptime_points, np.ones(lookback_minutes, dtype=int), "valid")[1:]

            off_spec_dps.extend(
                [
                    {
                        "externalId": f"{item}:off_spec",
                        "datapoints": get_payload(lambda x, off=bad_items: off[x], lookback_minutes, now),
                    }
                ]
            )

            quality_dps.extend(
                [
                    {
                        "externalId": f"{item}:quality",
                        "datapoints": get_payload(lambda x, q=quality: q[x], lookback_minutes, now),
                    }
                ]
            )

            performance_dps.extend(
                [
                    {
                        "externalId": f"{item}:performance",
                        "datapoints": get_payload(
                            lambda x, p=produced, up=uptime, r=ideal_rate: p[x] / up[x] / r[x]
                            if r[x] != 0 and up[x] != 0
                            else 0.0,
                            lookback_minutes,
                            now,
                        ),
                    }
                ]
            )

            availability_dps.extend(
                [
                    {
                        "externalId": f"{item}:availability",
                        "datapoints": get_payload(
                            lambda x, up=uptime, pl=planned_uptime: up[x] / pl[x] if pl[x] != 0 else 0.0,
                            lookback_minutes,
                            now,
                        ),
                    }
                ]
            )

            oee_dps.extend(
                [
                    {
                        "externalId": f"{item}:oee",
                        "datapoints": get_payload(
                            lambda x, q=quality, p=produced, r=ideal_rate, pl=planned_uptime: (q[x] * p[x] * r[x])
                                                                                              / pl[x]
                            if pl[x] != 0
                            else 0.0,
                            lookback_minutes,
                            now,
                        ),
                    }
                ]
            )

        insert_datapoints(client, performance_dps, "performance", data_set)
        insert_datapoints(client, quality_dps, "quality", data_set)
        insert_datapoints(client, availability_dps, "availability", data_set)
        insert_datapoints(client, off_spec_dps, "off_spec", data_set)
        insert_datapoints(client, oee_dps, "oee", data_set)
