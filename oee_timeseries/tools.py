from __future__ import annotations

import re
from math import floor
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

from arrow import Arrow
from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes import TimeSeries


def translate_to_a_name(text: str) -> str:
    output = re.split(":|_", text)  # split text at ":" or "_"
    output = [out.title() if i > 0 else out for i, out in enumerate(output)]
    return " ".join(output)


def insert_datapoints(
    client: CogniteClient, datapoints: List[Dict[str, Union[str, int, list]]], typ: str, data_set: DataSet
) -> None:
    """
    Takes a list datapoints and uploads data CDF
    """
    avail_ts = client.time_series.list(data_set_ids=[data_set.id], metadata={"type": typ}, limit=None)
    known_external_ids = {ts.external_id for ts in avail_ts}
    missing_ext_ids = {
        record.get("externalId") for record in datapoints if record.get("externalId") not in known_external_ids
    }

    timeseries_list = [
        TimeSeries(
            external_id=new_ts,
            name=translate_to_a_name(new_ts),
            metadata={"type": typ},
            data_set_id=data_set.id,
        )
        for new_ts in missing_ext_ids
    ]
    if len(timeseries_list) > 0:
        # Create missing timeseries
        client.time_series.create(timeseries_list)
        print(f"Created missing {len(timeseries_list)} timeserie(s).")

    # Insert datapoints
    client.time_series.data.insert_multiple(datapoints)
    print(f"Inserted datapoints for type {typ}. For {str(len(datapoints))} timeseries.")


def get_timeseries_for_site(client: CogniteClient, site: str):
    known_types = {"count", "good", "status", "planned_status"}
    outcome = {}
    for typ in known_types:
        for ts in client.time_series.list(metadata={"site": site, "type": typ}, limit=None):
            outcome[ts.external_id] = ts
    return outcome


def discover_datapoints(client: CogniteClient, ts: Dict[str, TimeSeries], window: Tuple[Arrow, Arrow]):
    outcome = {}
    data = client.time_series.data.retrieve(
        external_id=list(ts.keys()),
        start=window[0].float_timestamp * 1000,
        end=window[1].float_timestamp * 1000,
        aggregates=["sum"],
        granularity="1m",
    )
    for _r in data:
        outcome[_r.external_id] = sorted(zip(_r.timestamp, _r.sum), key=lambda x: x[0])

    # fill the gaps

    for k, v in outcome.items():
        if k.endswith("status"):
            dp = client.time_series.data.retrieve_latest(external_id=k, before=window[0].float_timestamp * 1000 + 1)

            if len(dp.timestamp) == 0:
                values = [(window[0].float_timestamp * 1000, 0.0)]
            elif len(v) == 0:
                values = list(zip(dp.timestamp, dp.value))
            else:
                values = list(zip(dp.timestamp, dp.value)) + v

            outcome[k] = sorted(
                [
                    (
                        _timestamp,
                        next((values[i - 1][1] for i, v in enumerate(values) if v[0] > _timestamp), values[0][1]),
                    )
                    for _timestamp in range(
                        floor(window[0].floor("minutes").float_timestamp * 1000),
                        floor(window[1].floor("minutes").shift(minutes=1).float_timestamp * 1000),
                        60_000,
                    )
                ],
                key=lambda x: x[0],
            )

    return outcome
