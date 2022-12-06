import re

from typing import List

import arrow
import pandas as pd

from cognite.client import CogniteClient
from cognite.client.data_classes import DatapointsList, DataSet, TimeSeries, TimeSeriesList
from pydantic import NonNegativeFloat, NonNegativeInt


def get_state(client: CogniteClient, db_name: str, table_name: str, tag: str = None):
    if tag:
        state = client.raw.rows.retrieve(db_name, table_name, tag)
        return (state.columns.get("low"), state.columns.get("high"))
    else:
        state = client.raw.rows.list(db_name, table_name, limit=None).to_pandas()
        return (min(state["low"]), max(state["high"]))


def split_and_join(text: str) -> str:
    output = re.split(":|_", text)  # split text at ":" or "_"
    output = [out.title() if i > 0 else out for i, out in enumerate(output)]
    return " ".join(output)


def get_timeseries_by_site_and_type(client: CogniteClient, typ: str, sites: List[str] = None) -> TimeSeriesList:
    if not sites:
        return client.time_series.list(metadata={"type": typ}, limit=None)
    ts = []
    for site in sites:
        ts.extend(client.time_series.list(metadata={"site": site, "type": typ}, limit=None))
    return TimeSeriesList(ts)


def insert_datapoints(client: CogniteClient, datapoints, typ: str, data_set: DataSet) -> None:
    """
    Takes a list datapoints and uploads data CDF
    """
    avail_ts = client.time_series.list(data_set_ids=[data_set.id], metadata={"type": typ}, limit=None)
    avail_ts_external_id = [ts.external_id for ts in avail_ts]
    missing_ts = [ts for ts in datapoints if ts["externalId"] not in avail_ts_external_id]
    timeseries_list = [
        TimeSeries(
            external_id=new_ts["externalId"],
            name=split_and_join(new_ts["externalId"]),
            metadata={"type": typ},
            data_set_id=data_set.id,
        )
        for new_ts in missing_ts
    ]
    if len(timeseries_list) > 0:
        # Create missing timeseries
        client.time_series.create(timeseries_list)
        print(f"Created missing {str(len(timeseries_list))} timeserie(s).")

    # Insert datapoints
    client.datapoints.insert_multiple(datapoints)
    print(f"Inserted datapoints for type {typ}. For {str(len(datapoints))} timeseries.")


def transform_and_sum_datapoints(dps: DatapointsList) -> pd.Series:
    """
    Takes a DatapointsList and rearranges it so that it can be summed over all retrieved timestamps and external ids
    """
    return dps.to_pandas().T.stack().groupby(level=[0]).sum()


def extract_uptime(dps: DatapointsList, period_start, period_end) -> pd.Series:
    """
    Takes a DatapointsList and calculate uptime

    """
    output = {}
    for ds in dps:
        period_start = arrow.get(period_start)
        period_end = arrow.get(period_end)
        series = ds.to_pandas().iloc[:, 0].sort_index(ascending=True)
        ts_period_start = pd.Timestamp(period_start.int_timestamp * 1000, unit="ms")
        ts_period_end = pd.Timestamp(period_end.int_timestamp * 1000, unit="ms")
        # Move outside datapoints to period start and end times
        if min(series.index) < ts_period_start:
            series.rename(index={min(series.index): ts_period_start}, inplace=True)
        if max(series.index) > ts_period_end:
            series.rename(index={max(series.index): ts_period_end}, inplace=True)
        # Forward fill values and resample to minute granularity
        series_ffill = series.resample("min").ffill()
        output[ds.external_id] = series_ffill.sum()
    return pd.Series(output)


def calculate_count(
    client: CogniteClient, timeseries: TimeSeriesList, end_time: NonNegativeInt, lookback_minutes: NonNegativeInt = 60
) -> pd.Series:
    """
    Sums the datapoints
    """
    end = arrow.get(end_time)
    start = end.shift(minutes=-lookback_minutes).timestamp() * 1000

    xids = [ts.external_id for ts in timeseries]

    dps = client.datapoints.retrieve(external_id=xids, start=start, end=end_time, aggregates=["sum"], granularity="1h")

    return transform_and_sum_datapoints(dps)


def calculate_uptime(
    client: CogniteClient, timeseries: TimeSeriesList, end_time: NonNegativeInt, lookback_minutes: NonNegativeInt = 60
) -> pd.Series:
    """
    Retrieving status from CDF
    """
    end = arrow.get(end_time)
    start = end.shift(minutes=-lookback_minutes).timestamp() * 1000

    xids = [ts.external_id for ts in timeseries]
    dps = client.datapoints.retrieve(external_id=xids, start=start, end=end_time, include_outside_points=True)

    return extract_uptime(dps, start, end_time)


def calculate_theoretical_runtime(cycle_time: NonNegativeFloat, total_count: NonNegativeInt) -> NonNegativeFloat:
    return cycle_time * total_count


def calculate_off_spec(good_count: NonNegativeInt, total_count: NonNegativeInt) -> NonNegativeInt:
    return total_count - good_count


def calculate_quality(good_count: NonNegativeInt, total_count: NonNegativeInt) -> NonNegativeFloat:
    if (good_count <= 0) or (total_count <= 0):
        return 0.0
    return good_count / total_count


def calculate_availability(actual_runtime: NonNegativeFloat, planned_runtime: NonNegativeFloat) -> NonNegativeFloat:
    if (actual_runtime <= 0) or (planned_runtime <= 0):
        return 0.0
    return actual_runtime / planned_runtime


def calculate_performance(actual_runtime: NonNegativeFloat, theoretical_runtime: NonNegativeFloat) -> NonNegativeFloat:
    if (actual_runtime <= 0) or (theoretical_runtime <= 0):
        return 0.0
    return actual_runtime / theoretical_runtime
