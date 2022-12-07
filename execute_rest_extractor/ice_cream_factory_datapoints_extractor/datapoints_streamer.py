from __future__ import annotations

import logging
from threading import Event
from typing import List
from typing import Set

import arrow
from cognite.client.data_classes import TimeSeries
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from retry import retry

from .config import IceCreamFactoryConfig
from .ice_cream_factory_api import IceCreamFactoryAPI


class Streamer:
    """
    Periodically query the Ice Cream Factory API for datapoints.

    Args:
        upload_queue: Where to put data points
        stop: Stopping event
        api: API to query
        timeseries_list: List of timeseries to query datapoints for
        config: Set of configuration parameters
    """

    def __init__(
        self,
        upload_queue: TimeSeriesUploadQueue,
        stop: Event,
        api: IceCreamFactoryAPI,
        timeseries_list: List[TimeSeries],
        config: IceCreamFactoryConfig,
        states: AbstractStateStore,
    ):
        # Target iteration time to allow some throttling between iterations
        self.target_iteration_time = int(1.5 * len(timeseries_list))
        self.upload_queue = upload_queue
        self.stop = stop
        self.api = api
        self.config = config
        self.states = states

        self.timeseries_list = timeseries_list
        self.timeseries_seen_set: Set[str] = set()

    @retry(tries=10)
    def _extract_timeseries(self, timeseries: TimeSeries) -> None:
        """
        Perform a query for a given time series. Function to send to thread pool in run().

        Args:
            timeseries: timeseries to get datapoints for
        """
        logging.info(f"Getting live data for {timeseries.external_id}")
        to_time = arrow.utcnow()
        # lookup back for X minutes. Allows late data.
        from_time = to_time.shift(minutes=-self.config.frontfill.lookback_min)
        single_query_lookback = min(3600, self.config.frontfill.lookback_min)

        while from_time < to_time:
            req_time = min(to_time, from_time.shift(minutes=single_query_lookback))
            datapoints_dict = self.api.get_oee_timeseries_datapoints(
                timeseries_ext_id=timeseries.external_id, start=from_time.float_timestamp, end=req_time.float_timestamp
            )

            for timeseries_ext_id in datapoints_dict:
                # API returns 2 associated timeseries.
                self.upload_queue.add_to_upload_queue(
                    external_id=timeseries_ext_id, datapoints=datapoints_dict[timeseries_ext_id]
                )

            from_time = req_time

    def run(self) -> None:
        """
        Run streamer until the stop event is set.
        """
        while True:
            for ts in self.timeseries_list:
                self._extract_timeseries(ts)
            if not (
                self.config.frontfill.continuous and self.stop.wait(60.0 * self.config.frontfill.lookback_min / 6.0)
            ):
                break
