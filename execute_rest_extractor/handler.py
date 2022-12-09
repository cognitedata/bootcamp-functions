from __future__ import annotations

import os

from ice_cream_factory_datapoints_extractor import extractor


def handle(secrets, data):
    print("running rest extractor.")
    if secrets:
        os.environ["COGNITE_CLIENT_ID"] = secrets.get("client-id")
        os.environ["COGNITE_CLIENT_SECRET"] = secrets.get("client-secret")
    if data:
        if data.get("frontfill_enabled"):
            os.environ["FRONTFILL_ENABLED"] = data.get("frontfill_enabled")
        if data.get("frontfill_lookback_min"):
            os.environ["FRONTFILL_LOOKBACK_MIN"] = data.get("frontfill_lookback_min")
        if data.get("backfill_enabled"):
            os.environ["BACKFILL_ENABLED"] = data.get("backfill_enabled")
        if data.get("backfill_history_days"):
            os.environ["BACKFILL_HISTORY_DAYS"] = data.get("backfill_history_days")
        if data.get("sites"):
            os.environ["SITES"] = data.get("sites")
        if data.get("backfill_shift_now_ts_backwards_days"):
            os.environ["BACKFILL_SHIFT_NOW_TS_BACKWARDS_DAYS"] = data.get("backfill_shift_now_ts_backwards_days")
    extractor.main()
    print("running rest extractor done.")
