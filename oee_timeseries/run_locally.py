from __future__ import annotations

from dotenv import load_dotenv

from common.oauth import get_client
from oee_timeseries.handler import handle

if __name__ == "__main__":
    # Import Environment Variables
    load_dotenv()
    client = get_client()
    data = {"sites": ["Oslo", "Hannover", "Nuremberg"], "lookback_minutes": 60, "data_set_external_id": "uc:001:oee:ds"}

    # Delete Timeseries
    handle(client, data)
