from dotenv import load_dotenv

from common.oauth import get_client
from oee_timeseries.handler import handle


# Import Environment Variables
load_dotenv()
# print(os.getenv("COGNITE_CLIENT_SECRET"))

client = get_client()

data = {"sites": ["Oslo", "Hannover"], "lookback_minutes": 60, "data_set_external_id": "uc:001:oee:ds"}

handle(client, data)
