from dotenv import load_dotenv

from common.oauth import get_client
from oee_timeseries.handler import handle


# Import Environment Variables
load_dotenv(".env")
# print(os.getenv("COGNITE_CLIENT_SECRET"))

client = get_client("common/function_config_test.yaml")
# print(client.iam.token.inspect())

data = {"sites": ["Oslo", "Hannover"], "lookback_minutes": 60}

handle(client, data)
