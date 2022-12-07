import os

from dotenv import load_dotenv

from execute_rest_extractor.handler import handle


# Import Environment Variables
load_dotenv()

secrets = {"client-id": os.getenv("COGNITE_CLIENT_ID"), "client-secret": os.getenv("COGNITE_CLIENT_SECRET")}

if __name__ == "__main__":
    data = {"frontfill_enabled": "True", "frontfill_lookback_min": "60", "backfill_enabled": "False"}
    handle(secrets, data)
