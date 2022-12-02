import os

from dotenv import load_dotenv
from ice_cream_factory_datapoints_extractor import extractor


# Import Environment Variables
load_dotenv(".env")


def handle(data):
    print("running rest extractor locally")
    os.environ["FRONTFILL_LOOKBACK_MIN"] = data.get("frontfill_lookback_min")
    extractor.main(config_file_path="execute_rest_extractor/extractor_config.yaml")
    print("running rest extractor locally is done")


if __name__ == "__main__":
    data = {"frontfill_lookback_min": "120"}
    handle(data)
