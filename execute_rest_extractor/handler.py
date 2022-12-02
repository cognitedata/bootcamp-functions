import os

from ice_cream_factory_datapoints_extractor import extractor


def handle(secrets, data):
    print("running rest extractor")
    os.environ["COGNITE_CLIENT_ID"] = secrets.get("client-id")
    os.environ["COGNITE_CLIENT_SECRET"] = secrets.get("client-secret")
    if data:
        if data.get("frontfill_lookback_min"):
            os.environ["FRONTFILL_LOOKBACK_MIN"] = data.get("frontfill_lookback_min")
        else:
            print("please supply `frontfill_lookback_min` variable in a valid JSON")
        print("overwrite default extractor config with the one supplied from JSON")
    extractor.main()
    print("running rest extractor done")
