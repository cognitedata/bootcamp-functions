import os
from ice_cream_factory_datapoints_extractor import extractor

def handle(secrets, data):
    print("running rest extractor")
    os.environ["COGNITE_CLIENT_ID"] = secrets.get("client-id")
    os.environ["COGNITE_CLIENT_SECRET"] = secrets.get("client-secret")
    extractor.main()
    print("running rest extractor done")