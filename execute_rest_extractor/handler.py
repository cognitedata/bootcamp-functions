import os

import yaml

from ice_cream_factory_datapoints_extractor import extractor


def handle(secrets, data):
    print("running rest extractor")
    os.environ["COGNITE_CLIENT_ID"] = secrets.get("client-id")
    os.environ["COGNITE_CLIENT_SECRET"] = secrets.get("client-secret")
    if data:
        print("overwrite default extractor config with the one supplied from JSON")
        _file_name = "extractor_config_schedule.yaml"
        with open(_file_name, "w") as output_config_file:
            yaml.dump(data, output_config_file, default_flow_style=False)
        extractor.main(config_file_path=_file_name)
    else:
        extractor.main()
    print("running rest extractor done")
