import os

from dataclasses import dataclass, field

from cognite.client import ClientConfig, CogniteClient
from cognite.client.config import global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.configtools import load_yaml


global_config.disable_pypi_version_check = True


@dataclass
class FunctionConfig:
    """
    Common parameters to specify for Github workflows.
    Full list: https://github.com/cognitedata/function-action-oidc#function-metadata-in-github-workflow
    """

    deployment_client_id: str
    deployment_tenant_id: str
    cdf_project: str
    cdf_cluster: str
    common_folder: str = "common"
    function_deploy_timeout: int = 1500
    post_deploy_cleanup: bool = False
    schedules_client_id: str = ""
    schedules_tenant_id: str = ""
    description: str = ""
    owner: str = ""
    cpu: float = 0.1
    memory: float = 0.2
    env_vars: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)


def get_client(config_file_path: str = "function_config_test.yaml") -> CogniteClient:
    """
    Method to get the CogniteClient. Meant to be used for running functions locally during development.
    Args:
        config_file_path:
    Returns:
        CogniteClient
    """
    with open(config_file_path) as config_file:
        config = load_yaml(config_file, FunctionConfig)

    scopes = [f"https://{config.cdf_cluster}.cognitedata.com/.default"]
    client_secret = os.getenv("COGNITE_CLIENT_SECRET")  # store secret in env variable
    token_url = f"https://login.microsoftonline.com/{config.deployment_tenant_id}/oauth2/v2.0/token"
    base_url = f"https://{config.cdf_cluster}.cognitedata.com"

    creds = OAuthClientCredentials(
        token_url=token_url,
        client_id=config.deployment_client_id,
        scopes=scopes,
        client_secret=client_secret,
    )
    cnf = ClientConfig(
        client_name="my-special-client",
        project=config.cdf_project,
        credentials=creds,
        base_url=base_url,
    )
    return CogniteClient(cnf)
