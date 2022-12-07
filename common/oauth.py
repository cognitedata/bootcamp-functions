from __future__ import annotations

import os
from dataclasses import dataclass
from dataclasses import field

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials


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


def get_client() -> CogniteClient:
    """
    Method to get the CogniteClient. Meant to be used for running functions locally during development.
    Returns:
        CogniteClient
    """

    base_url = os.getenv("COGNITE_BASE_URL")
    scopes = [f"{base_url}/.default"]
    client_id = os.getenv("COGNITE_CLIENT_ID")
    client_secret = os.getenv("COGNITE_CLIENT_SECRET")
    token_url = os.getenv("COGNITE_TOKEN_URL")
    cognite_project = os.getenv("COGNITE_PROJECT")

    creds = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        scopes=scopes,
        client_secret=client_secret,
    )

    cnf = ClientConfig(client_name="local-run-function", project=cognite_project, credentials=creds, base_url=base_url)

    return CogniteClient(cnf)
