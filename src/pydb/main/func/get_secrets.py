# Get secret from key vault
# contributer = smlee

# History
# 2024-12-22 | v2.0 - vault became no priority, making path a priority
# 2024-03-27 | v1.0 - first commit

# Module import
import os
from dotenv import load_dotenv
load_dotenv()
import json
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import logging
logger = logging.getLogger('pydb')
from pydb.conf import log
import yaml

# Main
@log(set_logger=logger)
def get_secret(secret_name:str,
               *,
               path:str=str(),
               vault:bool=False) -> str:
    """Get secret from key vault

    Args:
        path: credential file path, if it is not provided. It will search environment variable
        secret_name: secret name
        vault: if True, use imported secret path 

    Returns:
        str(secret value)
    """
    if path:
        with open(path) as f:
            secret = yaml.safe_load(f).get(secret_name)
        if secret:
            return secret
        else:
            raise ValueError(f"Failed to get secret on {secret_name}")
        
    elif vault:
        key_vault_name = os.environ.get("database_vault_name")
        if key_vault_name:
            key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
            credential = DefaultAzureCredential()

            client = SecretClient(vault_url=key_vault_uri, credential=credential)

            secret = client.get_secret(secret_name)
            return json.loads(secret.value)
        else:
            raise KeyError("Key vault name is not set in environment variable. Please set it with 'database_vault_name'")
    else:
        c__path = os.environ.get(f"database_{secret_name}")
        if c__path:
            with open(c__path, "r") as f:
                secret = json.load(f)

                return secret
        else:
            raise FileNotFoundError(f"Secret path not found: {secret_name}. Please setup your .env file.")