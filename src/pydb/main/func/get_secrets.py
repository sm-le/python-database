# Get secret from key vault
# contributer = smlee

# History
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

# Main
@log(set_logger=logger)
def get_secret(secret_name:str,
               *,
               override:bool=False,
               path:bool=False) -> str:
    """Get secret from key vault

    Args:
        secret_name: secret name
        override: if True, use imported secret path 

    Returns:
        str(secret value)
    """
    if override:
        path = os.environ.get(f"database_{secret_name}")
        if path:
            with open(path, "r") as f:
                secret = json.load(f)

                return secret
        else:
            raise FileNotFoundError(f"Secret path not found: {secret_name}")
    elif path:
        return secret_name
    else:
        key_vault_name = os.environ.get("database_vault_name")
        if key_vault_name:
            key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
            credential = DefaultAzureCredential()

            client = SecretClient(vault_url=key_vault_uri, credential=credential)

            secret = client.get_secret(secret_name)
            return json.loads(secret.value)
        else:
            raise KeyError("Key vault name is not set in environment variable. Please set it with 'database_vault_name'")