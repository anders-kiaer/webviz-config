import sys
import time
import secrets
import asyncio
import pathlib
import datetime
import warnings
import webbrowser
import subprocess
from typing import List, Dict, Optional, Tuple

import requests

try:
    from azure.identity import (
        AzureCliCredential,
        # TokenCachePersistenceOptions,
        # InteractiveBrowserCredential,
    )
    from azure.mgmt.resource.subscriptions import SubscriptionClient
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.storage import StorageManagementClient
    from azure.storage.blob import BlobServiceClient

    AZURE_CLI_INSTALLED = True
except ModuleNotFoundError:
    AZURE_CLI_INSTALLED = False

GRAPH_BASE_URL = "https://graph.microsoft.com"
PIMCOMMON_URL = (
    "https://portal.azure.com/#blade/Microsoft_Azure_PIMCommon/ActivationMenuBlade"
)


def logged_in() -> bool:
    """Returns true if user is logged into Azure,
    otherwise returns False.
    """
    # No easy way currently checking if logged in.
    # https://github.com/Azure/azure-cli/issues/6802

    if not AZURE_CLI_INSTALLED:
        raise RuntimeError(
            "In order to use webviz deploy features, you need to first install "
            "the optional deploy dependencies. You can do this by e.g. running "
            "'pip install webviz-config[deployment]'"
        )

    try:
        _azure_cli(["account", "list-locations"], devnull_stderr=False)
        return True
    except azure.cli.core.CLIError:
        return False


def log_in() -> None:
    _azure_cli(["login", "--use-device-code"])


def _credential():
    return AzureCliCredential()


def _subscription_id(subscription_name: str = None) -> str:
    subscription_list = SubscriptionClient(_credential()).subscriptions.list()

    if subscription_name is None:
        return next(subscription_list).subscription_id

    for sub in subscription_list:
        if sub.display_name == subscription_name:
            return sub.subscription_id

    raise ValueError(f"Could not find a subscription with name {subscription_name}")


def _connection_string(subscription, resource_group, storage_account) -> str:
    key = get_storage_account_access_key(subscription, resource_group, storage_account)
    return (
        f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={key}"
    )


def subscriptions() -> List[str]:
    """Returns list of all Azure subscriptions logged in user has read access to."""
    return [
        sub.display_name
        for sub in SubscriptionClient(_credential()).subscriptions.list()
    ]


def resource_groups(subscription: str) -> List[str]:
    """Returns list of all Azure resource group names logged in user has read access to
    within given subscription."""

    rmc = ResourceManagementClient(_credential(), _subscription_id(subscription))
    return [rg.name for rg in rmc.resource_groups.list()]


def storage_account_name_available(name: str) -> Tuple[bool, str]:
    sc = StorageManagementClient(_credential(), _subscription_id())
    result = sc.storage_accounts.check_name_availability({"name": name})
    return (result.name_available, result.message)


def storage_account_exists(name: str, subscription: str, resource_group: str) -> bool:
    sc = StorageManagementClient(_credential(), _subscription_id(subscription))

    if any(
        account.name == name
        for account in sc.storage_accounts.list_by_resource_group(resource_group)
    ):
        return True

    if any(account.name == name for account in sc.storage_accounts.list()):
        warnings.warn(
            f"Storage account with name {name} found, but it belongs "
            f"to another resource group ({account['resourceGroup']}."
        )
        return True

    return False


def storage_container_exists(
    container_name: str, account_name: str, subscription: str, resource_group: str
) -> bool:
    sc = StorageManagementClient(_credential(), _subscription_id(subscription))
    containers = sc.blob_containers.list(resource_group, account_name)
    return any(container.name == container_name for container in containers)


def create_storage_account(subscription: str, resource_group: str, name: str) -> None:
    """Creates an Azure storage account. Also adds upload access, as well
    as possibility to list/generate access keys, to the user creating it
    (i.e. the currently logged in user).

    Note that Azure documentation states that it can take up to five minutes
    after the command has finished until the added access is enabled in practice.
    """

    azure_pim_already_open = False

    while True:
        try:
            _azure_cli(
                [
                    "storage",
                    "account",
                    "create",
                    "--subscription",
                    subscription,
                    "--resource-group",
                    resource_group,
                    "--name",
                    name,
                    "--location",
                    "northeurope",
                    "--sku",
                    "Standard_ZRS",
                    "--encryption-services",
                    "blob",
                ]
            )
            break
        except (HttpResponseError, CloudError) as exc:
            if "AuthorizationFailed" in str(exc):
                if not azure_pim_already_open:
                    webbrowser.open(f"{PIMCOMMON_URL}/azurerbac")
                    print(
                        "Not able to create new storage account. Do you have "
                        "enough priviliges to do it? We automatically opened the URL "
                        "to where you activate Azure PIM. Please activate necessary "
                        "priviliges. You need to be 'Owner' of the subscription in "
                        "order to both create the new account and assign user "
                        "roles to it afterwards."
                    )
                    azure_pim_already_open = True
                print("New attempt of app registration in 1 minute.")
                time.sleep(60)
            else:
                raise RuntimeError("Not able to create new storage account.") from exc

    user_id: str = _azure_cli(
        ["ad", "signed-in-user", "show", "--query", "objectId", "-o", "tsv"]
    )
    resource_group_id: str = _azure_cli(
        ["group", "show", "--subscription", subscription, "--name", resource_group]
    )["id"]

    for role in [
        "Storage Blob Data Contributor",
        "Storage Account Key Operator Service Role",
    ]:
        _azure_cli(
            [
                "role",
                "assignment",
                "create",
                "--role",
                role,
                "--assignee",
                user_id,
                "--scope",
                f"{resource_group_id}/providers/Microsoft.Storage/storageAccounts/{name}",
            ]
        )


def get_storage_account_access_key(
    subscription: str,
    resource_group: str,
    account_name: str,
) -> str:
    sc = StorageManagementClient(_credential(), _subscription_id(subscription))
    return sc.storage_accounts.list_keys(resource_group, account_name).keys[0].value


def create_storage_container(
    subscription: str,
    resource_group: str,
    storage_account: str,
    container: str,
) -> None:
    BlobServiceClient.from_connection_string(
        _connection_string(subscription, resource_group, storage_account)
    ).get_container_client(container).create_container()


def _upload_batch(
    subscription,
    resource_group,
    storage_name: str,
    container_name: str,
    source_folder: pathlib.Path,
):
    paths_to_upload = [path for path in source_folder.rglob("*") if path.is_file()]

    if sys.version_info >= (3, 7):
        from tqdm.asyncio import tqdm
        from azure.storage.blob.aio import ContainerClient

        async def _upload_file(container_client, path, source_folder):
            with open(path, "rb") as fh:
                await container_client.upload_blob(
                    name=path.relative_to(source_folder).as_posix(),
                    data=fh,
                    overwrite=True,
                )

        async def _upload_blob():
            container_client = ContainerClient.from_connection_string(
                _connection_string(subscription, resource_group, storage_name),
                container_name,
            )

            async with container_client:
                tasks = [
                    asyncio.create_task(
                        _upload_file(container_client, path, source_folder)
                    )
                    for path in paths_to_upload
                ]

                for task in tqdm.as_completed(
                    tasks, bar_format="{l_bar} {bar} | Uploaded {n_fmt}/{total_fmt}"
                ):
                    await task

        asyncio.run(_upload_blob())

    else:  # Python 3.6 don't have the same rich set of features in asyncio.
        from tqdm import tqdm
        from azure.storage.blob import ContainerClient

        container_client = ContainerClient.from_connection_string(
            _connection_string(subscription, resource_group, storage_name),
            container_name,
        )

        for path in tqdm(
            paths_to_upload, bar_format="{l_bar} {bar} | Uploaded {n_fmt}/{total_fmt}"
        ):
            with open(path, "rb") as fh:
                container_client.upload_blob(
                    name=path.relative_to(source_folder).as_posix(),
                    data=fh,
                    overwrite=True,
                )


def storage_container_upload_folder(
    storage_name: str,
    container_name: str,
    source_folder: pathlib.Path,
) -> None:
    # If the upload access was recently added, Azure documentation
    # says it can take up until five minutes before the access is
    # enabled in practice.

    for _ in range(5):
        try:
            _upload_batch(
                subscription,
                resource_group,
                storage_name,
                container_name,
                source_folder,
            )
            return
        except AzureHttpError:
            pass
        finally:
            print("Waiting on Azure access activation... please wait.")
            time.sleep(60)

    raise RuntimeError("Not able to upload folder to blob storage container.")


def _graph_headers() -> Dict[str, str]:
    token = _credential().get_token(GRAPH_BASE_URL).token
    return {"Authorization": f"bearer {token}", "Content-type": "application/json"}


def _object_id_from_app_id(app_registration_id: str) -> str:
    endpoint = f"applications?$filter=appID eq '{app_registration_id}'"

    data = requests.get(
        f"{GRAPH_BASE_URL}/v1.0/{endpoint}",
        headers=_graph_headers(),
    ).json()

    object_id = data["value"][0]["id"]
    return object_id


def existing_app_registration(display_name: str) -> Optional[str]:
    """Returns application (client) ID with given display_name if it exists,
    otherwise returns None.
    """
    endpoint = f"applications?$filter=displayName eq '{display_name}'"
    data = requests.get(
        f"{GRAPH_BASE_URL}/v1.0/{endpoint}", headers=_graph_headers()
    ).json()

    if data["value"]:
        return data["value"][0]["appId"]
    return None


def create_app_registration(display_name: str) -> str:

    existing_app_id = existing_app_registration(display_name)
    if existing_app_id is not None:
        return existing_app_id

    data = requests.post(
        f"{GRAPH_BASE_URL}/v1.0/applications",
        json={"displayName": display_name, "signInAudience": "AzureADMyOrg"},
        headers=_graph_headers(),
    ).json()

    if "error" in data and data["error"]["code"] == "Authorization_RequestDenied":
        raise PermissionError("Insufficient privileges to create new app registration.")

    return data["appId"]


def create_secret(
    app_registration_id: str, secret_description: str, years: int = 100
) -> str:

    object_id = _object_id_from_app_id(app_registration_id)

    end_datetime = datetime.datetime.now() + datetime.timedelta(days=365.242 * years)
    data = requests.post(
        f"{GRAPH_BASE_URL}/v1.0/applications/{object_id}/addPassword",
        json={
            "passwordCredential": {
                "displayName": secret_description,
                "endDateTime": end_datetime.isoformat(),
            }
        },
        headers=_graph_headers(),
    ).json()

    return data["secretText"]


def add_reply_url(app_registration_id: str, reply_url: str) -> None:
    """Will add web reply url to given app registration id, if it does not alredy exist."""

    object_id = _object_id_from_app_id(app_registration_id)

    data = requests.get(
        f"{GRAPH_BASE_URL}/v1.0/applications/{object_id}",
        headers=_graph_headers(),
    ).json()

    web = data["web"]

    if reply_url not in web["redirectUris"]:
        web["redirectUris"].append(reply_url)

    requests.patch(
        f"{GRAPH_BASE_URL}/v1.0/applications/{object_id}",
        json={"web": web},
        headers=_graph_headers(),
    )


def create_service_principal(app_registration_id: str) -> Tuple[str, str]:

    endpoint = f"servicePrincipals?$filter=appID eq '{app_registration_id}'"
    data = requests.get(
        f"{GRAPH_BASE_URL}/v1.0/{endpoint}",
        headers=_graph_headers(),
    ).json()

    if "error" not in data and data["value"]:
        data_object = data["value"][0]
        if not data_object["appRoleAssignmentRequired"]:
            raise RuntimeError(
                "Service principal already exists, and it does not require app role "
                "assignments. Deployment stopped, as this should be set to true in "
                "order to secure access."
            )
    else:
        data_object = requests.post(
            f"{GRAPH_BASE_URL}/v1.0/servicePrincipals",
            json={"appId": app_registration_id, "appRoleAssignmentRequired": True},
            headers=_graph_headers(),
        ).json()

        if "error" in data:
            raise RuntimeError(f"Graph query failed with response {data}")

    object_id = data_object["id"]
    directory_tenant_id = data_object["appOwnerOrganizationId"]

    return object_id, directory_tenant_id


def azure_app_registration_setup(
    display_name: str, proxy_redirect_url: str
) -> Dict[str, str]:

    azure_pim_already_open = False

    while True:
        try:
            app_registration_id = create_app_registration(display_name)
            object_id, tenant_id = create_service_principal(app_registration_id)
            break
        except PermissionError:
            if not azure_pim_already_open:
                webbrowser.open(f"{PIMCOMMON_URL}/aadmigratedroles")
                azure_pim_already_open = True

                print(
                    "Not able to create new app registration. Do you have enough "
                    "priviliges to do it? We automatically opened the URL to where "
                    "you activate Azure PIM. Please activate necessary priviliges."
                )

            print("New attempt of app registration in 30 seconds.")
            time.sleep(30)

    proxy_client_secret = create_secret(app_registration_id, "cli secret")
    add_reply_url(app_registration_id, proxy_redirect_url)

    return {
        "app_registration_id": app_registration_id,
        "object_id": object_id,
        "proxy_client_secret": proxy_client_secret,
        "proxy_cookie_secret": secrets.token_urlsafe(nbytes=16),
        "proxy_redirect_url": proxy_redirect_url,
        "tenant_id": tenant_id,
    }
