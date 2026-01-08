from __future__ import annotations

import functools
import pandas as pd
from typing import Any, Callable, Dict, NewType

from dbt.config.runtime import RuntimeConfig
from dbt.adapters.contracts.connection import AdapterResponse

from dbt.adapters.fal_experimental.connections import TeleportTypeEnum
from dbt.adapters.fal_experimental.utils.environments import (
    EnvironmentDefinition,
)
from dbt.adapters.fal_experimental.utils import (
    extra_path,
    get_fal_scripts_path,
    retrieve_symbol,
)
from dbt.fal.adapters.teleport.info import TeleportInfo


DataLocation = NewType("DataLocation", Dict[str, str])


def _prepare_for_teleport(
    function: Callable, teleport: TeleportInfo, locations: DataLocation
) -> Callable:
    @functools.wraps(function)
    def wrapped(relation: str, *args, **kwargs) -> Any:
        relation = relation.lower()
        return function(teleport, locations, relation, *args, **kwargs)

    return wrapped


def _teleport_df_from_external_storage(
    teleport_info: TeleportInfo, locations: DataLocation, relation: str
) -> pd.DataFrame:
    if relation not in locations:
        raise RuntimeError(f"Could not find url for '{relation}' in {locations}")

    if teleport_info.format == "parquet":
        relation_path = locations[relation]
        url = teleport_info.build_url(relation_path)
        storage_options = _build_teleport_storage_options(teleport_info)
        return pd.read_parquet(url, storage_options=storage_options)
    else:
        # TODO: support more
        raise RuntimeError(f"Format {teleport_info.format} not supported")


def _teleport_df_to_external_storage(
    teleport_info: TeleportInfo,
    locations: DataLocation,
    relation: str,
    data: pd.DataFrame,
):
    if teleport_info.format == "parquet":
        relation_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(relation_path)
        storage_options = _build_teleport_storage_options(teleport_info)

        data.to_parquet(url, storage_options=storage_options)
        locations[relation] = relation_path
        return relation_path
    else:
        raise RuntimeError(f"Format {teleport_info.format} not supported")


def _build_teleport_storage_options(teleport_info: TeleportInfo) -> Dict[str, str]:
    storage_options = {}
    if teleport_info.credentials.type == TeleportTypeEnum.REMOTE_S3:
        storage_options = {
            "key": teleport_info.credentials.s3_access_key_id,
            "secret": teleport_info.credentials.s3_access_key,
        }
    elif teleport_info.credentials.type == TeleportTypeEnum.LOCAL:
        pass
    else:
        raise RuntimeError(
            f"Teleport storage type {teleport_info.credentials.type} not supported"
        )
    return storage_options


def run_with_teleport(
    code: str,
    teleport_info: TeleportInfo,
    locations: DataLocation,
    config: RuntimeConfig,
) -> str:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    fal_scripts_path = str(get_fal_scripts_path(config))

    with extra_path(fal_scripts_path):
        main = retrieve_symbol(code, "main")
        return main(
            read_df=_prepare_for_teleport(
                _teleport_df_from_external_storage, teleport_info, locations
            ),
            write_df=_prepare_for_teleport(
                _teleport_df_to_external_storage, teleport_info, locations
            ),
        )


def run_in_environment_with_teleport(
    environment: EnvironmentDefinition,
    code: str,
    teleport_info: TeleportInfo,
    locations: DataLocation,
    config: RuntimeConfig,
    adapter_type: str,
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""

    # Only local execution is supported
    if environment.kind != "local":
        raise NotImplementedError(
            f"Environment kind '{environment.kind}' is not supported. "
            "Only 'local' execution is available. Remote/cloud execution "
            "via fal serverless has been removed."
        )

    result = run_with_teleport(
        code=code,
        teleport_info=teleport_info,
        locations=locations,
        config=config,
    )
    return result
