from __future__ import annotations

from typing import Any

from dbt.adapters.base.impl import BaseAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.flags import get_flags, Namespace

from dbt.adapters.fal_experimental.utils.environments import (
    EnvironmentDefinition,
)

from dbt.parser.manifest import MacroManifest, Manifest

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    write_df_to_relation,
)

from .utils import extra_path, get_fal_scripts_path, retrieve_symbol


def run_with_adapter(code: str, adapter: BaseAdapter, config: RuntimeConfig) -> Any:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    fal_scripts_path = str(get_fal_scripts_path(config))
    with extra_path(fal_scripts_path):
        main = retrieve_symbol(code, "main")
        return main(
            read_df=prepare_for_adapter(adapter, read_relation_as_df),
            write_df=prepare_for_adapter(adapter, write_df_to_relation),
        )


def _isolated_runner(
    code: str,
    flags: Namespace,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
) -> Any:
    # This function can be run in an entirely separate
    # process or an environment, so we need to reconstruct
    # the DB adapter solely from the config.
    adapter = reconstruct_adapter(flags, config, manifest, macro_manifest)
    return run_with_adapter(code, adapter, config)


def run_in_environment_with_adapter(
    environment: EnvironmentDefinition,
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
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

    result = _isolated_runner(
        code=code,
        flags=get_flags(),
        config=config,
        manifest=manifest,
        macro_manifest=macro_manifest,
    )
    return result
