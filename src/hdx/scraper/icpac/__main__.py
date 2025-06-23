#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join
from typing import Any, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.geonode.geonodetohdx import GeoNodeToHDX
from hdx.scraper.icpac._version import __version__
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-icpac"


def process_dataset_name(name):
    return name.replace("igad-climate-prediction-and-application-center", "icpac")


def create_dataset_showcase(
    dataset: Dataset, showcase: Showcase, **kwargs: Any
) -> None:
    """
    Create dataset and showcase

    Args:
        dataset (Dataset): Dataset to create
        showcase (Showcase): Showcase to create
        **kwargs: Args to pass to dataset create_in_hdx call

    Returns:
        None

    """
    dataset.update_from_yaml(
        script_dir_plus_file(join("config", "hdx_dataset_static.yaml"), main)
    )
    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, **kwargs)
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)


def delete_from_hdx(dataset: Dataset) -> None:
    """
    Delete dataset and any associated showcases

    Args:
        dataset (Dataset): Dataset to delete

    Returns:
        None

    """
    if dataset["name"][:6] == "igad-":
        return
    logger.info(f"Deleting {dataset['title']} and any associated showcases")
    for showcase in dataset.get_showcases():
        showcase.delete_from_hdx()
    dataset.delete_from_hdx()


def main(verify_ssl: Optional[str] = None):
    """Generate dataset and create it in HDX

    Args:
        verify_ssl (Optional[str]): Whether to verify SSL certificates. Defaults to None (verify).

    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    metadata = {
        "maintainerid": "196196be-6037-4488-8b71-d786adf4c081",
        "orgid": "04436cdf-24da-4826-b5b8-67cba9962423",
    }
    User.check_current_user_write_access(metadata["orgid"], configuration=configuration)
    if verify_ssl is None:
        verify_ssl = getenv("VERIFYSSL", "Y")
    if verify_ssl.lower() in ("false", "n", ""):
        verify_ssl = False
        logger.info("SSL certificate verification is disabled!")
    else:
        verify_ssl = True
    with Download(verify=verify_ssl) as downloader:
        base_url = configuration["base_url"]
        geonodetohdx = GeoNodeToHDX(base_url, downloader)
        datasets = geonodetohdx.generate_datasets_and_showcases(
            metadata,
            create_dataset_showcase=create_dataset_showcase,
            use_count=False,
            process_dataset_name=process_dataset_name,
            updated_by_script="HDX Scraper: ICPAC",
        )
        geonodetohdx.delete_other_datasets(datasets, metadata)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
