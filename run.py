#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.api.configuration import Configuration
from hdx.scraper.geonode.geonodetohdx import GeoNodeToHDX
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-icpac"


def process_dataset_name(name):
    return name.replace("igad-climate-prediction-and-application-center", "icpac")


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


def main():
    """Generate dataset and create it in HDX"""

    with Download() as downloader:
        configuration = Configuration.read()
        base_url = configuration["base_url"]
        geonodetohdx = GeoNodeToHDX(base_url, downloader)
        metadata = {
            "maintainerid": "196196be-6037-4488-8b71-d786adf4c081",
            "orgid": "04436cdf-24da-4826-b5b8-67cba9962423",
        }
        datasets = geonodetohdx.generate_datasets_and_showcases(metadata,
                                                                use_count=False,
                                                                process_dataset_name=process_dataset_name,
                                                                updated_by_script="HDX Scraper: ICPAC")
        geonodetohdx.delete_other_datasets(datasets, metadata)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
