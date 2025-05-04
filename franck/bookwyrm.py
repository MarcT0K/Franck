"""Bookwyrm Graph Crawler"""

from .common import (
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)


class BookwyrmFederationCrawler(FederationCrawler):
    SOFTWARE = "bookwyrm"
    API_ENDPOINTS = ["/api/v1/instance", "/api/v1/instance/peers"]
    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "registration_enabled",
        "description_language",
        "error",
        "Id",
        "Label",
    ]

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        connected_instances = []
        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v1/instance")
            instance_dict["description_language"] = self._detect_language(
                info_dict["description"]
            )
            instance_dict["version"] = info_dict["version"]
            instance_dict["registration_enabled"] = info_dict["registrations"]

            connected_instances = list(
                await self._fetch_json("http://" + host + "/api/v1/instance/peers")
            )

        except CrawlerException as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict)
        await self._write_connected_instance(host, connected_instances)


async def launch_bookwyrm_crawl():
    start_urls = await fetch_fediverse_instance_list("bookwyrm")

    async with BookwyrmFederationCrawler(start_urls) as crawler:
        await crawler.launch()
