"""Bookwyrm Graph Crawler"""

from csv import DictWriter

from .common import (
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)

DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


class BookwyrmFederationCrawler(FederationCrawler):
    SOFTWARE = "bookwyrm"
    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "registration_enabled",
        "error",
        "Id",
        "Label",
    ]

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        linked_instances = []
        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v1/instance")
            instance_dict["version"] = info_dict["version"]
            instance_dict["registration_enabled"] = info_dict["registrations"]

            linked_instances = await self._fetch_json(
                "http://" + host + "/api/v1/instance/peers"
            )

        except CrawlerException as err:
            instance_dict["error"] = str(err)

        async with self.csv_locks[self.INSTANCES_FILENAME]:
            with open(self.INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.INSTANCES_CSV_FIELDS)
                writer.writerow(instance_dict)

        async with self.csv_locks[self.FOLLOWERS_FILENAME]:
            with open(self.FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
                for dest in linked_instances:
                    writer.writerow({"Source": host, "Target": dest, "Weight": 1})


async def launch_bookwyrm_crawl():
    start_urls = await fetch_fediverse_instance_list("bookwyrm")

    async with BookwyrmFederationCrawler(start_urls) as crawler:
        await crawler.launch()
