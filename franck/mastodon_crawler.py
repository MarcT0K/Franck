"""Mastodon Graph Crawler"""

import asyncio

from csv import DictWriter, DictReader
import urllib.parse

import scipy.sparse as sp

from .common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)

DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


class MastodonFederationCrawler(FederationCrawler):
    SOFTWARE = "mastodon"
    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "active_users",
        "languages",
        "registration_enabled",
        "error",
        "Id",
        "Label",
    ]

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        linked_instances = []
        blocked_instances = []

        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v2/instance")
            instance_dict["version"] = info_dict["version"]
            instance_dict["active_users"] = info_dict["usage"]["users"]["active_month"]
            instance_dict["languages"] = "/".join(info_dict["languages"])

            linked_instances = await self._fetch_json(
                "http://" + host + "/api/v1/instance/peers"
            )
            # blocked_instances = await self._fetch_json(
            #     "http://" + host + "/api/v1/instance/domain_blocks"
            # ) # Not always publicly available => removed for consistency
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

                # for dest in blocked_instances:
                #     writer.writerow({"Source": host, "Target": dest, "Weight": -1})


async def launch_mastodon_crawl():
    start_urls = await fetch_fediverse_instance_list("lemmy")
    start_urls = ["mastodon.social", "mastodon.acm.org"]  # FOR DEBUG

    async with MastodonFederationCrawler(start_urls) as crawler:
        await crawler.launch()
