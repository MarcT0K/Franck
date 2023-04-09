# Copyright (C) 2023  Marc "TOK_" D.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
from csv import DictWriter

import aiohttp
from tqdm.asyncio import tqdm

INSTANCE_CSV_FIELDS = [
    "host",
]
# https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats
# https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows

FOLLOWERS_CSV_FIELDS = ["Source", "Target"]


class CrawlerException(Exception):
    def __init__(self, err):
        self.msg = err

    def __str__(self):
        return self.msg


class PeertubeCrawler:
    def __init__(
        self,
        first_urls=None,
        crawl_depth=-1,
    ):
        self.info_csv_lock = asyncio.Lock()
        self.link_csv_lock = asyncio.Lock()

        with open("instances.csv", "a", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=INSTANCE_CSV_FIELDS)
            writer.writeheader()

        with open("followers.csv", "a", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=FOLLOWERS_CSV_FIELDS)
            writer.writeheader()

        self.session = aiohttp.ClientSession()
        self.max_crawl_depth = crawl_depth

        if first_urls is None:
            self.urls = []
        else:
            assert isinstance(first_urls, list)
            self.urls = first_urls

    async def fetch_instance_list(
        self, url="https://index.kraut.zone/api/v1/instances/hosts"
    ):
        async with self.session.get(url) as resp:
            if resp.status != 200:
                print(
                    f"Failed to fetch the instance list from {url} (code {resp.status})"
                )
            else:
                instances = await resp.json()
                self.urls = [instance["host"] for instance in instances["data"]]

    async def _fetch_json(self, url):
        async with self.session.get(url) as resp:
            if resp.status != 200:
                raise CrawlerException("Error code " + str(resp.status))
            return await resp.json()

    async def inspect_instance(self, url):
        instance_dict = {"host": url}
        try:
            # Fetch instance info
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats/operation/getInstanceStats
            info_dict = await self._fetch_json("http://" + url + "/api/v1/server/stats")

            # Fetch instance followers
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows/paths/~1api~1v1~1server~1followers/get
            followers_dict = await self._fetch_json(
                "http://" + url + "/api/v1/server/followers"
            )

            # Fetch instance followees
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows/paths/~1api~1v1~1server~1following/get
            followee_dict = await self._fetch_json(
                "http://" + url + "/api/v1/server/following"
            )

        except (aiohttp.ClientError, CrawlerException) as err:
            instance_dict["error"] = str(err)

        async with self.info_csv_lock:
            with open("instances.csv", "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=INSTANCE_CSV_FIELDS)
                writer.writerow(instance_dict)

        async with self.link_csv_lock:
            with open("followers.csv", "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=FOLLOWERS_CSV_FIELDS)
                writer.writerow(instance_dict)

    async def check_unknown_urls_in_csv(self):
        raise NotImplementedError

    def drop_duplicate_followers(self):
        raise NotImplementedError

    async def launch(self):
        if not self.urls:
            print("No urls to crawl")

        crawl_done = False
        current_depth = 0

        while not crawl_done:
            tasks = [self.inspect_instance(url) for url in self.urls]

            for task in tqdm.as_completed(tasks):
                await task

            self.urls = await self.check_unknown_urls_in_csv()
            current_depth += 1

            if len(self.urls) == 0 or current_depth == self.max_crawl_depth:
                crawl_done = True

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.session.close()


async def main():
    async with PeertubeCrawler() as crawler:
        await crawler.fetch_instance_list()
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
