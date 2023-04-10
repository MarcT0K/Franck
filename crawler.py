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
import fileinput
import json
from csv import DictWriter, DictReader

import aiohttp
from tqdm.asyncio import tqdm

INSTANCE_CSV_FIELDS = [
    "host",
    "totalUsers",
    "totalDailyActiveUsers",
    "totalWeeklyActiveUsers",
    "totalMonthlyActiveUsers",
    "totalLocalVideos",
    "totalLocalVideoViews",
    "totalVideos",
    "totalInstanceFollowers",
    "totalPeertubeInstanceFollowers",
    "totalInstanceFollowing",
    "totalPeertubeInstanceFollowing",
    "totalLocalPlaylists",
    "totalVideoComments",
    "error",
    "Id",
    "Label",
]
FOLLOWERS_CSV_FIELDS = ["Source", "Target"]
INSTANCES_FILENAME = "instances.csv"
FOLLOWERS_FILENAME = "followers.csv"


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

        with open(INSTANCES_FILENAME, "w", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=INSTANCE_CSV_FIELDS)
            writer.writeheader()

        with open(FOLLOWERS_FILENAME, "w", encoding="utf-8") as csv_file:
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
        instances = await self._fetch_json(url)
        self.urls = [instance["host"] for instance in instances["data"]]

    async def _fetch_json(self, url, params=None):
        try:
            async with self.session.get(url, timeout=300, params=params) as resp:
                if resp.status != 200:
                    raise CrawlerException(f"Error code {str(resp.status)} on {url}")
                data = await resp.read()
                try:
                    return json.loads(data)
                except (json.JSONDecodeError, UnicodeDecodeError) as err:
                    raise CrawlerException(
                        f"Cannot decode JSON on {url} ({err})"
                    ) from err
        except aiohttp.ClientError as err:
            raise CrawlerException(f"{err}") from err
        except asyncio.TimeoutError as err:
            raise CrawlerException(f"Connection to {url} timed out") from err

    async def inspect_instance(self, host):
        instance_dict = {"host": host}
        follower_links = []
        try:
            # Fetch instance info
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats/operation/getInstanceStats
            info_dict = await self._fetch_json(
                "http://" + host + "/api/v1/server/stats"
            )
            info_dict = {
                key: val for key, val in info_dict.items() if key in INSTANCE_CSV_FIELDS
            }
            instance_dict.update(info_dict)

            # Fetch instance followers
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows/paths/~1api~1v1~1server~1followers/get
            followees_dict = await self._fetch_json(
                "http://" + host + "/api/v1/server/followers",
            )
            instance_dict["totalInstanceFollowers"] = followees_dict["total"]
            for i in range(0, instance_dict["totalInstanceFollowers"], 100):
                followers_dict = await self._fetch_json(
                    "http://" + host + "/api/v1/server/followers",
                    params={"count": 100, "start": i},
                )
                for link_dict in followers_dict["data"]:
                    if link_dict["follower"]["name"] == "peertube":
                        # We avoid Mastodon followers
                        follower_links.append((link_dict["follower"]["host"], host))
            instance_dict["totalPeertubeInstanceFollowers"] = len(follower_links)

            # Fetch instance followees
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows/paths/~1api~1v1~1server~1following/get
            followees_dict = await self._fetch_json(
                "http://" + host + "/api/v1/server/following",
            )
            instance_dict["totalInstanceFollowing"] = followees_dict["total"]
            for i in range(0, instance_dict["totalInstanceFollowing"], 100):
                followees_dict = await self._fetch_json(
                    "http://" + host + "/api/v1/server/following",
                    params={"count": 100, "start": i},
                )
                for link_dict in followees_dict["data"]:
                    if link_dict["following"]["name"] == "peertube":
                        # We avoid Mastodon followers
                        follower_links.append((host, link_dict["following"]["host"]))
            instance_dict["totalPeertubeInstanceFollowing"] = (
                len(follower_links) - instance_dict["totalPeertubeInstanceFollowers"]
            )

        except CrawlerException as err:
            instance_dict["error"] = str(err)

        async with self.info_csv_lock:
            with open(INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=INSTANCE_CSV_FIELDS)
                writer.writerow(instance_dict)

        async with self.link_csv_lock:
            with open(FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=FOLLOWERS_CSV_FIELDS)
                for source, dest in follower_links:
                    writer.writerow({"Source": source, "Target": dest})

    def check_unknown_urls_in_csv(self):
        crawled = set()
        with open(INSTANCES_FILENAME, encoding="utf-8") as csvfile:
            data = DictReader(csvfile)
            for row in data:
                crawled.add(row["host"])

        from_links = set()
        with open(FOLLOWERS_FILENAME, encoding="utf-8") as csvfile:
            data = DictReader(csvfile)
            for row in data:
                from_links.add(row["Source"])
                from_links.add(row["Target"])

        return list(from_links - crawled)

    def drop_duplicate_followers(self):
        seen = set()
        for line in fileinput.FileInput(FOLLOWERS_FILENAME, inplace=True):
            prev_len = len(seen)
            seen.add(line)
            if len(seen) > prev_len:
                print(line, end="")

    def data_cleaning(self):
        working_instances = set()
        with open(INSTANCES_FILENAME, encoding="utf-8") as rawfile, open(
            "clean_" + INSTANCES_FILENAME, "w", encoding="utf-8"
        ) as cleanfile:
            data = DictReader(rawfile)
            writer = DictWriter(cleanfile, fieldnames=INSTANCE_CSV_FIELDS)
            writer.writeheader()
            for row in data:
                if row["error"] == "":
                    working_instances.add(row["host"])
                    row["Id"] = row["host"]
                    row["Label"] = row["host"]
                    writer.writerow(row)

        with open(FOLLOWERS_FILENAME, encoding="utf-8") as rawfile, open(
            "clean_" + FOLLOWERS_FILENAME, "w", encoding="utf-8"
        ) as cleanfile:
            data = DictReader(rawfile)
            writer = DictWriter(cleanfile, fieldnames=FOLLOWERS_CSV_FIELDS)
            writer.writeheader()
            for row in data:
                if (
                    row["Source"] in working_instances
                    and row["Target"] in working_instances
                ):
                    writer.writerow(row)

    async def launch(self):
        if not self.urls:
            raise CrawlerException("No URL to crawl")

        crawl_done = False
        current_depth = 1

        print("Crawl begins...")
        while not crawl_done:
            print("Crawling round ", current_depth)
            tasks = [self.inspect_instance(url) for url in self.urls]

            for task in tqdm.as_completed(tasks):
                await task

            self.drop_duplicate_followers()
            self.urls = self.check_unknown_urls_in_csv()

            if len(self.urls) == 0 or current_depth == self.max_crawl_depth:
                crawl_done = True
            current_depth += 1
        print("Crawl completed!!!")
        print("Cleaning the data...")
        self.data_cleaning()
        print("Done.")

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
