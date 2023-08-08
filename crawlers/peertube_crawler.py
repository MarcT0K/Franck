"Peertube graph crawler"

import asyncio
import fileinput

from csv import DictWriter

from common import FederationCrawler, CrawlerException


class PeertubeCrawler(FederationCrawler):
    SOFTWARE = "peertube"
    INSTANCES_CSV_FIELDS = [
        "host",
        "totalUsers",
        "totalDailyActiveUsers",
        "totalWeeklyActiveUsers",
        "totalMonthlyActiveUsers",
        "totalLocalVideos",
        "totalVideos",
        "totalInstanceFollowers",
        "totalPeertubeInstanceFollowers",
        "totalInstanceFollowing",
        "totalPeertubeInstanceFollowing",
        "totalLocalPlaylists",
        "totalVideoComments",
        "totalLocalVideoComments",
        "totalLocalVideoViews",
        "serverVersion",
        "error",
        "Id",
        "Label",
    ]

    async def fetch_instance_list(
        self, url: str = "https://index.kraut.zone/api/v1/instances/hosts"
    ):
        instances = await self._fetch_json(url)
        self.urls = [instance["host"] for instance in instances["data"]]

    async def inspect_instance(self, host: str):
        instance_dict = {"host": host}
        follower_links = []
        try:
            # Fetch instance info
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats/operation/getInstanceStats
            info_dict = await self._fetch_json(
                "http://" + host + "/api/v1/server/stats"
            )
            info_dict = {
                key: val
                for key, val in info_dict.items()
                if key in self.INSTANCE_CSV_FIELDS
            }
            instance_dict.update(info_dict)

            config_dict = await self._fetch_json("http://" + host + "/api/v1/config")
            instance_dict["serverVersion"] = config_dict["server_version"]

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
            with open(self.INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.INSTANCE_CSV_FIELDS)
                writer.writerow(instance_dict)

        async with self.link_csv_lock:
            with open(self.FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
                for source, dest in follower_links:
                    writer.writerow({"Source": source, "Target": dest})

    def post_round(self):
        seen = set()

        # Remove duplicate rows
        for line in fileinput.FileInput(self.FOLLOWERS_FILENAME, inplace=True):
            prev_len = len(seen)
            seen.add(line)
            if len(seen) > prev_len:
                print(line, end="")

        super(FederationCrawler, self).post_round()


async def main():
    async with PeertubeCrawler() as crawler:
        await crawler.fetch_instance_list()
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
