"Peertube graph crawler"
import fileinput
from csv import DictWriter

from .common import CrawlerException, FederationCrawler, fetch_fediverse_instance_list


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

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
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
                if key in self.INSTANCES_CSV_FIELDS
            }
            instance_dict.update(info_dict)

            config_dict = await self._fetch_json("http://" + host + "/api/v1/config")
            instance_dict["serverVersion"] = config_dict.get("serverVersion", "None")

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
            instance_dict["totalPeertubeInstanceFollowers"] = str(len(follower_links))

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
            instance_dict["totalPeertubeInstanceFollowing"] = str(
                len(follower_links)
                - int(instance_dict["totalPeertubeInstanceFollowers"])
            )

        except CrawlerException as err:
            str_err = str(err)
            instance_dict["error"] = str_err
            self.logger.debug("Error with instance " + host + " : " + str_err)
        except KeyError as err:
            str_err = "Missing key in the JSON " + str(err)
            instance_dict["error"] = str_err
            self.logger.debug("Error with instance " + host + " : " + str_err)
        except AttributeError as err:
            str_err = "Unexpected aiohttp-related error with " + host + " : " + str(err)
            instance_dict["error"] = str_err
            self.logger.debug("Error with instance " + host + " : " + str_err)
        else:
            async with self.info_csv_lock:
                with open(self.INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                    writer = DictWriter(csv_file, fieldnames=self.INSTANCES_CSV_FIELDS)
                    writer.writerow(instance_dict)

            async with self.link_csv_lock:
                with open(self.FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                    writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
                    for source, dest in follower_links:
                        writer.writerow({"Source": source, "Target": dest})
            self.logger.debug("Successfully finished crawling of " + host)

    def post_round(self):
        seen = set()

        # Remove duplicate rows
        for line in fileinput.FileInput(self.FOLLOWERS_FILENAME, inplace=True):
            prev_len = len(seen)
            seen.add(line)
            if len(seen) > prev_len:
                print(line, end="")

        super().post_round()


async def launch_peertube_crawl():
    start_urls = await fetch_fediverse_instance_list("peertube")
    async with PeertubeCrawler(start_urls) as crawler:
        await crawler.launch()
