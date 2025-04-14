"Peertube graph crawler"
import csv
import os

from .common import CrawlerException, FederationCrawler, fetch_fediverse_instance_list


class PeertubeCrawler(FederationCrawler):
    SOFTWARE = "peertube"
    CRAWL_SUBJECT = "follow"

    API_ENDPOINTS = [
        "/api/v1/server/following",
        "/api/v1/server/followers",
        "/api/v1/server/stats",
        "/api/v1/config",
    ]
    INSTANCES_CSV_FIELDS = [
        "host",
        "totalUsers",
        "totalDailyActiveUsers",
        "totalWeeklyActiveUsers",
        "totalMonthlyActiveUsers",
        "totalLocalVideos",
        "totalVideos",
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

            # Fetch instance followees
            # https://docs.joinpeertube.org/api-rest-reference.html#tag/Instance-Follows/paths/~1api~1v1~1server~1following/get
            followees_dict = await self._fetch_json(
                "http://" + host + "/api/v1/server/following",
            )
            for i in range(0, followees_dict["total"], 100):
                followees_dict = await self._fetch_json(
                    "http://" + host + "/api/v1/server/following",
                    params={"count": 100, "start": i},
                )
                for link_dict in followees_dict["data"]:
                    if link_dict["follower"]["host"] in self.crawled_instances:
                        follower_links.append((host, link_dict["following"]["host"]))

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

        await self._write_instance_csv(instance_dict)

        assert len(self.INTERACTIONS_CSVS) == 1
        lock, _file, writer = self.csvs[self.INTERACTIONS_CSVS[0]]
        async with lock:
            for source, dest in follower_links:
                writer.writerow({"Source": source, "Target": dest, "Weight": 1})

    def data_postprocessing(
        self,
    ):  # A few instances generate duplicates for some unknwon reason...
        seen_rows = set()
        unique_rows = []

        with open(
            self.INTERACTIONS_CSVS[0], "r", newline="", encoding="utf-8"
        ) as infile:
            reader = csv.reader(infile)
            header = next(reader, None)
            if header:
                unique_rows.append(header)
                seen_rows.add(tuple(header))
            for row in reader:
                row_tuple = tuple(row)
                if row_tuple not in seen_rows:
                    unique_rows.append(row)
                    seen_rows.add(row_tuple)

        # Create a temporary file
        with open("temp.csv", "w", newline="", encoding="utf-8") as tmpfile:
            writer = csv.writer(tmpfile)
            writer.writerows(unique_rows)

        # Replace the original file with the temporary file
        os.replace("temp.csv", self.INTERACTIONS_CSVS[0])


async def launch_peertube_crawl():
    start_urls = await fetch_fediverse_instance_list("peertube")
    async with PeertubeCrawler(start_urls) as crawler:
        await crawler.launch()
