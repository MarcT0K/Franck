"""Pleroma/Akkoma Graph Crawler"""

import asyncio

from csv import DictWriter

from .common import fetch_fediverse_instance_list
from .mastodon_crawler import MastodonActiveUserCrawler, MastodonFederationCrawler

DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


class PleromaFederationCrawler(MastodonFederationCrawler):
    SOFTWARE = "pleroma"
    INSTANCE_INFO_API = "/api/v1/instance"


class PleromaActiveUserCrawler(MastodonActiveUserCrawler):
    SOFTWARE = "pleroma"
    INSTANCE_INFO_API = "/api/v1/instance"
    MAX_PAGE_SIZE = 40
    MAX_ID_REGEX = r"max_id=([a-zA-Z0-9]+)"

    # async def _crawl_user_interactions(self, host, user_info):
    #     follow_dicts = {}

    #     offset = 0
    #     while True:
    #         params = {"offset": offset, "limit": 40}
    #         resp = await self._fetch_json(
    #             f"https://{host}/api/v1/accounts/{user_info['id']}/following",
    #             params=params,
    #         )

    #         for followee_dict in resp:
    #             # NB: Sometimes, the API was returning some duplicates (idk why...)
    #             #   Using a dictionary instead of a list avoid these duplicates
    #             followee_instance = (
    #                 followee_dict["acct"].split("@")[1]
    #                 if "@" in followee_dict["acct"]
    #                 else host
    #             )

    #             follow_dicts[followee_dict["username"]] = {
    #                 "followee": followee_dict["username"],
    #                 "followee_instance": followee_instance,
    #                 "follower": user_info["username"],
    #                 "follower_instance": host,
    #             }

    #         if len(resp) == 0:
    #             if len(follow_dicts) == 0 and user_info["following_count"] != 0:
    #                 self.logger.debug(
    #                     "User %s@%s set its follower list as private.",
    #                     user_info["username"],
    #                     host,
    #                 )
    #             break

    #         offset += self.MAX_PAGE_SIZE
    #         await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

    #     async with self.csv_locks[self.CRAWLED_FOLLOWS_CSV]:
    #         with open(self.CRAWLED_FOLLOWS_CSV, "a", encoding="utf-8") as csv_file:
    #             writer = DictWriter(csv_file, fieldnames=self.CRAWLED_FOLLOWS_FIELDS)
    #             for follow in follow_dicts.values():
    #                 writer.writerow(follow)


async def launch_pleroma_crawl():
    start_urls = await fetch_fediverse_instance_list("pleroma")
    start_urls += await fetch_fediverse_instance_list("akkoma")
    start_urls = ["poa.st", "spinster.xyz", "fe.disroot.org"]  # FOR DEBUG

    async with PleromaFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with PleromaActiveUserCrawler(start_urls) as crawler:
        await crawler.launch()
