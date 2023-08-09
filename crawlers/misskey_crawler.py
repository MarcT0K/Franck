"""Misskey graph crawler."""
import asyncio
import os

from common import Crawler, CrawlerException, fetch_fediverse_instance_list


class MisskeyTopUserCrawler(Crawler):
    SOFTWARE = "lemmy"
    CRAWL_SUBJECT = "top_user"

    INSTANCES_CSV = "instances.csv"
    INSTANCES_FIELDS = ["instance", "users_count", "posts"]
    FOLLOWS_CSV = "follows.csv"
    FOLLOWS_FIELDS = ["Source", "Target", "Weight"]

    TEMP_FOLLOWS_CSV = "temporary.csv"
    TEMP_FOLLOWS_FIELDS = [
        "follower",
        "follower_instance",
        "followee",
        "followee_instance",
    ]

    def __init__(self, first_urls, nb_top_users=100):
        super().__init__(first_urls, 1)

        self.nb_top_users = nb_top_users

        self.instances_file_lock = self.init_csv_file(
            self.INSTANCES_CSV, self.INSTANCES_FIELDS
        )
        self.follows_file_lock = self.init_csv_file(
            self.FOLLOWS_CSV, self.FOLLOWS_FIELDS
        )
        self.temp_file.lock = self.init_csv_file(
            self.TEMP_FOLLOWS_CSV, self.TEMP_FOLLOWS_FIELDS
        )

    async def inspect_instance(self, host):
        try:
            users = await self._crawl_user_list(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the user list of %s: %s", host, str(err)
            )

        for user in users:
            try:
                await self._crawl_user_interactions(host, user)  # Followers
                await self._crawl_user_interactions(
                    host, user, follower_list=False
                )  # Following
            except CrawlerException as err:
                self.logger.debug(
                    "Error while crawling the interactions of %s of %s: %s",
                    user,
                    host,
                    str(err),
                )

    async def _crawl_user_list(self, host):
        # https://misskey.io/api/meta
        # https://misskey.io/api/users
        users = []

        while len(users) < self.nb_top_users:
            ...

    async def _crawl_user_interactions(self, host, username, follower_list=True):
        # https://misskey.io/api-doc#tag/users/operation/users/followers
        # https://misskey.io/api-doc#tag/users/operation/users/followers
        ...

    async def data_cleaning(self):
        # TODO

        os.remove(self.TEMP_FOLLOWS_CSV)


async def main():
    start_urls = await fetch_fediverse_instance_list("misskey")

    async with MisskeyTopUserCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
