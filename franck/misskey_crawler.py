"""Misskey graph crawler."""

import asyncio

from csv import DictWriter, DictReader

from .common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)


DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


class MisskeyFederationCrawler(FederationCrawler):
    SOFTWARE = "misskey"
    INSTANCES_CSV_FIELDS = [
        "host",
        "users_count",
        "posts_count",
        "error",
        "Id",
        "Label",
    ]

    MAX_PAGE_SIZE = 30

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        connected_instances = []

        try:
            stats_dict = await self._fetch_json(
                "https://" + host + "/api/stats", body={}, op="POST"
            )
            instance_dict["users_count"] = stats_dict["originalUsersCount"]
            instance_dict["posts_count"] = stats_dict["originalNotesCount"]

            offset = 0
            while True:
                body = {
                    "limit": self.MAX_PAGE_SIZE,
                    "offset": offset,
                    "sort": "+users",
                }
                resp = await self._fetch_json(
                    "https://" + host + "/api/federation/instances",
                    body=body,
                    op="POST",
                )

                new_connected_instances = [
                    inst_dict["host"]
                    for inst_dict in resp
                    if inst_dict["softwareName"] == "misskey"
                ]
                # The conditions limits the number of false positive entries that need to be cleaned later.

                connected_instances += new_connected_instances

                if offset > 10**5:
                    raise CrawlerException("Infinite loop problem")

                if len(resp) < self.MAX_PAGE_SIZE:
                    break

                offset += self.MAX_PAGE_SIZE

                await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

        except CrawlerException as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict=instance_dict)
        await self._write_connected_instance(host, connected_instances)


class MisskeyTopUserCrawler(Crawler):
    SOFTWARE = "misskey"
    CRAWL_SUBJECT = "top_user"

    INSTANCES_CSV_FIELDS = [
        "instance",
        "users_count",
        "posts_count",
        "error",
        "Id",
        "Label",
    ]

    CRAWLED_FOLLOWS_CSV = "detailed_follows.csv"
    CRAWLED_FOLLOWS_FIELDS = [
        "follower",
        "follower_instance",
        "followee",
        "followee_instance",
    ]
    CRAWLED_USERS_CSV = "crawled_users.csv"
    CRAWLED_USERS_FIELDS = [
        "id",
        "username",
        "instance",
        "followers_count",
        "following_count",
        "posts_count",
        "lang",
    ]

    TEMP_FILES = [CRAWLED_FOLLOWS_CSV, CRAWLED_USERS_CSV]

    MAX_PAGE_SIZE = 100

    def __init__(self, urls, nb_top_users=1000):
        super().__init__(urls)

        self.nb_top_users = nb_top_users

        self.csv_information = [
            (self.INSTANCES_CSV, self.INSTANCES_CSV_FIELDS),
            (self.INTERACTIONS_CSVS[0], self.INTERACTIONS_CSV_FIELDS),
            (self.CRAWLED_FOLLOWS_CSV, self.CRAWLED_FOLLOWS_FIELDS),
            (self.CRAWLED_USERS_CSV, self.CRAWLED_USERS_FIELDS),
        ]

    async def inspect_instance(self, host):
        await self._fetch_instance_stats(host)

        try:
            users = await self._crawl_user_list(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the user list of %s: %s", host, str(err)
            )
            return

        for user in users:
            try:
                if user["followersCount"] > 0:
                    await self._crawl_user_interactions(host, user)
            except CrawlerException as err:
                self.logger.debug(
                    "Error while crawling the interactions of %s of %s: %s",
                    user["id"],
                    host,
                    str(err),
                )

    async def _fetch_instance_stats(self, host):
        # https://misskey.io/api/stats
        instance_dict = {"instance": host}
        try:
            stats_dict = await self._fetch_json(
                "https://" + host + "/api/stats", body={}, op="POST"
            )

            instance_dict["users_count"] = stats_dict["originalUsersCount"]
            instance_dict["posts_count"] = stats_dict["originalNotesCount"]

        except Exception as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict)

    async def _crawl_user_list(self, host):
        # https://misskey.io/api/users
        users = []
        offset = 0

        while len(users) < self.nb_top_users:
            nb_missing_users = self.nb_top_users - len(users)
            body = {
                "limit": min(self.MAX_PAGE_SIZE, nb_missing_users),
                "offset": offset,
                "origin": "local",
                "sort": "+follower",
            }
            resp = await self._fetch_json(
                "https://" + host + "/api/users", body=body, op="POST"
            )

            users.extend(resp)

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            offset += self.MAX_PAGE_SIZE

            await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

        lock, _file, writer = self.csvs[self.CRAWLED_USERS_CSV]
        async with lock:
            for user in users:
                writer.writerow(
                    {
                        "id": user["id"],
                        "username": user["username"],
                        "instance": host,
                        "followers_count": user["followersCount"],
                        "following_count": user["followingCount"],
                        "posts_count": user["notesCount"],
                        "lang": user.get("lang"),
                    }
                )

        return users

    async def _crawl_user_interactions(self, host, user_info):
        follow_dicts = []

        last_id = "0"
        while True:
            body = {
                "limit": self.MAX_PAGE_SIZE,
                "sinceId": last_id,
                "userId": user_info["id"],
                "host": host,
            }
            resp = await self._fetch_json(
                "https://" + host + "/api/users/followers", body=body, op="POST"
            )

            host_check = lambda host_input: host if host_input is None else host_input

            for follow_dict in resp:
                follower_instance = host_check(follow_dict["follower"]["host"])
                if follower_instance in self.crawled_instances:
                    follow_dicts.append(
                        {
                            "follower": follow_dict["follower"]["username"],
                            "follower_instance": follower_instance,
                            "followee": user_info["username"],
                            "followee_instance": host_check(user_info["host"]),
                        }
                    )

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            last_id = resp[-1]["id"]

            await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

        lock, _file, writer = self.csvs[self.CRAWLED_FOLLOWS_CSV]
        async with lock:
            for follow in follow_dicts:
                writer.writerow(follow)

    def data_postprocessing(self):
        follows_dict = {}
        with open(self.CRAWLED_FOLLOWS_CSV, "r", encoding="utf-8") as csv_file:
            reader = DictReader(csv_file, fieldnames=self.CRAWLED_FOLLOWS_FIELDS)
            next(reader, None)  # Skip the header
            for follow in reader:
                follower = follow["follower_instance"]
                followee = follow["followee_instance"]
                prev_follower = follows_dict.get(follower, {})
                prev_follower[followee] = prev_follower.get(followee, 0) + 1
                follows_dict[follower] = prev_follower

        with open(self.INTERACTIONS_CSVS[0], "a", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=self.INTERACTIONS_CSV_FIELDS)
            for follower, followees_dict in follows_dict.items():
                for followee, follows_count in followees_dict.items():
                    writer.writerow(
                        {
                            "Source": follower,
                            "Target": followee,
                            "Weight": follows_count,
                        }
                    )


async def launch_misskey_crawl():
    start_urls = await fetch_fediverse_instance_list("misskey")
    # start_urls = ["pari.cafe", "mi.yumechi.jp", "misskey.io"]  # For debug purpose

    async with MisskeyFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with MisskeyTopUserCrawler(start_urls) as crawler:
        await crawler.launch()
