"""Misskey graph crawler."""

import asyncio

from csv import DictWriter, DictReader

from .common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)


class MisskeyFederationCrawler(FederationCrawler):
    SOFTWARE = "misskey"
    API_ENDPOINTS = [
        "/api/federation/instances",
        "/api/stats",
        "/api/meta",
    ]
    INSTANCES_CSV_FIELDS = [
        "host",
        "users_count",
        "posts_count",
        "languages",
        "description_language",
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

            meta_dict = await self._fetch_json(
                "https://" + host + "/api/meta", body={"detail": True}, op="POST"
            )
            instance_dict["languages"] = meta_dict["langs"]
            instance_dict["description_language"] = self._detect_language(
                meta_dict["description"]
            )

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

                await asyncio.sleep(self._get_crawl_delay(host))

        except CrawlerException as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict=instance_dict)
        await self._write_connected_instance(host, connected_instances)


class MisskeyActiveUserCrawler(Crawler):
    SOFTWARE = "misskey"
    CRAWL_SUBJECT = "active_user"
    API_ENDPOINTS = ["/api/stats", "/api/users", "/api/users/following", "/api/meta"]

    INSTANCES_CSV_FIELDS = [
        "host",
        "users_count",
        "posts_count",
        "languages",
        "description_language",
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
    FAILURE_TOLERANCE = 0.05

    def __init__(self, urls, nb_active_users=10000):
        super().__init__(urls)

        self.nb_active_users = nb_active_users

        self.csv_information = [
            (self.INSTANCES_CSV, self.INSTANCES_CSV_FIELDS),
            (self.INTERACTIONS_CSVS[0], self.INTERACTIONS_CSV_FIELDS),
            (self.CRAWLED_FOLLOWS_CSV, self.CRAWLED_FOLLOWS_FIELDS),
            (self.CRAWLED_USERS_CSV, self.CRAWLED_USERS_FIELDS),
        ]

    async def inspect_instance(self, host):
        instance_dict = await self._fetch_instance_stats(host)

        try:
            users = await self._crawl_user_list(host)
        except CrawlerException as err:
            users = []
            err_msg = f"Error while crawling the user list of {host}: " + str(err)
            self.logger.debug(err_msg)
            instance_dict["error"] = err_msg

        nb_failure = 0
        for i, user in enumerate(users):
            self.logger.debug(
                "Instance %s: %d users out of %d crawled", host, i, len(users)
            )
            try:
                if user["followingCount"] == "?":
                    self.logger.debug(
                        "Instance %s: user %s has an unknown number of followers [user ignored]",
                        host,
                        user["username"],
                    )
                elif user["followingCount"] > 0:
                    await self._crawl_user_interactions(host, user)
                elif user["followingCount"] == 0:
                    self.logger.debug(
                        "Instance %s: user %s has no followee [user ignored]",
                        host,
                        user["username"],
                    )
                else:
                    self.logger.debug(
                        "Instance %s: user %s has an invalid followee cound: %d [user ignored]",
                        host,
                        user["username"],
                        user["followingCount"],
                    )
            except CrawlerException as err:
                err_msg = (
                    f"Error while crawling the interactions of {user['id']} of {host}: "
                    + str(err)
                )
                self.logger.debug(err_msg)
                nb_failure += 1

            if nb_failure > self.FAILURE_TOLERANCE * len(users):
                instance_dict["error"] = (
                    "Too many failures while crawling user interactions"
                )
                break

        await self._write_instance_csv(instance_dict)

    async def _fetch_instance_stats(self, host):
        instance_dict = {"host": host}
        try:
            stats_dict = await self._fetch_json(
                "https://" + host + "/api/stats", body={}, op="POST"
            )

            instance_dict["users_count"] = stats_dict["originalUsersCount"]
            instance_dict["posts_count"] = stats_dict["originalNotesCount"]

            meta_dict = await self._fetch_json(
                "https://" + host + "/api/meta", body={}, op="POST"
            )
            instance_dict["languages"] = meta_dict["langs"]
            instance_dict["description_language"] = self._detect_language(
                meta_dict["description"]
            )
        except Exception as err:
            instance_dict["error"] = str(err)

        return instance_dict

    async def _crawl_user_list(self, host):
        users = {}
        offset = 0

        while len(users) < self.nb_active_users:
            nb_missing_users = self.nb_active_users - len(users)
            body = {
                "limit": min(self.MAX_PAGE_SIZE, nb_missing_users),
                "offset": offset,
                "origin": "local",
                "sort": "+updatedAt",
            }
            resp = await self._fetch_json(
                "https://" + host + "/api/users", body=body, op="POST"
            )

            for user in resp:
                users[user["username"]] = user
                # Since users can publish at anytime,
                # The list of active users can shift
                # The shift could cause a give user to be seen multiple times

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            offset += self.MAX_PAGE_SIZE

            await asyncio.sleep(self._get_crawl_delay(host))

        user_list = users.values()
        lock, _file, writer = self.csvs[self.CRAWLED_USERS_CSV]
        async with lock:
            for user in user_list:
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

        return user_list

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
                "https://" + host + "/api/users/following", body=body, op="POST"
            )

            host_check = lambda host_input: host if host_input is None else host_input

            for follow_dict in resp:
                followee_instance = host_check(follow_dict["followee"]["host"])
                if followee_instance in self.crawled_instances:
                    follow_dicts.append(
                        {
                            "followee": follow_dict["followee"]["username"],
                            "followee_instance": followee_instance,
                            "follower": user_info["username"],
                            "follower_instance": host_check(user_info["host"]),
                        }
                    )

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            last_id = resp[-1]["id"]

            await asyncio.sleep(self._get_crawl_delay(host))

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

    async with MisskeyFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with MisskeyActiveUserCrawler(start_urls) as crawler:
        await crawler.launch()
