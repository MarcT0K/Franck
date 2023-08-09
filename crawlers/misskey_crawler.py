"""Misskey graph crawler."""
import asyncio

from csv import DictWriter, DictReader

from common import Crawler, CrawlerException, fetch_fediverse_instance_list


class MisskeyTopUserCrawler(Crawler):
    SOFTWARE = "misskey"
    CRAWL_SUBJECT = "top_user"

    INSTANCES_CSV = "instances.csv"
    INSTANCES_FIELDS = ["instance", "users_count", "posts_count"]
    FOLLOWS_CSV = "follows.csv"
    FOLLOWS_FIELDS = ["Source", "Target", "Weight"]

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

    MAX_PAGE_SIZE = 100

    def __init__(self, first_urls, nb_top_users=100):
        super().__init__(first_urls, 1)

        self.nb_top_users = nb_top_users

        self.instances_file_lock = self.init_csv_file(
            self.INSTANCES_CSV, self.INSTANCES_FIELDS
        )
        self.follows_file_lock = self.init_csv_file(
            self.FOLLOWS_CSV, self.FOLLOWS_FIELDS
        )
        self.crawled_follows_lock = self.init_csv_file(
            self.CRAWLED_FOLLOWS_CSV, self.CRAWLED_FOLLOWS_FIELDS
        )
        self.crawled_users_lock = self.init_csv_file(
            self.CRAWLED_USERS_CSV, self.CRAWLED_USERS_FIELDS
        )

    async def inspect_instance(self, host):
        try:
            await self._fetch_instance_stats(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the stats of %s: %s", host, str(err)
            )
            return

        try:
            users = await self._crawl_user_list(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the user list of %s: %s", host, str(err)
            )
            return

        for user in users:
            try:
                await self._crawl_user_interactions(host, user)  # Followers
                await self._crawl_user_interactions(
                    host, user, followers_list=False
                )  # Following
            except CrawlerException as err:
                self.logger.debug(
                    "Error while crawling the interactions of %s of %s: %s",
                    user["id"],
                    host,
                    str(err),
                )

    async def _fetch_instance_stats(self, host):
        # https://misskey.io/api/stats
        stats_dict = await self._fetch_json(
            "https://" + host + "/api/stats", body={}, op="POST"
        )

        instance_dict = {
            "instance": host,
            "users_count": stats_dict["originalUsersCount"],
            "posts_count": stats_dict["originalNotesCount"],
        }

        async with self.instances_file_lock:
            with open(self.INSTANCES_CSV, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.INSTANCES_FIELDS)
                writer.writerow(instance_dict)

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

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            users.extend(resp)

            offset += self.MAX_PAGE_SIZE

        async with self.crawled_users_lock:
            with open(self.CRAWLED_USERS_CSV, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.CRAWLED_USERS_FIELDS)
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

    async def _crawl_user_interactions(self, host, user_info, followers_list=True):
        if followers_list:
            endpoint = "/api/users/followers"
        else:
            endpoint = "/api/users/following"

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
                "https://" + host + endpoint, body=body, op="POST"
            )

            host_check = lambda host_input: host if host_input is None else host_input

            if followers_list:
                new_follows = [
                    {
                        "follower": follow_dict["follower"]["username"],
                        "follower_instance": host_check(
                            follow_dict["follower"]["host"]
                        ),
                        "followee": user_info["username"],
                        "followee_instance": host_check(user_info["host"]),
                    }
                    for follow_dict in resp
                ]
            else:
                new_follows = [
                    {
                        "followee": follow_dict["followee"]["username"],
                        "followee_instance": host_check(
                            follow_dict["followee"]["host"]
                        ),
                        "follower": user_info["username"],
                        "follower_instance": host_check(user_info["host"]),
                    }
                    for follow_dict in resp
                ]

            follow_dicts.extend(new_follows)

            if len(resp) < self.MAX_PAGE_SIZE:
                break

            last_id = resp[-1]["id"]

        async with self.crawled_follows_lock:
            with open(self.CRAWLED_FOLLOWS_CSV, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.CRAWLED_FOLLOWS_FIELDS)
                for follow in follow_dicts:
                    writer.writerow(follow)

    def data_cleaning(self):
        # TODO
        ...


async def main():
    start_urls = await fetch_fediverse_instance_list("misskey")

    async with MisskeyTopUserCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
