"""Mastodon Graph Crawler"""

import asyncio
import json
import re

from csv import DictWriter, DictReader
from typing import Any, Dict, Optional, Tuple

import aiohttp

from .common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)

DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


class MastodonFederationCrawler(FederationCrawler):
    SOFTWARE = "mastodon"
    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "users",
        "statuses",
        "languages",
        "registration_enabled",
        "error",
        "Id",
        "Label",
    ]

    async def inspect_instance(self, host: str):
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        connected_instances = []
        # blocked_instances = []

        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v1/instance")
            instance_dict["version"] = info_dict["version"]
            instance_dict["users"] = info_dict["stats"]["user_count"]
            instance_dict["statuses"] = info_dict["stats"]["status_count"]
            instance_dict["languages"] = "/".join(info_dict["languages"])
            instance_dict["registration_enabled"] = info_dict["registrations"]

            connected_instances = await self._fetch_json(
                "http://" + host + "/api/v1/instance/peers"
            )
            # blocked_instances = await self._fetch_json(
            #     "http://" + host + "/api/v1/instance/domain_blocks"
            # ) # Not always publicly available => removed for consistency
        except CrawlerException as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict)
        await self._write_connected_instance(host, connected_instances)


class MastodonActiveUserCrawler(Crawler):
    SOFTWARE = "mastodon"
    CRAWL_SUBJECT = "active_user"

    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "users",
        "statuses",
        "languages",
        "registration_enabled",
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
    ]

    TEMP_FILES = [CRAWLED_FOLLOWS_CSV, CRAWLED_USERS_CSV]

    MAX_PAGE_SIZE = 80
    MAX_ID_REGEX = r"max_id=(\d+)"

    def __init__(self, urls, nb_active_users=10000):
        super().__init__(urls)

        self.nb_active_users = nb_active_users

        self.csv_information = [
            (self.INSTANCES_CSV, self.INSTANCES_CSV_FIELDS),
            (self.INTERACTIONS_CSVS[0], self.INTERACTIONS_CSV_FIELDS),
            (self.CRAWLED_FOLLOWS_CSV, self.CRAWLED_FOLLOWS_FIELDS),
            (self.CRAWLED_USERS_CSV, self.CRAWLED_USERS_FIELDS),
        ]

    async def _fetch_json_with_pagination(
        self, url: str, params=None
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """Query an instance API and returns the resulting JSON along with the next page URL

        Args:
            url (str): URL of the API endpoint
        Raises:
            CrawlerException: if the HTTP request fails.

        Returns:
            Dict: dictionary containing the JSON response.
        """
        if params is None:
            params = {}
        if "limit" not in params:
            params["limit"] = self.MAX_PAGE_SIZE

        self.logger.debug("Fetching (with pagination) %s [params:%s]", url, str(params))

        next_max_id = None
        async with self.concurrent_connection_sem:
            try:
                async with self.session.get(url, timeout=180, params=params) as resp:
                    if resp.status != 200:
                        raise CrawlerException(
                            f"Error code {str(resp.status)} on {url}"
                        )
                    data = await resp.read()
                    if "Link" in resp.headers and "next" in resp.headers["Link"]:
                        next_link = resp.headers["Link"].split(",")[
                            0
                        ]  # Extract the next page link
                        max_id_regex = re.search(self.MAX_ID_REGEX, next_link)
                        next_max_id = max_id_regex.group(1)
                    try:
                        return json.loads(data), next_max_id
                    except (json.JSONDecodeError, UnicodeDecodeError) as err:
                        raise CrawlerException(
                            f"Cannot decode JSON on {url} ({err})"
                        ) from err
            except aiohttp.ClientError as err:
                raise CrawlerException(f"{err}") from err
            except asyncio.TimeoutError as err:
                raise CrawlerException(f"Connection to {url} timed out") from err
            except ValueError as err:
                if err.args[0] == "Can redirect only to http or https":
                    raise CrawlerException("Invalid redirect") from err
                raise

    async def inspect_instance(self, host):
        try:
            await self._fetch_instance_info(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the information of %s: %s", host, str(err)
            )
            return

        try:
            users = await self._crawl_user_list(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the user list of %s: %s", host, str(err)
            )
            return

        for ind, user in enumerate(users):
            self.logger.debug(
                "Instance %s: %d users out of %d crawled", host, ind + 1, len(users)
            )
            try:
                if user["following_count"] > 0:
                    await self._crawl_user_interactions(host, user)
            except CrawlerException as err:
                self.logger.debug(
                    "Error while crawling the interactions of %s of %s: %s",
                    user["id"],
                    host,
                    str(err),
                )

    async def _fetch_instance_info(self, host):
        instance_dict = {"host": host}
        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v1/instance")
            instance_dict["version"] = info_dict["version"]
            instance_dict["users"] = info_dict["stats"]["user_count"]
            instance_dict["statuses"] = info_dict["stats"]["status_count"]
            instance_dict["languages"] = "/".join(info_dict["languages"])
            instance_dict["registration_enabled"] = info_dict["registrations"]
        except Exception as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict)

    async def _crawl_user_list(self, host):
        users = []
        offset = 0

        while len(users) < self.nb_active_users:
            nb_missing_users = self.nb_active_users - len(users)
            params = {
                "limit": min(self.MAX_PAGE_SIZE, nb_missing_users),
                "local": "true",
                "order": "active",
                "offset": offset,
            }
            resp = await self._fetch_json(
                "https://" + host + "/api/v1/directory", params=params
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
                        "followers_count": user["followers_count"],
                        "following_count": user["following_count"],
                        "posts_count": user["statuses_count"],
                    }
                )

        return users

    async def _crawl_user_interactions(self, host, user_info):
        follow_dicts = {}

        max_id = None
        while True:
            params = {"max_id": max_id} if max_id is not None else None
            resp, new_max_id = await self._fetch_json_with_pagination(
                f"https://{host}/api/v1/accounts/{user_info['id']}/following",
                params=params,
            )

            for followee_dict in resp:
                # NB: Sometimes, the API was returning some duplicates (idk why...)
                #   Using a dictionary instead of a list avoid these duplicates
                followee_instance = (
                    followee_dict["acct"].split("@")[1]
                    if "@" in followee_dict["acct"]
                    else host
                )

                if (
                    followee_instance in self.crawled_instances
                ):  # Avoid adding useless follows that will be cleaned later
                    follow_dicts[followee_dict["username"]] = {
                        "followee": followee_dict["username"],
                        "followee_instance": followee_instance,
                        "follower": user_info["username"],
                        "follower_instance": host,
                    }

            # if len(follow_dicts) > user_info["following_count"]: # Had problems when the users were following/unfollowing during the crawl
            #     raise ValueError(
            #         "Found %s followees instead of %s for user %s",
            #         len(follow_dicts),
            #         user_info["following_count"],
            #         user_info["username"],
            #         list(follow_dicts.keys()),
            #     )

            if new_max_id is None:
                if (
                    len(resp) == 9
                    and max_id is None
                    and user_info["following_count"] != 0
                ):
                    # NB: we need these "complicated" conditions instead of just checking the size of follow_dicts because we filter some follows
                    self.logger.debug(
                        "User %s@%s set its follower list as private.",
                        user_info["username"],
                        host,
                    )
                break

            max_id = new_max_id
            await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

        lock, _file, writer = self.csvs[self.CRAWLED_FOLLOWS_CSV]
        async with lock:
            for follow in follow_dicts.values():
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


async def launch_mastodon_crawl():
    start_urls = await fetch_fediverse_instance_list("mastodon")
    # start_urls = ["mastodon.social", "mastodon.acm.org"]  # FOR DEBUG

    async with MastodonFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with MastodonActiveUserCrawler(start_urls) as crawler:
        await crawler.launch()
