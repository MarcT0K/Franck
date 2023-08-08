"Lemmy graph crawlers"

import asyncio
import json
from csv import DictWriter

import aiohttp

from common import Crawler, CrawlerException, FederationCrawler


async def fetch_lemmy_instance_list():
    # GraphQL query
    body = """{
        nodes(softwarename:"lemmy", status:"UP") {
            domain
        }
        }"""

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "https://api.fediverse.observer", json={"query": body}, timeout=300
        )
        data = json.loads(await resp.read())
    return [instance["domain"] for instance in data["data"]["nodes"]]


class LemmyFederationCrawler(FederationCrawler):
    SOFTWARE = "lemmy"
    INSTANCES_CSV_FIELDS = [
        "host",
        "version",
        "users",
        "posts",
        "comments",
        "communities",
        "users_active_day",
        "users_active_week",
        "users_active_month",
        "users_active_half_year",
        "captcha_enabled",
        "require_email_verification",
        "error",
        "Id",
        "Label",
    ]

    async def inspect_instance(self, host: str):
        instance_dict = {"host": host}
        linked_instances = []
        blocked_instances = []

        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v3/site")
            instance_dict.update(
                {
                    key: val
                    for key, val in info_dict["site_view"]["counts"].items()
                    if key in self.INSTANCE_CSV_FIELDS
                }
            )

            instance_dict["version"] = info_dict["version"]

            if "local_site" not in info_dict["site_view"]:  # For older API versions
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["site"].items()
                        if key in self.INSTANCE_CSV_FIELDS
                    }
                )
            else:
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["local_site"].items()
                        if key in self.INSTANCE_CSV_FIELDS
                    }
                )

            if (
                info_dict["site_view"].get("local_site")
                and not info_dict["site_view"]["local_site"]["federation_enabled"]
            ):
                # NB: By default the older API have the federation actived by default
                raise CrawlerException("Federation disabled")

            if "federated_instances" in info_dict.keys():
                # Once again, the API evolved between the versions
                if info_dict["federated_instances"]["linked"] is not None:
                    linked_instances = info_dict["federated_instances"]["linked"]
                if info_dict["federated_instances"]["blocked"] is not None:
                    blocked_instances = info_dict["federated_instances"]["blocked"]
            else:
                if info_dict["site_view"]["local_site"]["federation_enabled"]:
                    instances_resp = await self._fetch_json(
                        "http://" + host + "/api/v3/federated_instances",
                    )

                    federated_instances = instances_resp["federated_instances"]
                    linked_instances = [
                        instance["domain"]
                        for instance in federated_instances["linked"]
                        if instance.get("software") == "lemmy"
                    ]
                    blocked_instances = [
                        instance["domain"]
                        for instance in federated_instances["blocked"]
                        if instance.get("software") == "lemmy"
                    ]
        except CrawlerException as err:
            instance_dict["error"] = str(err)

        async with self.info_csv_lock:
            with open(self.INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.INSTANCE_CSV_FIELDS)
                writer.writerow(instance_dict)

        async with self.link_csv_lock:
            with open(self.FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
                for dest in linked_instances:
                    writer.writerow({"Source": host, "Target": dest, "Weight": 1})

                for dest in blocked_instances:
                    writer.writerow({"Source": host, "Target": dest, "Weight": -1})


class LemmyCommunityCrawler(Crawler):
    SOFTWARE = "lemmy"
    CRAWL_SUBJECT = "community"

    COMMUNITY_ACTIVITY_CSV = "community_activity.csv"
    COMMUNITY_ACTIVITY_FIELDS = ["instance", "community", "number_posts"]
    COMMUNITY_OWNERSHIP_CSV = "community_ownership.csv"
    COMMUNITY_OWNERSHIP_FIELDS = [
        "instance",
        "community",
        "subscribers",
        "posts",
        "comments",
        "users_active_day",
        "users_active_week",
        "users_active_month",
    ]
    CROSS_INSTANCE_CSV = "cross_instance_interactions.csv"
    INTRA_INSTANCE_CSV = "intra_instance_interactions.csv"
    CSV_FIELDS = ["Source", "Target", "Weight"]

    TEMP_CSV = "temporary.csv"
    TEMP_FIELDS = ["user_instance", "community", "community_instance"]

    # TODO: Tout ecrire dans un CSV temporaire puis le charger ligne par ligne pour contruire des creuses
    # Ce système de fichier simplifiera le truc si on veut passer en multi proc => On crawl naivement puis on aggrège

    def __init__(
        self, first_urls, activity_scope="TopWeek", min_active_user_per_community=10
    ):
        super().__init__(first_urls, 1)
        if activity_scope not in ("TopDay", "TopWeek", "TopMonth"):
            raise CrawlerException("Invalid activity window.")
        self.activity_scope = activity_scope
        self.min_active_user_per_community = min_active_user_per_community

        self.temp_file_lock = Crawler.init_csv_file(self.TEMP_CSV, self.TEMP_FIELDS)
        self.intra_inst_lock = Crawler.init_csv_file(
            self.INTRA_INSTANCE_CSV, self.CSV_FIELDS
        )
        self.cross_inst_lock = Crawler.init_csv_file(
            self.CROSS_INSTANCE_CSV, self.CSV_FIELDS
        )
        self.community_act_lock = Crawler.init_csv_file(
            self.COMMUNITY_ACTIVITY_CSV, self.COMMUNITY_ACTIVITY_FIELDS
        )
        self.community_own_lock = Crawler.init_csv_file(
            self.COMMUNITY_OWNERSHIP_CSV, self.COMMUNITY_OWNERSHIP_FIELDS
        )

    async def inspect_instance(self, host):
        try:
            communities = await self.crawl_community_list(host)
        except CrawlerException as err:
            print(f"Error while crawling the community list of {host}: {str(err)}")

        for community in communities:
            try:
                await self.crawl_community_posts(host, community)
            except CrawlerException as err:
                print(
                    f"Error while crawling the posts of {community} from {host}: {str(err)}"
                )

    async def crawl_community_list(self, host):
        local_communities = []

        page = 1
        while True:
            params = {
                "page": page,
                "limit": 50,
                "type_": "Local",
                "sort": self.activity_scope,
            }
            resp = await self._fetch_json(
                "http://" + host + "/api/v3/community/list", params=params
            )

            new_communities = []
            for community in resp["communities"]:
                current_community = {
                    key: val
                    for key, val in community["counts"].items()
                    if key in self.COMMUNITY_OWNERSHIP_FIELDS
                }
                current_community["instance"] = host
                current_community["community"] = community["community"]["name"]

                new_communities.append(current_community)

                if (
                    (
                        self.activity_scope == "TopDay"
                        and current_community["users_active_day"]
                        > self.min_active_user_per_community
                    )
                    or (
                        self.activity_scope == "TopWeek"
                        and current_community["users_active_week"]
                        > self.min_active_user_per_community
                    )
                    or (
                        self.activity_scope == "TopMonth"
                        and current_community["users_active_month"]
                        > self.min_active_user_per_community
                    )
                ):
                    break

                if len(resp["communities"]) < 50:
                    break

            async with self.community_own_lock:
                with open(
                    self.COMMUNITY_OWNERSHIP_CSV, "a", encoding="utf-8"
                ) as csv_file:
                    writer = DictWriter(
                        csv_file, fieldnames=self.COMMUNITY_OWNERSHIP_FIELDS
                    )
                    for community_dict in new_communities:
                        writer.writerow(community_dict)

            page += 1
        return local_communities

    async def crawl_community_posts(self, host, community):
        ...

    def data_cleaning(self):
        ...


async def main():
    start_urls = await fetch_lemmy_instance_list()

    async with LemmyFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with LemmyCommunityCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
