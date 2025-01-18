"Lemmy graph crawlers"

import asyncio

from csv import DictWriter, DictReader
import urllib.parse

import scipy.sparse as sp

from .common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)

DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.2


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
        assert self.INSTANCES_CSV_FIELDS is not None
        instance_dict = {"host": host}
        connected_instances = []
        blocked_instances = []

        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v3/site")
            instance_dict.update(
                {
                    key: val
                    for key, val in info_dict["site_view"]["counts"].items()
                    if key in self.INSTANCES_CSV_FIELDS
                }
            )

            instance_dict["version"] = info_dict["version"]

            if "local_site" not in info_dict["site_view"]:  # For older API versions
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["site"].items()
                        if key in self.INSTANCES_CSV_FIELDS
                    }
                )
            else:
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["local_site"].items()
                        if key in self.INSTANCES_CSV_FIELDS
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
                    connected_instances = info_dict["federated_instances"]["linked"]
                if info_dict["federated_instances"]["blocked"] is not None:
                    blocked_instances = info_dict["federated_instances"]["blocked"]
            else:
                if info_dict["site_view"]["local_site"]["federation_enabled"]:
                    instances_resp = await self._fetch_json(
                        "http://" + host + "/api/v3/federated_instances",
                    )

                    federated_instances = instances_resp["federated_instances"]
                    connected_instances = [
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

        await self._write_instance_csv(instance_dict)
        await self._write_connected_instance(
            host, connected_instances, blocked_instances
        )


class LemmyCommunityCrawler(Crawler):
    SOFTWARE = "lemmy"
    CRAWL_SUBJECT = "community"

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

    CROSS_INSTANCE_INTERACTIONS_CSV = "cross_instance_interactions.csv"
    INTRA_INSTANCE_INTERACTIONS_CSV = "intra_instance_interactions.csv"
    INTERACTIONS_CSVS = [
        CROSS_INSTANCE_INTERACTIONS_CSV,
        INTRA_INSTANCE_INTERACTIONS_CSV,
    ]

    DETAILED_INTERACTIONS_CSV = "detailed_interactions.csv"
    DETAILED_INTERACTIONS_FIELDS = [
        "user_instance",
        "community",
        "community_instance",
        "username",
        "post_id",
    ]

    TEMP_FILES = [DETAILED_INTERACTIONS_CSV]
    MAX_PAGE_SIZE = 50

    def __init__(
        self, urls, activity_scope="TopMonth", min_active_user_per_community=5
    ):
        super().__init__(urls)
        if activity_scope not in ("TopDay", "TopWeek", "TopMonth"):
            raise CrawlerException("Invalid activity window.")
        self.activity_scope = activity_scope
        self.min_active_user_per_community = min_active_user_per_community

        self.csv_information = [
            (self.INSTANCES_CSV, self.INSTANCES_CSV_FIELDS),
            (self.DETAILED_INTERACTIONS_CSV, self.DETAILED_INTERACTIONS_FIELDS),
            (self.INTRA_INSTANCE_INTERACTIONS_CSV, self.INTERACTIONS_CSV_FIELDS),
            (self.CROSS_INSTANCE_INTERACTIONS_CSV, self.INTERACTIONS_CSV_FIELDS),
            (self.COMMUNITY_ACTIVITY_CSV, self.COMMUNITY_ACTIVITY_FIELDS),
            (self.COMMUNITY_OWNERSHIP_CSV, self.COMMUNITY_OWNERSHIP_FIELDS),
        ]

    async def inspect_instance(self, host):
        await self._inspect_instance_info(host)

        communities = []
        try:
            communities = await self.crawl_community_list(host)
        except CrawlerException as err:
            self.logger.debug(
                "Error while crawling the community list of %s: %s", host, str(err)
            )
            return

        for community in communities:
            try:
                await self.crawl_community_posts(host, community)
            except CrawlerException as err:
                self.logger.debug(
                    "Error while crawling the posts of %s from %s: %s",
                    community,
                    host,
                    str(err),
                )
                return

    async def _inspect_instance_info(self, host: str):
        instance_dict = {"host": host}

        try:
            info_dict = await self._fetch_json("http://" + host + "/api/v3/site")
            instance_dict.update(
                {
                    key: val
                    for key, val in info_dict["site_view"]["counts"].items()
                    if key in self.INSTANCES_CSV_FIELDS
                }
            )

            instance_dict["version"] = info_dict["version"]

            if "local_site" not in info_dict["site_view"]:  # For older API versions
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["site"].items()
                        if key in self.INSTANCES_CSV_FIELDS
                    }
                )
            else:
                instance_dict.update(
                    {
                        key: val
                        for key, val in info_dict["site_view"]["local_site"].items()
                        if key in self.INSTANCES_CSV_FIELDS
                    }
                )

            if (
                info_dict["site_view"].get("local_site")
                and not info_dict["site_view"]["local_site"]["federation_enabled"]
            ):
                # NB: By default the older API have the federation actived by default
                raise CrawlerException("Federation disabled")
        except CrawlerException as err:
            instance_dict["error"] = str(err)

        await self._write_instance_csv(instance_dict)

    async def crawl_community_list(self, host):
        local_communities = []

        page = 1
        crawl_over = False
        while not crawl_over:
            params = {
                "page": page,
                "limit": self.MAX_PAGE_SIZE,
                "type_": "Local",
                "sort": self.activity_scope,
            }
            resp = await self._fetch_json(
                "http://" + host + "/api/v3/community/list", params=params
            )

            if not resp["communities"]:
                break

            new_communities = []
            for community in resp["communities"]:
                current_community = {
                    key: val
                    for key, val in community["counts"].items()
                    if key in self.COMMUNITY_OWNERSHIP_FIELDS
                }
                current_community["instance"] = host
                current_community["community"] = community["community"]["name"]

                if (
                    (
                        self.activity_scope == "TopDay"
                        and current_community["users_active_day"]
                        < self.min_active_user_per_community
                    )
                    or (
                        self.activity_scope == "TopWeek"
                        and current_community["users_active_week"]
                        < self.min_active_user_per_community
                    )
                    or (
                        self.activity_scope == "TopMonth"
                        and current_community["users_active_month"]
                        < self.min_active_user_per_community
                    )
                ):
                    crawl_over = True
                    break

                new_communities.append(current_community)
                local_communities.append(community["community"]["name"])

            lock, _file, writer = self.csvs[self.COMMUNITY_OWNERSHIP_CSV]
            async with lock:
                for community_dict in new_communities:
                    writer.writerow(community_dict)

            if len(resp["communities"]) < self.MAX_PAGE_SIZE:
                break

            page += 1
            await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)
        return local_communities

    async def crawl_community_posts(self, host, community):
        page = 1
        total_posts = 0

        crawl_over = False
        while not crawl_over:
            params = {
                "page": page,
                "limit": 50,
                "sort": self.activity_scope,
                "community_name": community,
            }
            resp = await self._fetch_json(
                "http://" + host + "/api/v3/post/list", params=params
            )

            if not resp["posts"]:
                break

            new_posts = []
            for post in resp["posts"]:
                current = {
                    "community": community + "@" + host,
                    "community_instance": host,
                }
                current["user_instance"] = urllib.parse.urlparse(
                    post["creator"]["actor_id"]
                ).netloc
                current["username"] = post["creator"]["name"]
                current["post_id"] = post["post"]["ap_id"]

                if current["user_instance"] in self.crawled_instances:
                    new_posts.append(current)

            lock, _file, writer = self.csvs[self.DETAILED_INTERACTIONS_CSV]
            async with lock:
                for post_dict in new_posts:
                    writer.writerow(post_dict)

            total_posts += len(resp["posts"])

            if len(resp["posts"]) < 50:
                break

            page += 1
            await asyncio.sleep(DELAY_BETWEEN_CONSECUTIVE_REQUESTS)

    def data_postprocessing(self):
        # NB: No concurrent tasks so we do not need the locks
        community_list = []
        community_dict = {}

        instance_list = []
        instance_dict = {}

        with open(self.COMMUNITY_OWNERSHIP_CSV, "r", encoding="utf-8") as csv_file:
            reader = DictReader(csv_file, fieldnames=self.COMMUNITY_OWNERSHIP_FIELDS)
            next(reader, None)  # Skip the csv header
            for row in reader:
                instance_ind = -1
                if row["instance"] in instance_dict:
                    instance_ind = instance_dict[row["instance"]]
                else:
                    instance_ind = len(instance_list)
                    instance_dict[row["instance"]] = instance_ind
                    instance_list.append(row["instance"])

                community_ind = len(community_list)
                community_full_name = row["community"] + "@" + row["instance"]
                community_dict[community_full_name] = (community_ind, instance_ind)
                community_list.append(community_full_name)

        ownership_mat = sp.dok_matrix(
            (len(community_list), len(instance_list)), dtype=int
        )
        for community_ind, instance_ind in community_dict.values():
            ownership_mat[community_ind, instance_ind] = 1

        interaction_mat = sp.dok_matrix(
            (len(instance_list), len(community_list)), dtype=int
        )

        with open(self.DETAILED_INTERACTIONS_CSV, "r", encoding="utf-8") as csv_file:
            reader = DictReader(csv_file, fieldnames=self.DETAILED_INTERACTIONS_FIELDS)
            next(reader, None)  # Skip the CSV header
            for post_dict in reader:
                instance = post_dict["user_instance"]
                community = post_dict["community"]
                try:
                    user_instance_ind = instance_dict[instance]
                except KeyError:
                    self.logger.debug(
                        "Ignoring post %s: instance unknown %s (community %s)",
                        post_dict["post_id"],
                        instance,
                        community,
                    )
                else:
                    community_ind = community_dict[community][0]
                    interaction_mat[user_instance_ind, community_ind] += 1

        intra_instance_mat = sp.coo_matrix(interaction_mat @ ownership_mat)
        bool_interaction_mat = (interaction_mat != 0).astype(int)
        cross_instance_mat = sp.coo_matrix(
            bool_interaction_mat @ bool_interaction_mat.T
        )
        # NB: convert to COO format to iterate over non-zero elements

        community_act_mat = interaction_mat.sum(axis=0)
        assert community_act_mat.shape == (1, len(community_list))

        # Write the community activity CSV
        _lock, _file, writer = self.csvs[self.COMMUNITY_ACTIVITY_CSV]
        for community_ind, nb_posts in enumerate(community_act_mat.tolist()[0]):
            community = community_list[community_ind]
            instance_ind = community_dict[community][1]
            writer.writerow(
                {
                    "instance": instance_list[instance_ind],
                    "community": community,
                    "number_posts": nb_posts,
                }
            )

        # Write the two CSV storing possible weighted graphs between the active instances
        for csv_name, sp_mat in [
            (self.CROSS_INSTANCE_INTERACTIONS_CSV, cross_instance_mat),
            (self.INTRA_INSTANCE_INTERACTIONS_CSV, intra_instance_mat),
        ]:
            _lock, _file, writer = self.csvs[csv_name]
            for src_inst_ind, dest_inst_ind, weight in zip(
                sp_mat.row, sp_mat.col, sp_mat.data
            ):
                writer.writerow(
                    {
                        "Source": instance_list[src_inst_ind],
                        "Target": instance_list[dest_inst_ind],
                        "Weight": weight,
                    }
                )


async def launch_lemmy_crawl():
    start_urls = await fetch_fediverse_instance_list("lemmy")

    async with LemmyFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with LemmyCommunityCrawler(start_urls) as crawler:
        await crawler.launch()
