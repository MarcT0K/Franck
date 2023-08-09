"Lemmy graph crawlers"

import asyncio
from csv import DictWriter, DictReader
import urllib.parse

import scipy.sparse as sp

from common import (
    Crawler,
    CrawlerException,
    FederationCrawler,
    fetch_fediverse_instance_list,
)


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

    INTERACTIONS_CSV = "interactions.csv"
    INTERACTIONS_FIELDS = [
        "user_instance",
        "community",
        "community_instance",
        "username",
        "post_id",
    ]

    def __init__(
        self, first_urls, activity_scope="TopWeek", min_active_user_per_community=5
    ):
        super().__init__(first_urls, 1)
        if activity_scope not in ("TopDay", "TopWeek", "TopMonth"):
            raise CrawlerException("Invalid activity window.")
        self.activity_scope = activity_scope
        self.min_active_user_per_community = min_active_user_per_community

        self.interation_file_lock = Crawler.init_csv_file(
            self.INTERACTIONS_CSV, self.INTERACTIONS_FIELDS
        )
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

    async def crawl_community_list(self, host):
        local_communities = []

        page = 1
        crawl_over = False
        while not crawl_over:
            params = {
                "page": page,
                "limit": 50,
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

            async with self.community_own_lock:
                with open(
                    self.COMMUNITY_OWNERSHIP_CSV, "a", encoding="utf-8"
                ) as csv_file:
                    writer = DictWriter(
                        csv_file, fieldnames=self.COMMUNITY_OWNERSHIP_FIELDS
                    )
                    for community_dict in new_communities:
                        writer.writerow(community_dict)

            if len(resp["communities"]) < 50:
                break

            page += 1
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

                new_posts.append(current)

            async with self.interation_file_lock:
                with open(self.INTERACTIONS_CSV, "a", encoding="utf-8") as csv_file:
                    writer = DictWriter(csv_file, fieldnames=self.INTERACTIONS_FIELDS)
                    for post_dict in new_posts:
                        writer.writerow(post_dict)

            total_posts += len(resp["posts"])

            if len(resp["posts"]) < 50:
                break

            page += 1

    def data_cleaning(self):
        # NB: No concurrent tasks so we do not need the locks
        community_list = []
        community_dict = {}

        instance_list = []
        instance_dict = {}

        with open(self.COMMUNITY_OWNERSHIP_CSV, "r", encoding="utf-8") as csv_file:
            reader = DictReader(csv_file, fieldnames=self.COMMUNITY_OWNERSHIP_FIELDS)
            next(reader, None)
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

        with open(self.INTERACTIONS_CSV, "r", encoding="utf-8") as csv_file:
            reader = DictReader(csv_file, fieldnames=self.INTERACTIONS_FIELDS)
            next(reader, None)
            for post_dict in reader:
                instance = post_dict["user_instance"]
                community = post_dict["community"]
                try:
                    user_instance_ind = instance_dict[instance]
                    community_ind = community_dict[community][0]
                    interaction_mat[user_instance_ind, community_ind] += 1
                except KeyError:
                    self.logger.debug(
                        "Ignoring post %s: instance unknown %s",
                        post_dict["post_id"],
                        instance,
                    )

        intra_instance_mat = sp.coo_matrix(interaction_mat @ ownership_mat)
        bool_interaction_mat = (interaction_mat != 0).astype(int)
        cross_instance_mat = sp.coo_matrix(
            bool_interaction_mat @ bool_interaction_mat.T
        )
        # NB: convert to COO format to iterate over non-zero elements

        community_act_mat = interaction_mat.sum(axis=0)
        assert community_act_mat.shape == (1, len(community_list))

        # Write the community activity CSV
        with open(self.COMMUNITY_ACTIVITY_CSV, "a", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=self.COMMUNITY_ACTIVITY_FIELDS)
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
            (self.CROSS_INSTANCE_CSV, cross_instance_mat),
            (self.INTRA_INSTANCE_CSV, intra_instance_mat),
        ]:
            with open(csv_name, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.CSV_FIELDS)
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


async def main():
    start_urls = await fetch_fediverse_instance_list("lemmy")

    async with LemmyFederationCrawler(start_urls) as crawler:
        await crawler.launch()

    async with LemmyCommunityCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
