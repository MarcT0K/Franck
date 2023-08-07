"Lemmy graph crawlers"

import asyncio
import json

from csv import DictWriter

from common import FederationCrawler, CrawlerException


class LemmyFederationCrawler(FederationCrawler):
    SOFTWARE = "lemmy"
    INSTANCE_CSV_FIELDS = [
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

    async def fetch_instance_list(self, url: str = "https://api.fediverse.observer"):
        # GraphQL query
        body = """{
            nodes(softwarename:"lemmy", status:"UP") {
                domain
            }
            }"""
        resp = await self.session.post(url, json={"query": body}, timeout=300)
        data = json.loads(await resp.read())
        instances = [instance["domain"] for instance in data["data"]["nodes"]]
        self.urls = instances

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


async def main():
    async with LemmyFederationCrawler() as crawler:
        await crawler.fetch_instance_list()
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
