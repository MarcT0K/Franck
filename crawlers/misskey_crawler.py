"""Misskey graph crawler."""

from common import Crawler, CrawlerException, fetch_fediverse_instance_list


class MisskeyTopUserCrawler(Crawler):
    def __init__(self, start_urls, nb_top_users=100):
        # TODO
        ...

    async def inspect_instance(self, host):
        # TODO
        ...

    async def crawl_user_list(self, host):
        # https://misskey.io/api/users
        ...

    async def crawl_user_interactions(self, host, username, follower_list=True):
        # https://misskey.io/api-doc#tag/users/operation/users/followers
        # https://misskey.io/api-doc#tag/users/operation/users/followers
        ...

    async def data_cleaning(self):
        # TODO
        ...


async def main():
    start_urls = await fetch_fediverse_instance_list("misskey")

    async with MisskeyTopUserCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
