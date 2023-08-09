"""Misskey graph crawler."""

from common import Crawler, CrawlerException


async def fetch_misskey_instance_list():
    ...


class MisskeyUserCrawler(Crawler):
    def __init__():
        # TODO
        ...

    async def inspect_instance(self, host):
        # TODO
        ...

    async def data_cleaning(self):
        # TODO
        ...


async def main():
    start_urls = await fetch_misskey_instance_list()

    async with MisskeyUserCrawler(start_urls) as crawler:
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
