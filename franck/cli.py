import asyncio

from argparse import ArgumentParser

from .friendica import launch_friendica_crawl
from .lemmy_crawler import launch_lemmy_crawl
from .mastodon_crawler import launch_mastodon_crawl
from .misskey_crawler import launch_misskey_crawl
from .peertube_crawler import launch_peertube_crawl

SOFTWARE_LAUNCH = {
    "peertube": launch_peertube_crawl,
    "lemmy": launch_lemmy_crawl,
    "misskey": launch_misskey_crawl,
    "mastodon": launch_mastodon_crawl,
    "friendica": launch_friendica_crawl,
}


def main():
    parser = ArgumentParser(
        description="Franck crawls the Fediverse to provide various graphs useful for researchers."
    )
    subparsers = parser.add_subparsers(dest="subcommand")
    subparsers.required = True
    crawl_parser = subparsers.add_parser("crawl")
    crawl_parser.add_argument(
        "software",
        help="Fediverse software subject of the crawl",
        choices=["peertube", "lemmy", "misskey", "mastodon", "friendica"],
    )
    args = parser.parse_args()

    if args.subcommand == "crawl":
        asyncio.run(SOFTWARE_LAUNCH[args.software]())
