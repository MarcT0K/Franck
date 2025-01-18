import asyncio

from argparse import ArgumentParser

from .bookwyrm import launch_bookwyrm_crawl
from .friendica import launch_friendica_crawl
from .lemmy_crawler import launch_lemmy_crawl
from .mastodon_crawler import launch_mastodon_crawl
from .misskey_crawler import launch_misskey_crawl
from .peertube_crawler import launch_peertube_crawl
from .pleroma_crawler import launch_pleroma_crawl

SOFTWARE_LAUNCH = {
    "peertube": launch_peertube_crawl,
    "lemmy": launch_lemmy_crawl,
    "misskey": launch_misskey_crawl,
    "mastodon": launch_mastodon_crawl,
    "friendica": launch_friendica_crawl,
    "bookwyrm": launch_bookwyrm_crawl,
    "pleroma": launch_pleroma_crawl,
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
        choices=list(SOFTWARE_LAUNCH.keys()) + ["*"],
    )
    args = parser.parse_args()

    if args.subcommand == "crawl":
        if args.software == "*":
            for software, launch_function in SOFTWARE_LAUNCH.items():
                print("Start " + software)
                asyncio.run(launch_function())
        else:
            asyncio.run(SOFTWARE_LAUNCH[args.software]())
