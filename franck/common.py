"Base crawler classes"

import asyncio
import glob
import json
import logging
import os
from abc import abstractmethod
from csv import DictReader, DictWriter
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import aiohttp
import colorlog
import pandas as pd
import requests

from aiohttp_retry import RetryClient, ExponentialRetry
from tqdm.asyncio import tqdm


class CrawlerException(Exception):
    """Base exception class for the crawlers"""

    def __init__(self, err):
        self.msg = err

    def __str__(self):
        return self.msg


async def fetch_fediverse_instance_list(software):
    # GraphQL query
    body = '''{nodes(softwarename:"''' + software + """" status: "UP"){domain}}"""

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.fediverse.observer", json={"query": body}, timeout=300
            )
            data = await resp.read()
            data = json.loads(data)
    except json.decoder.JSONDecodeError:  # Sometimes, Cloudflare blocks aiohttp
        resp = requests.post(
            "https://api.fediverse.observer", json={"query": body}, timeout=300
        )
        data = resp.json()
    return [instance["domain"] for instance in data["data"]["nodes"]]


class Crawler:
    SOFTWARE = None
    CRAWL_SUBJECT = None

    def __init__(
        self,
        urls: List[str],
    ):
        # Create the result folder
        self.result_dir = (
            self.SOFTWARE
            + "_"
            + self.CRAWL_SUBJECT
            + "_"
            + datetime.now().strftime("%Y%m%d-%H%M%S")
        )
        os.mkdir(self.result_dir)

        # Load balacing
        self.concurrent_connection_sem = asyncio.Semaphore(1000)

        # CSV locks
        self.csv_locks: Dict[str, asyncio.Lock] = {}
        self.csv_information: List[Tuple[str, List[str]]] = []

        # Initialize HTTP session
        aiohttp_session = aiohttp.ClientSession(
            headers={"User-Agent": "Fediverse Graph Crawler (Academic Research)"}
        )
        retry_options = ExponentialRetry(attempts=3)
        self.session = RetryClient(
            client_session=aiohttp_session, retry_options=retry_options
        )

        self.urls = urls

        self._setup_logger()

    def _setup_logger(self):
        self.logger = colorlog.getLogger(self.SOFTWARE + "_" + self.CRAWL_SUBJECT)
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers = []  # Reset handlers
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s[%(asctime)s %(levelname)s]%(reset)s %(white)s%(message)s",
                datefmt="%H:%M:%S",
                reset=True,
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red",
                },
            )
        )
        handler.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        fhandler = logging.FileHandler(
            self.result_dir
            + "/crawl_"
            + self.SOFTWARE
            + "_"
            + self.CRAWL_SUBJECT
            + ".log"
        )
        fhandler.setFormatter(
            logging.Formatter("[%(asctime)s %(levelname)s] %(message)s")
        )
        fhandler.setLevel(logging.DEBUG)
        self.logger.addHandler(fhandler)

    def _init_csv_file(self, filename, fields):
        with open(filename, "w", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=fields)
            writer.writeheader()
        self.csv_locks[filename] = asyncio.Lock()

    def init_all_files(self):
        for filename, fields in self.csv_information:
            self._init_csv_file(filename, fields)

    @abstractmethod
    async def inspect_instance(self, host: str):
        """Fetches the instance information and the list of connected instances.

        Args:
            host (str): Hostname of the instance
        """
        raise NotImplementedError

    def post_round(self):
        """Various post-round operations."""

    @abstractmethod
    def data_cleaning(self): ...

    async def __inspect_instance_with_logging(self, url):
        self.logger.info("Start inspecting instance %s", url)
        await self.inspect_instance(url)
        self.logger.info("Finished inspecting instance %s", url)

    async def launch(self):
        """Launch the crawl"""

        os.chdir(self.result_dir)
        self.init_all_files()

        if not self.urls:
            raise CrawlerException("No URL to crawl")

        self.logger.info("Crawl begins...")
        tasks = [self.__inspect_instance_with_logging(url) for url in self.urls]

        for task in tqdm.as_completed(tasks):
            await task

        self.logger.info("Crawl completed!!!")
        self.logger.info("Cleaning the data...")
        self.data_cleaning()
        Crawler.compress_csv_files()
        self.logger.info("Done.")

        os.chdir("../")

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.session.close()

    async def _fetch_json(
        self,
        url: str,
        params: Optional[Mapping[str, Union[str, int]]] = None,
        body=None,
        op="GET",
    ) -> Dict[str, Any]:
        """Query an instance API and returns the resulting JSON.

        Args:
            url (str): URL of the API endpoint
            params (Optional[Mapping[str, str]], optional): parameters of the HTTP query. Defaults to None.

        Raises:
            CrawlerException: if the HTTP request fails.

        Returns:
            Dict: dictionary containing the JSON response.
        """
        self.logger.debug(
            "Fetching %s [params:%s] [body:%s]", url, str(params), str(body)
        )

        async with self.concurrent_connection_sem:
            try:
                if op == "GET":
                    req_func = self.session.get
                elif op == "POST":
                    req_func = self.session.post
                else:
                    raise NotImplementedError

                async with req_func(url, timeout=180, params=params, json=body) as resp:
                    if resp.status != 200:
                        err_msg = f"Error code {str(resp.status)} on {url}"
                        self.logger.error(err_msg)
                        raise CrawlerException(err_msg)
                    data = await resp.read()
                    try:
                        return json.loads(data)
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

    @staticmethod
    def compress_csv_files():
        for fname in glob.glob("*.csv"):
            dataframe = pd.read_csv(fname)
            dataframe.to_parquet(fname[:-4] + ".parquet")


class FederationCrawler(Crawler):
    """Abstract class for crawler exploring a federation of instances.

    This crawler works for Fediverse projects where instances are explicitly
    interconnected (e.g., Lemmy or Peertube). Hence, this graph depends on
    instance configuration and not on the user activity.
    """

    CRAWL_SUBJECT = "federation"
    INSTANCES_FILENAME = "instances.csv"
    FOLLOWERS_FILENAME = "followers.csv"
    FOLLOWERS_CSV_FIELDS = ["Source", "Target", "Weight"]
    INSTANCES_CSV_FIELDS: List[str] = ["host", "error", "Id", "Label"]

    def __init__(self, urls: List[str]):
        super().__init__(urls)

        self.csv_information = [
            (self.INSTANCES_FILENAME, self.INSTANCES_CSV_FIELDS),
            (self.FOLLOWERS_FILENAME, self.FOLLOWERS_CSV_FIELDS),
        ]

    @abstractmethod
    async def inspect_instance(self, host: str):
        raise NotImplementedError

    async def _write_instance_csv(self, instance_dict):
        async with self.csv_locks[self.INSTANCES_FILENAME]:
            with open(self.INSTANCES_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.INSTANCES_CSV_FIELDS)
                writer.writerow(instance_dict)

    async def _write_linked_instance(
        self,
        host: str,
        linked_instances: List[str],
        blocked_instances: Optional[List[str]] = None,
    ):
        async with self.csv_locks[self.FOLLOWERS_FILENAME]:
            with open(self.FOLLOWERS_FILENAME, "a", encoding="utf-8") as csv_file:
                writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
                for dest in set(linked_instances):
                    writer.writerow({"Source": host, "Target": dest, "Weight": 1})

                if blocked_instances is not None:
                    for dest in set(blocked_instances):
                        writer.writerow({"Source": host, "Target": dest, "Weight": -1})

    def data_cleaning(self):
        """Clean the final result file."""
        working_instances = set()
        with open(self.INSTANCES_FILENAME, encoding="utf-8") as rawfile, open(
            "clean_" + self.INSTANCES_FILENAME, "w", encoding="utf-8"
        ) as cleanfile:
            data = DictReader(rawfile)
            assert self.INSTANCES_CSV_FIELDS is not None
            writer = DictWriter(cleanfile, fieldnames=self.INSTANCES_CSV_FIELDS)
            writer.writeheader()
            for row in data:
                if row["error"] == "":
                    working_instances.add(row["host"])
                    row["Id"] = row["host"]
                    row["Label"] = row["host"]
                    writer.writerow(row)

        with open(self.FOLLOWERS_FILENAME, encoding="utf-8") as rawfile, open(
            "clean_" + self.FOLLOWERS_FILENAME, "w", encoding="utf-8"
        ) as cleanfile:
            data = DictReader(rawfile)
            writer = DictWriter(cleanfile, fieldnames=self.FOLLOWERS_CSV_FIELDS)
            writer.writeheader()
            for row in data:
                if (
                    row["Source"] in working_instances
                    and row["Target"] in working_instances
                ):
                    writer.writerow(row)
