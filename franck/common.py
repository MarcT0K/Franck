"Base crawler classes"

import asyncio
from io import TextIOWrapper
import json
import logging
import os

from abc import abstractmethod
from csv import DictReader, DictWriter
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from urllib import robotparser

import aiohttp
import colorlog
import requests

from aiohttp_retry import RetryClient, ListRetry
from langdetect import detect_langs
from tqdm.asyncio import tqdm

import franck

DEFAULT_DELAY_BETWEEN_CONSECUTIVE_REQUESTS = 0.4


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
    USER_AGENT = (
        "Franck, the Fediverse Graph Crawler (https://github.com/MarcT0K/Franck)"
    )
    SOFTWARE: Optional[str] = None
    CRAWL_SUBJECT: Optional[str] = None
    NB_SEMAPHORE: int = 100
    LANGUAGE_DETECTION_THRESHOLD = 0.7

    DELAY_PER_HOST: Dict[str, float] = {}

    API_ENDPOINTS: List[str] = []

    INTERACTIONS_CSVS = ["interactions.csv"]
    # NB: some crawlers can produce multiple graphs
    #   but all graphs have the same format.
    INTERACTIONS_CSV_FIELDS = ["Source", "Target", "Weight"]  # DO NOT OVERWRITE

    INSTANCES_CSV = "instances.csv"
    INSTANCES_CSV_FIELDS: List[str] = [
        "host",
        "error",
        "Id",
        "Label",
    ]  # CAN BE EXTENDED

    TEMP_FILES = []

    def __init__(
        self,
        urls: List[str],
    ):
        assert self.INTERACTIONS_CSV_FIELDS == [
            "Source",
            "Target",
            "Weight",
        ]  # Avoid regression in the child classes
        for field in [
            "host",
            "error",
            "Id",
            "Label",
        ]:
            assert field in self.INSTANCES_CSV_FIELDS

        assert self.SOFTWARE is not None
        assert self.CRAWL_SUBJECT is not None

        # Create the result folder
        self.result_dir = (
            self.SOFTWARE
            + "_"
            + self.CRAWL_SUBJECT
            + "_"
            + datetime.now().strftime("%Y%m%d-%H%M%S")
        )
        os.mkdir(self.result_dir)

        # Version file
        with open(
            self.result_dir + "/version.txt", "w", encoding="utf-8"
        ) as version_file:
            version_file.write(franck.__version__)

        # Load balacing
        self.concurrent_connection_sem = asyncio.Semaphore(self.NB_SEMAPHORE)

        # CSV locks
        self.csvs: Dict[str, Tuple[asyncio.Lock, TextIOWrapper, DictWriter]] = {}
        self.csv_information: List[Tuple[str, List[str]]] = []

        # Initialize HTTP session
        aiohttp_session = aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT})
        retry_options = ListRetry(
            timeouts=[30, 60, 180, 300, 600],
            statuses={429},
            retry_all_server_errors=True,
        )
        self.session = RetryClient(
            client_session=aiohttp_session, retry_options=retry_options
        )

        self.crawled_instances = set(urls)

        # Setup the logger
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

    async def _parse_robots_txt(self, host) -> Tuple[str, bool]:
        if not self.API_ENDPOINTS:
            raise ValueError("Invalid crawler: no API endpoint listed")

        robots_url = f"https://{host}/robots.txt"
        async with self.concurrent_connection_sem:
            try:
                async with self.session.get(robots_url, timeout=180) as resp:
                    if resp.status == 404:
                        self.logger.debug("Instance %s has no robots.txt", host)
                        return (host, True)
                    elif resp.status == 200:
                        self.logger.debug("Instance %s: found the robots.txt", host)
                        data = await resp.read()
                    else:
                        self.logger.warning(
                            "Unknown error while fetching the robots.txt of Instance %s (code %d)",
                            host,
                            resp.status,
                        )
                        return (host, False)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                self.logger.warning(
                    "Unknown error while fetching the robots.txt of Instance %s: %s",
                    host,
                    str(err),
                )
                return (host, False)

        parser = robotparser.RobotFileParser()
        parser.parse(data.decode().splitlines())

        for url in self.API_ENDPOINTS:
            if not parser.can_fetch(self.USER_AGENT, url) or not parser.can_fetch(
                self.USER_AGENT, url + "/"
            ):
                self.logger.debug("robots.txt of %s disallows the crawl", host)
                return (host, False)

        req_rate = parser.request_rate(self.USER_AGENT)
        if req_rate is not None:
            delay = req_rate.requests / req_rate.seconds
            if delay > 60:
                self.logger.warning(
                    "Crawl delay is too high on Instance %s (delay: %f)",
                    host,
                    delay,
                )
                return (host, False)

            self.DELAY_PER_HOST[host] = delay

        crawl_delay = parser.crawl_delay(self.USER_AGENT)
        if crawl_delay is not None:
            try:
                if float(crawl_delay) > 60:
                    self.logger.warning(
                        "Crawl delay is too high on Instance %s (delay: %f)",
                        host,
                        crawl_delay,
                    )
                    return (host, False)

                self.DELAY_PER_HOST[host] = float(crawl_delay)
            except ValueError:
                pass  # Invalid format

        if host not in self.DELAY_PER_HOST:
            self.DELAY_PER_HOST[host] = DEFAULT_DELAY_BETWEEN_CONSECUTIVE_REQUESTS

        return (host, True)

    def _detect_language(self, text: str) -> str:
        detected_languages = detect_langs(text)
        if detected_languages[0].prob > self.LANGUAGE_DETECTION_THRESHOLD:
            return detected_languages[0].lang
        else:
            return "Unknown"

    def _get_crawl_delay(self, host):
        """Returns the crawl delay for a specific host."""
        try:
            return self.DELAY_PER_HOST[host]
        except KeyError as err:
            raise CrawlerException(
                f"The robots.txt of {host} has not been parsed."
            ) from err

    def _init_csv_file(self, filename, fields):
        csv_file = open(filename, "w", encoding="utf-8")
        writer = DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        csv_file.flush()
        self.csvs[filename] = (asyncio.Lock(), csv_file, writer)

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

    def data_postprocessing(self):
        pass

    def data_cleaning(self):
        assert self.SOFTWARE is not None
        assert self.CRAWL_SUBJECT is not None

        # Remove temporary files
        log_file = "crawl_" + self.SOFTWARE + "_" + self.CRAWL_SUBJECT + ".log"
        os.rename(log_file, log_file + ".to_remove")

        for file in self.TEMP_FILES:
            os.rename(file, file + ".to_remove")

        # Remove the unreachable instances
        working_instances = set()
        with open(self.INSTANCES_CSV, encoding="utf-8") as rawfile, open(
            "clean_" + self.INSTANCES_CSV, "w", encoding="utf-8"
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

        os.rename(self.INSTANCES_CSV, self.INSTANCES_CSV + ".old.to_remove")
        os.rename("clean_" + self.INSTANCES_CSV, self.INSTANCES_CSV)

        for interaction_file in self.INTERACTIONS_CSVS:
            with open(interaction_file, encoding="utf-8") as rawfile, open(
                "clean_" + interaction_file, "w", encoding="utf-8"
            ) as cleanfile:
                data = DictReader(rawfile)
                writer = DictWriter(cleanfile, fieldnames=self.INTERACTIONS_CSV_FIELDS)
                writer.writeheader()
                for row in data:
                    if (
                        row["Source"] in working_instances
                        and row["Target"] in working_instances
                    ):
                        writer.writerow(row)

            os.rename(interaction_file, interaction_file + ".old.to_remove")
            os.rename("clean_" + interaction_file, interaction_file)

    async def __inspect_instance_with_logging(self, url):
        self.logger.debug("Start inspecting instance %s", url)
        await self.inspect_instance(url)
        self.logger.debug("Finished inspecting instance %s", url)

    async def launch(self):
        """Launch the crawl"""

        os.chdir(self.result_dir)
        self.init_all_files()

        if not self.crawled_instances:
            raise CrawlerException("No URL to crawl")

        tasks = [self._parse_robots_txt(url) for url in self.crawled_instances]

        robots_results = await tqdm.gather(
            *tasks,
            desc=f"Analysing  the robots.txt for {self.SOFTWARE} ({self.CRAWL_SUBJECT})",
        )

        allowed_crawled_instances = []
        for host, accepted in robots_results:
            if accepted:
                allowed_crawled_instances.append(host)
            else:
                err_msg = "Crawling disallowed or problem while analysing robots.txt (see the logs)"
                await self._write_instance_csv({"host": host, "error": err_msg})

        self.crawled_instances = allowed_crawled_instances

        self.logger.info("Crawl begins...")

        try:
            tasks = [
                self.__inspect_instance_with_logging(url)
                for url in self.crawled_instances
            ]

            for task in tqdm.as_completed(
                tasks, desc=f"Crawling {self.SOFTWARE} ({self.CRAWL_SUBJECT})"
            ):
                await task
            for _lock, csv_file, _writer in self.csvs.values():
                csv_file.flush()

            self.logger.info("Crawl completed!!!")
            self.logger.info("Processing the data...")

            self.data_postprocessing()
            self.data_cleaning()

        except Exception as err:
            err_msg = str(err)
            self.logger.error("Crawl failed: %s", err_msg)
            os.chdir("../")
            raise err
        self.logger.info("Done.")
        os.chdir("../")

    async def close(self):
        await self.session.close()
        for _lock, file, _writer in self.csvs.values():
            file.close()

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
                        try:
                            err_data = await resp.read()
                        except aiohttp.ClientResponseError:
                            err_data = "Cannot read the response data"
                        err_msg = f"Error code {str(resp.status)} on {url}"
                        self.logger.error(err_msg)
                        self.logger.debug("Error response: %s", err_data)
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

    async def _write_instance_csv(self, instance_dict):
        lock, _file, writer = self.csvs[self.INSTANCES_CSV]
        async with lock:
            writer.writerow(instance_dict)

    async def _write_connected_instance(
        self,
        host: str,
        connected_instances: List[str],
        blocked_instances: Optional[List[str]] = None,
    ):
        assert len(self.INTERACTIONS_CSVS) == 1
        lock, _file, writer = self.csvs[self.INTERACTIONS_CSVS[0]]
        async with lock:
            for dest in set(connected_instances):
                if dest in self.crawled_instances:  # Minimizes the cleaning necessary
                    writer.writerow({"Source": host, "Target": dest, "Weight": 1})

            if blocked_instances is not None:
                for dest in set(blocked_instances):
                    if (
                        dest in self.crawled_instances
                    ):  # Minimizes the cleaning necessary
                        writer.writerow({"Source": host, "Target": dest, "Weight": -1})


class FederationCrawler(Crawler):
    """Abstract class for crawler exploring a federation of instances.

    This crawler works for Fediverse projects where instances are explicitly
    interconnected (e.g., Lemmy or Peertube). Hence, this graph depends on
    instance configuration and not on the user activity.
    """

    CRAWL_SUBJECT = "federation"

    def __init__(self, urls: List[str]):
        super().__init__(urls)
        assert len(self.INTERACTIONS_CSVS) == 1
        self.csv_information = [
            (self.INSTANCES_CSV, self.INSTANCES_CSV_FIELDS),
            (self.INTERACTIONS_CSVS[0], self.INTERACTIONS_CSV_FIELDS),
        ]

    @abstractmethod
    async def inspect_instance(self, host: str):
        raise NotImplementedError
