"Base crawler classes"

import asyncio
import json
import os

from abc import abstractmethod
from csv import DictWriter, DictReader
from datetime import datetime
from typing import Optional, List, Mapping, Dict, Any

import aiohttp
from tqdm.asyncio import tqdm


class CrawlerException(Exception):
    """Base exception class for the crawlers"""

    def __init__(self, err):
        self.msg = err

    def __str__(self):
        return self.msg


class FederationCrawler:
    """Abstract class for crawler exploring a federation of instances.

    This crawler works for Fediverse projects where instances are explicitly
    interconnected (e.g., Lemmy or Peertube). Hence, this graph depends on
    instance configuration and not on the user activity.
    """

    SOFTWARE = NotImplementedError
    INSTANCES_FILENAME = "instances.csv"
    FOLLOWERS_FILENAME = "followers.csv"
    FOLLOWERS_CSV_FIELDS = ["Source", "Target", "Weight"]
    INSTANCE_CSV_FIELDS: Optional[List[str]] = None

    def __init__(
        self,
        first_urls: List[str] = None,
        crawl_depth: int = -1,
    ):
        # Create the result folder
        result_dir = self.SOFTWARE + "_" + datetime.now().strftime("%Y%m%d-%H%M%S")
        os.mkdir(result_dir)
        os.chdir(result_dir)

        self.info_csv_lock = asyncio.Lock()
        self.link_csv_lock = asyncio.Lock()

        # Initialize the result CSV files
        with open(self.INSTANCES_FILENAME, "w", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=self.INSTANCE_CSV_FIELDS)
            writer.writeheader()

        with open(self.FOLLOWERS_FILENAME, "w", encoding="utf-8") as csv_file:
            writer = DictWriter(csv_file, fieldnames=self.FOLLOWERS_CSV_FIELDS)
            writer.writeheader()

        # Initialize the HTTP session
        self.session = aiohttp.ClientSession()
        self.max_crawl_depth = crawl_depth

        self.urls = []
        if first_urls is not None:
            assert isinstance(first_urls, list)
            self.urls = first_urls

    async def _fetch_json(
        self, url: str, params: Optional[Mapping[str, str]] = None
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
        try:
            async with self.session.get(url, timeout=300, params=params) as resp:
                if resp.status != 200:
                    raise CrawlerException(f"Error code {str(resp.status)} on {url}")
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

    @abstractmethod
    async def fetch_instance_list(self, url: str):
        """Fetch a list of instances provided by a public database.

        Args:
            url (str): URL of the public database
        """
        raise NotImplementedError

    @abstractmethod
    async def inspect_instance(self, host: str):
        """Fetches the instance information and the list of connected instances.

        Args:
            host (str): Hostname of the instance
        """
        raise NotImplementedError

    def check_unknown_urls_in_csv(self) -> List[str]:
        """Extract the unexplored instances from the CSV files.

        Returns:
            List[str]: a list of instance hostnames
        """
        crawled = set()
        with open(self.INSTANCES_FILENAME, encoding="utf-8") as csvfile:
            data = DictReader(csvfile)
            for row in data:
                crawled.add(row["host"])

        from_links = set()
        with open(self.FOLLOWERS_FILENAME, encoding="utf-8") as csvfile:
            data = DictReader(csvfile)
            for row in data:
                from_links.add(row["Source"])
                from_links.add(row["Target"])

        return list(from_links - crawled)

    @abstractmethod
    def post_round_cleaning(self):
        """Clean the CSV files after a crawling round."""
        raise NotImplementedError

    def data_cleaning(self):
        """Clean the final result file."""
        working_instances = set()
        with open(self.INSTANCES_FILENAME, encoding="utf-8") as rawfile, open(
            "clean_" + self.INSTANCES_FILENAME, "w", encoding="utf-8"
        ) as cleanfile:
            data = DictReader(rawfile)
            writer = DictWriter(cleanfile, fieldnames=self.INSTANCE_CSV_FIELDS)
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

    async def launch(self):
        """Launch the crawl"""

        if not self.urls:
            raise CrawlerException("No URL to crawl")

        crawl_done = False
        current_depth = 1

        print("Crawl begins...")
        while not crawl_done:
            print("Crawling round ", current_depth)
            tasks = [self.inspect_instance(url) for url in self.urls]

            for task in tqdm.as_completed(tasks):
                await task

            self.post_round_cleaning()
            self.urls = self.check_unknown_urls_in_csv()

            if len(self.urls) == 0 or current_depth == self.max_crawl_depth:
                crawl_done = True
            current_depth += 1
        print("Crawl completed!!!")
        print("Cleaning the data...")
        self.data_cleaning()
        print("Done.")

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.session.close()