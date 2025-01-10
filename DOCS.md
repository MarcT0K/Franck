# Documentation

## About the implementation

This repository uses [aiohttp](https://docs.aiohttp.org/en/stable/index.html) and leverages asynchronous co-routines to optimize the crawl. For now, we perform focused crawl, so the task is small enough to make multi-processing unnecessary/overkill.

## Crawling initialization

For each software, the crawler inspects the instances listed on https://fediverse.observer.

## About the output

### Peertube

The crawl outputs raw and cleaned CSV files. The file `instances.csv` contains information about the instances (views, users, videos, etc.). If you would like additional information about the instances, check the [API documentation](https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats) and submit a PR/issue. The file `followers.csv` contains the follower links. It can be loaded on the graph visualizer [Gephi](https://gephi.org/). In `instances.csv`, I duplicated the column `host` in `Id` and `Label` to use this file as a node table in Gephi.

Along with these two files, the script provides `clean_instances.csv` and `clean_followers.csv`, which only includes the working instances.