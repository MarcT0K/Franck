# Peertube graph crawler

This repository contains a Python script to crawl the Peertube network. The goal is to fetch information about the Peertube instances and their connexion graph.

I developed this codebase for research purposes but feel free to re-use it and submit issues if you foresee relevant improvements. The file `TODO.md` already contains some naive ideas (submit issues if you consider one of these ideas relevant to your application).

## About the implementation

This repository uses [aiohttp](https://docs.aiohttp.org/en/stable/index.html) and leverages asynchronous co-routines to optimize the crawl. For now, the Peertube network is small enough to make multi-processing unnecessary/overkill.

By default, the crawl starts from the instance list of <https://index.kraut.zone/api/v1/instances/hosts>, which is more exhaustive than the Peertube instance list.

## About the output

The crawl outputs raw and cleaned CSV files. The file `instances.csv` contains information about the instances (views, users, videos, etc.). If you would like additional information about the information, check the [API documentation](https://docs.joinpeertube.org/api-rest-reference.html#tag/Stats) and submit a PR/issue. The file `followers.csv` contains the follower links. It can be loaded on the graph visualizer [Gephi](https://gephi.org/). In `instances.csv`, I duplicated the column `host` in `Id` and `Label` to use this file as a node table in Gephi.

Along with these two files, the script provides `clean_instances.csv` and `clean_followers.csv`, which only includes the working instances.

## Getting started

There are only two dependencies that can be installed with the following command:

```bash
pip3 install -r requirements.txt
```

To start the crawl, you can use the following command:

```bash
python3 crawler.py
```
