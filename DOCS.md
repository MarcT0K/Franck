## About the output

`franck` uses [aiohttp](https://docs.aiohttp.org/en/stable/index.html) and leverages asynchronous co-routines to optimize the crawl.

## Crawling process

Our crawler starts by fetching an instance list on [Fediverse Observer](https://fediverse.observer).
Then, our crawler queries each of the instances one by one.
During the crawl, some raw CSV files are produced: they contain all the instances and interactions that we have fetched.
At the end of a crawl, these CSV are reanalyzed to produce clean output files only including the relevant instances and interactions.

## Available graphs

Each graph represents the interactions between servers running the same software.
For example, the Peertube graph only represents interactions between Peertube servers.
While Peertube and Mastodon servers interact, our graphs do not represent this interaction for simplicity.
Focusing on a software is important to produce coherent graphs.

Graphs covering multiple systems are challenging because they would mix entities of different nature.
Thus, building a complete graph of the Fediverse is an interesting future work, but it is not our priority.

### "Federation" graphs

Bookwyrm, Friendica, Lemmy, Mastodon, Misskey, Peertube

### Lemmy community graphs

### Mastodon "Active Users" graphs

### Misskey "Top Users" graphs

