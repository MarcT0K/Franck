## About the output

`franck` uses [aiohttp](https://docs.aiohttp.org/en/stable/index.html) and leverages asynchronous co-routines to optimize the crawl.

## Crawling process

Our crawler starts by fetching an instance list on [Fediverse Observer](https://fediverse.observer).
Then, our crawler queries each of the instances one by one.
During the crawl, some raw CSV files are produced: they contain all the instances and interactions that we have fetched.
At the end of a crawl, these CSV are reanalyzed to produce clean output files only including the relevant instances and interactions.

Along with each CSV "graph" file, we provide an CSV "instance" file listing all the instances with interesting information about them; e.g., number of users.
The available information varies for each software.

## Available graphs

Each graph represents the interactions between servers running the same software.
For example, the Peertube graph only represents interactions between Peertube servers.
While Peertube and Mastodon servers interact, our graphs do not represent this interaction for simplicity.
Focusing on a software is important to produce coherent graphs.

Graphs covering multiple systems are challenging because they would mix entities of different nature.
Thus, building a complete graph of the Fediverse is an interesting future work, but it is not our priority.

### "Federation" graphs

The "Federation" graphs is provided for Bookwyrm, Friendica, Lemmy, Mastodon, Misskey, and Peertube.

Most Fediverse software provide an API endpoint such as `api/instance/peers` listing all the ActivityPub servers to which an instance is connected.
The federation graph represent these connections.
It is then an undirected graph with all edges having a weight of 1.
Some software such as Peertube also provide a list of blocked servers, so we added edges with a weight -1 for them.

This graph is quite to obtain.
The graph is quite dense because it does not distinguish instances with a lot of interactions from instances with fewer interactions.
Thus, this graph should also observe fewer temporal evolutions than others.


### Lemmy community graphs

`franck` produces two graphs based on the community activity.
A "community" on Lemmy is a discussion topic hosted by a Lemmy instance.
While a community is "owned" by a single instance, users from any connected instance can publish on it.
A community lists many discussions and each discussion contains comments.

First, in the "intra-instance" graph, Server A interacts with server B if a user from A publishes a discussion in a community from Server B.
Second, in the "inter-instance" graph, Sever Ainteracts with server B if users from A and B publishes a discussion in the same community.
In this second graph, the community can be owned by a Server C different from A and B.

To reduce the network traffic, we model our interaction dynamics only based on discussion publication, and not on the comments.
Basing the interactions on the comments would require to crawl much more information... possibly, overloading the Lemmy servers.

Finally, our graphs only analyze the discussions published during the last month.
On the one hand, this focus only reduces the network traffic.
On the other hand, this monthly focus provides highly evolving graphs.
Such graphs accurately represent the interaction in the evolutions compared to other graphs such as the Federation graphs.

Thus, Lemmy community graphs are valuable to obtain graphs with a realistic temporal evolution.

### Mastodon "Active Users" graphs


The Misskey "Top Users" crawler extracts the 10K most recently active users from each instance and analyse their following lists.
In this graph, Server A interacts with Server B if a recently active user from server A follows a user from Server B.
The graph is directed and the weight of each weight is equal to the total number of follows from A to B.

We rely on the recently active users to avoid querying all the Mastodon users.
This technique significantly reduces the network traffic while providing a coherent estimation of the interactions.

### Misskey "Top Users" graphs

The Misskey "Top Users" crawler extracts the 1K most followed users from each instance and analyse their follower lists.
In this graph, Server A interacts with Server B if a user from server A follows one of the most followed users from Server B.
The graph is directed and the weight of each weight is equal to the total number of follows from A to B.

We rely on the most followed users to avoid querying all the Misskey users.
This technique significantly reduces the network traffic while providing a coherent estimation of the interactions.

## Adding new graphs?

Ideally, we would like to cover all social media of the Fediverse.
However, some Fediverse software provide fewer API endpoints than others.
For example, PixelFed does not even provide an `api/instance/peers` endpoint.
Similarly, some software require authentication to access some endpoints.

We respect the will of these servers, and rely only on Public APIS.
Hence, some Fediverse social media are not yet support by `franck`.
Currently, Fediverse developers focused (legitimately) their efforts on endpoints useful to developers.
Our research could be an incentive for Fediverse developpers to provide more API endpoints; facilitating research on the Fediverse.

If you identify a new Fediverse social media compatible with our approach, **do not hesitate to create an issue or submit a pull request**.
The file `common.py` provides base classes from which crawler can be implemented.
To implement a new crawler, you can take inspiration from existing crawlers using these classes.
These classes already handling all the querying, parallelization, load balancing, and logging.
Implementing a new crawler essentially require to find the approriate endpoints and to extract the valuable information from their responses.
