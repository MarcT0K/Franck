# Franck, the Fediverse graph crawler

This repository contains a Python script to crawl various social media from the Fediverse. Our tool represent the dynamics existing between the Fediverse servers. These dynamics are represented using different graphs.

Currently, `franck` is able to crawl Bookwyrm, Mastodon, Friendica, Misskey, Peertube, and Lemmy.
Our tool **only queries public APIs.**
Moreover, our implementation includes some load balancing (e.g., maximum 5 queries per second and per Fediverse instance).
Our goal is to crawl the Fediverse while respecting its philosophy and minimizing our impact.

As detailed in [DOCS.md](DOCS.md), our crawling approach is targeted to minimize the number of queries necessary.
As much as possible, we rely on aggregated information provided by the servers.
When such aggregated information is not available, we sample information (e.g., analyze the most active users or the most recently active threads).
This sampling significantly reduces the number of queries and is sufficient to represent the Fediverse dynamics.

## Research purpose

We are researchers in privacy-preserving Machine Learning (ML).
This scientific literature has attracted a lot of attention lately, especially decentralized ML protocols.
Currently, the scientific literature relies on historical graph datasets (e.g., interactions between users on a social media).
Unfortunately, these graphs do not represent realistic interactions between servers involved in decentralized ML.
Our goal is to build a novel graph dataset representing the interactions between the servers hosting decentralized social networks; i.e., the "Fediverse."

Moreover, we will repeat the crawling procedure during several months to observe the temporal evolution.
Existing graphs datasets do not incorporate a temporal evolution, while such evolutions can have major impact in decentralized ML.
Thus, our dataset provides the first graph dataset with a realistic temporal evolution.

Finally, producing a dataset about the Fediverse could have positive consequences on the Fediverse.
Currently, the Fediverse is not seen as a valuable application area for ML research.
Our dataset could bring some visibility to the Fediverse and some valuable ML systems may emerge (e.g., federated spam detection or content recommendation).

While our research focus on privacy-preserving ML, our graphs could also find other unforeseen applications in other research fields such as sociology.
However, our scientific background does not allow us to assess their relevance in such research field.

## Fediverse graphs

Our graphs represent interactions between the servers.
If Server A and Server B interact, they have an edge connecting them on the graph.
Some graphs are weighted, each edge has a value defining how strong the interaction is.
For each software, we have a different definition of the interaction.
For example, an interaction can be simply that the servers are connected (like on Peertube) or  an interaction can be a user from Server A following a user from server B (like on Mastodon).
[DOCS.md](DOCS.md) details all the graphs built by `franck`.

Our graphs contain only aggregated information about the servers.
Thus, our graphs **reveal no personal information about the users**.

We output CSV and parquet files.
The format of the CSV files storing the graphs is compatible with [Gephi](https://gephi.org/) (a graph visualization software).
Thus, our results can directly be visualized after the crawl.

## Getting started

To install our tool, you need to execute the following command:

```bash
pip3 install -e .
```

To start the crawl, you can use the following command:

```bash
franck crawl [mastodon|peertube|lemmy|friendica|bookwyrm|misskey]
```
