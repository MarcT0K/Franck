# Franck, the Fediverse graph crawler

This repository contains a Python script to crawl various social media from the Fediverse. Our tool represent the interactions dynamics between the Fediverse servers. These dynamics are represented using different graphs.

Currently, `franck` is able to crawl Bookwyrm, Mastodon, Friendica, Misskey, Peertube, Pleroma/Akkoma, and Lemmy.
Our tool **only queries public APIs.**
Moreover, our implementation includes some **load balancing** (e.g., maximum 5 queries per second and per Fediverse instance).
Our goal is to crawl the Fediverse while respecting its philosophy and minimizing our impact.

Our crawling approach is targeted to minimize the number of queries necessary.
As much as possible, we rely on aggregated information provided by the servers.
When such aggregated information is not available, we sample information (e.g., analyze the most active users or the most recently active threads).
This sampling significantly reduces the number of queries and is sufficient to represent the Fediverse dynamics.

Refer to our research paper for a formal presentation of each graph (and of the whole research project): [TODO] 

## Graph dataset: Fedivertex

We built a graph dataset using our crawlers: https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset .
This Kaggle page presents more thoroughly the dataset and how to interact with it.
Our graphs contain only aggregated information about the servers (i.e., **reveal no personal information about the users**).

## Research purpose

We are researchers in privacy-preserving machine learning (ML), a field that has recently attracted significant attention, particularly in decentralized ML protocols. Currently, the scientific literature relies on historical graph datasets, such as social media user interactions, which fail to accurately represent the interactions between servers in decentralized ML systems. To address this gap, our goal is to create a novel graph dataset that captures the interactions between servers hosting decentralized social networks—specifically, the Fediverse.

Beyond advancing ML research, this dataset could have broader benefits for the Fediverse itself. At present, the Fediverse is often overlooked as a valuable domain for ML applications. By increasing its visibility, our work could spur the development of useful ML systems, such as federated spam detection or content recommendation tools.

Although our primary focus is privacy-preserving ML, our graph dataset may also find unexpected applications in other fields, such as sociology. That said, given our scientific background, we cannot fully assess its potential impact in these areas.

## Getting started

To install our tool, you need to execute the following command:

```bash
pip3 install -e .
```

To start the crawl, you can use the following command:

```bash
franck crawl [all|mastodon|peertube|lemmy|pleroma|friendica|bookwyrm|misskey]
```

## ⚠️⚠️ WARNING ⚠️⚠️

Even if we developed some load balancing to avoid getting flagged as a DDoS attack by the Fediverse servers, the network traffic induced by `franck` can still be significantly high, especially for Mastodon crawling.
Thus, you should deploy the crawler in a controlled environment and discuss with your ISP appropriate deployment conditions.

Furthermore, crawling the Web usually raise some privacy concerns.
Even though our crawler only queries public APIs, we have prepared dedicated GDPR declarations before deploying our own crawlers to be fully compliant.
We recommend considering GDPR (or any relevant data privacy regulation) if you plan to deploy `franck`.
