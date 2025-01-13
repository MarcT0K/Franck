from .common import Crawler
from .friendica import FriendicaFederationCrawler
from .lemmy_crawler import LemmyCommunityCrawler, LemmyFederationCrawler
from .mastodon_crawler import MastodonFederationCrawler
from .misskey_crawler import MisskeyTopUserCrawler
from .peertube_crawler import PeertubeCrawler
from .pleroma_crawler import PleromaActiveUserCrawler, PleromaFederationCrawler

__version__ = "0.0.1"
__license__ = "GPLv3"
