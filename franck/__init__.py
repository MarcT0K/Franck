from importlib.metadata import version

from .common import Crawler
from .friendica import FriendicaFederationCrawler
from .lemmy_crawler import LemmyCommunityCrawler, LemmyFederationCrawler
from .mastodon_crawler import MastodonFederationCrawler
from .misskey_crawler import MisskeyActiveUserCrawler, MisskeyFederationCrawler
from .peertube_crawler import PeertubeCrawler
from .pleroma_crawler import PleromaActiveUserCrawler, PleromaFederationCrawler

__version__ = version("franck")
__license__ = "GPLv3"
