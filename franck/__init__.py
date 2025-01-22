from .common import Crawler
from .friendica import FriendicaFederationCrawler
from .lemmy_crawler import LemmyCommunityCrawler, LemmyFederationCrawler
from .mastodon_crawler import MastodonFederationCrawler
from .misskey_crawler import MisskeyActiveUserCrawler, MisskeyFederationCrawler
from .peertube_crawler import PeertubeCrawler
from .pleroma_crawler import PleromaActiveUserCrawler, PleromaFederationCrawler

from pkg_resources import get_distribution

__version__ = get_distribution("franck").version
__license__ = "GPLv3"
