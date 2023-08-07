# Copyright (C) 2023  Marc "TOK_" Damie
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio

from .common import FederationCrawler


class PeertubeCrawler(FederationCrawler):
    SOFTWARE = "peertube"
    INSTANCE_CSV_FIELDS = [
        "host",
        "totalUsers",
        "totalDailyActiveUsers",
        "totalWeeklyActiveUsers",
        "totalMonthlyActiveUsers",
        "totalLocalVideos",
        "totalVideos",
        "totalInstanceFollowers",
        "totalPeertubeInstanceFollowers",
        "totalInstanceFollowing",
        "totalPeertubeInstanceFollowing",
        "totalLocalPlaylists",
        "totalVideoComments",
        "totalLocalVideoComments",
        "totalLocalVideoViews",
        "error",
        "Id",
        "Label",
    ]


async def main():
    async with PeertubeCrawler() as crawler:
        await crawler.fetch_instance_list()
        await crawler.launch()


if __name__ == "__main__":
    asyncio.run(main())
