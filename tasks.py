from common.database.objects import DBBeatmapset
from common.service import RepeatedTask
from common.app import database, ossapi
from common.logging import get_logger

from ossapi import *

import common.repos.beatmaps as beatmaps

logger = get_logger("beatmaps_svc")

class UpdateBanchoMaps(RepeatedTask):
    
    def __init__(self) -> None:
        super().__init__("update_bancho_maps", 600)
        
    def run(self):
        added = 0
        with database.session as session:
            cursor = None
            while True:
                sets = ossapi.search_beatmapsets(
                    category=BeatmapsetSearchCategory.HAS_LEADERBOARD,
                    explicit_content=BeatmapsetSearchExplicitContent.SHOW,
                    sort=BeatmapsetSearchSort.RANKED_DESCENDING,
                    cursor=cursor
                )
                cursor = sets.cursor
                if not sets.beatmapsets:
                    break
                for set in sets.beatmapsets:
                    if session.get(DBBeatmapset, set.id):
                        cursor = None
                        break
                    try:
                        print(beatmaps.get_beatmapset(set.id).artist_unicode)
                        added += 1
                    except:
                        logger.warn(f"Failed to get beatmapset {set.id}!")
                if not cursor:
                    break
            logger.info(f"Added {added} beatmapsets.")
        return True