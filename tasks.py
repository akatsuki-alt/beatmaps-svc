from common.database.objects import DBBeatmapset, DBBeatmapPack
from common.service import RepeatedTask
from common.app import database, ossapi
from common.logging import get_logger

from ossapi import *

import common.repos.beatmaps as beatmaps

import time

logger = get_logger("beatmaps_svc")

class UpdateBanchoMaps(RepeatedTask):
    
    def __init__(self) -> None:
        super().__init__("update_bancho_maps", 600)
        
    def run(self):
        added = 0
        with database.managed_session() as session:
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
    
class UpdateBeatmapPacks(RepeatedTask):
    
    def __init__(self) -> None:
        super().__init__("update_beatmap_packs", 2400)
        
    def run(self):
        added = 0
        with database.managed_session() as session:
            cursor = None
            while True:
                packs = ossapi.beatmap_packs(cursor_string=cursor)
                cursor = packs.cursor_string
                if not packs.beatmap_packs:
                    break
                for pack_compact in packs.beatmap_packs:
                    if session.get(DBBeatmapPack, pack_compact.tag):
                        cursor = None
                        break
                    pack = ossapi.beatmap_pack(pack_compact.tag)
                    sets_id = list()
                    for beatmapset in pack.beatmapsets:
                        if beatmaps.get_beatmapset(beatmapset.id, force_fetch=True):
                            sets_id.append(beatmapset.id)
                    added += 1
                    session.add(DBBeatmapPack(
                        author=pack.author,
                        date=pack.date,
                        name=pack.name,
                        link=pack.url,
                        tag=pack.tag,
                        no_diff_reduction=pack.no_diff_reduction,
                        beatmapsets=sets_id
                    ))
                    logger.debug(f"Added beatmap pack {pack.tag}.")
                    time.sleep(0.2)
                if not cursor:
                    break
            session.commit()
            logger.info(f"Added {added} beatmap packs.")