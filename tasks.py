from typing import List, Tuple
from common.database.objects import DBBeatmapset, DBBeatmapPack, DBBeatmap, DBTask
from sqlalchemy.orm.attributes import flag_modified

from common.service import RepeatedTask
from common.app import database, ossapi
from common.logging import get_logger

from datetime import datetime, timedelta
from sqlalchemy import Integer

from ossapi import *

import beatmaps.selfbot as selfbot
import common.repos.beatmaps as beatmaps
import time

from beatmaps.selfbot import Embed

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
                        added += 1
                    except:
                        logger.warn(f"Failed to get beatmapset {set.id}!")
                if not cursor:
                    break
            logger.info(f"Added {added} beatmapsets.")
        return True

class UpdateQualifiedMaps(RepeatedTask):
    
    def __init__(self) -> None:
        super().__init__("update_qualified_beatmaps", 2400)
        
    def run(self):
        with database.managed_session() as session:
            sets = []
            for map in session.query(DBBeatmap).filter(DBBeatmap.status['bancho'].astext.cast(Integer) == 3, (datetime.now() - DBBeatmap.last_db_update) > timedelta(days=1)):
                if map.set_id in sets:
                    continue
                # TODO: check if beatmaps got deleted
                beatmaps.get_beatmapset(map.set_id, force_fetch=True, session=session)
                sets.append(map.set_id)
            for map in session.query(DBBeatmap).filter(DBBeatmap.status['akatsuki'].astext.cast(Integer) == 3, (datetime.now() - DBBeatmap.last_db_update) > timedelta(days=1)):
                if map.set_id in sets:
                    continue
                # Same
                beatmaps.get_beatmapset(map.set_id, force_fetch=True, session=session)
                sets.append(map.set_id)
            session.commit()
            return True

class UpdateAkatsukiMaps(RepeatedTask):
    
    def __init__(self):
        super().__init__("update_akatsuki_maps", 2400)

    def can_run(self) -> bool:
        
        return super().can_run()

    def parse_akatsuki_embed(self, embed: Embed) -> Tuple[int, str]:
        if 'url' in embed and embed['url']: # new embed format
            beatmap_id = int(embed['url'].split("/")[-1])
            nominator = embed['author']['name'].split()[0]
            return beatmap_id, nominator
        else:
            if embed['fields'][3]['name'] == "Gamemode": # very old embed format
                beatmap_id = int(embed['fields'][4]['value'].split("/")[-1][:-1])
            else:
                beatmap_id = int(embed['fields'][3]['value'].split("/")[-1][:-1])
            nominator = embed['fields'][2]['value'].split("]")[0][1:]
            return beatmap_id, nominator

    def run(self):
        logger.info("Fetching akatsuki beatmaps...")
        found: List[selfbot.Message] = list()
        with database.managed_session() as session:
            full_run = session.get(DBTask, self.task_name) is None
            for x in range(0, 10000000 if full_run else 250 ,25):
                messages = selfbot.search_channel(offset=x)
                if not messages:
                    logger.info('messages done.')
                    break
                found.extend(messages)
            found.reverse()
            maps = list()
            for message in found:
                message = message[0]
                if not message['embeds']:
                    continue
                embed = message['embeds'][0]
                maps.append(self.parse_akatsuki_embed(embed))
            logger.info(f"Found {len(maps)} Akatsuki updates")

            for beatmap_id, nominator in maps:
                if (beatmap := beatmaps.get_beatmap(beatmap_id, force_fetch=True, session=session)):
                    beatmap.status['akatsuki'] = -1
                    beatmap.beatmapset.nominators['akatsuki'] = [nominator]
                    flag_modified(beatmap, 'status')
                    flag_modified(beatmap.beatmapset, 'nominators')
                else:
                    pass # Deleted map most likely
                session.commit()
                session.flush()
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