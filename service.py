from common.service import TaskedService
from beatmaps.tasks import *

def get_service() -> TaskedService:
    return TaskedService("beatmaps_svc", [UpdateBanchoMaps()])