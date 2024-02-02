from common.service import TaskedService
from .tasks import *

def get_service() -> TaskedService:
    return TaskedService("beatmaps_svc", [UpdateBanchoMaps()])