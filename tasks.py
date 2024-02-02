from common.service import RepeatedTask

class UpdateBanchoMaps(RepeatedTask):
    
    def __init__(self) -> None:
        super().__init__("update_bancho_maps", 3600)
    
    def run(self):
        return True
