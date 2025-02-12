from gaas_server_proxy import ServerProxy
from typing import Optional
import random
import string

class Insight(ServerProxy):
    def __init__(self, insight_id=None):
        super().__init__()
        self.insight_id = insight_id

    def run_pixel(self, pixel: str = None, insight_id: Optional[str] = None):
        """
        This method is responsible for running an input pixel command

        Args:
            pixel (`str`): The pixel expression to execute
            insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated

        Returns:
            List[Dict]: the json object output from the pixel expression
        """
        assert pixel is not None
        if insight_id is None:
            insight_id = self.insight_id
        assert insight_id is not None

        #epoc = super().get_next_epoc()
        epoc ="py_" + "".join(random.choice(string.digits) for _ in range(17))
        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn
