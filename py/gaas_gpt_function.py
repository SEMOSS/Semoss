from gaas_server_proxy import ServerProxy
from typing import Optional
import logging

logger: logging.Logger = logging.getLogger(__name__)


class FunctionEngine(ServerProxy):
    def __init__(self, engine_id: str, insight_id: Optional[str] = None):
        super().__init__()

        self.engine_id = engine_id
        self.insight_id = insight_id

        logger.info("FunctionEngine initialized with engine id " + engine_id)

    def execute(self, parameterMap: dict, insight_id: Optional[str] = None) -> None:
        """
        Connect to a function and execute

        Args:
            parameterMap (`dict`): A dictionary with the payload for the function engine
        """
        if insight_id is None:
            insight_id = self.insight_id

        pixel = (
            f'ExecuteFunctionEngine(engine = "{self.engine_id}", map=[{parameterMap}]);'
        )
        epoc = super().get_next_epoc()

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            return output["output"]

        return pixelReturn