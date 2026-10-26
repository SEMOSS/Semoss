"""Unified interface for SEMOSS Python TCP server components.

This module shadows the external `ai-server-sdk` PyPI package so that scripts
written against the SDK can run inside the SEMOSS platform without changes.
The engine classes (ModelEngine, DatabaseEngine, etc.) are aliased to the
internal TCP-bridge implementations, and ServerClient is an in-platform
stand-in that reuses the already-active insight context instead of opening an
HTTP connection back to the server.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from gaas_gpt_model import ModelEngine
from gaas_gpt_database import DatabaseEngine
from gaas_gpt_function import FunctionEngine
from gaas_gpt_storage import StorageEngine
from gaas_gpt_vector import VectorEngine
from semoss import Insight
from gaas_rest_server import RESTServer

logger: logging.Logger = logging.getLogger(__name__)

__all__ = [
    "ModelEngine",
    "DatabaseEngine",
    "FunctionEngine",
    "StorageEngine",
    "VectorEngine",
    "Insight",
    "RESTServer",
    "ServerClient",
]


class ServerClient:
    """In-platform stand-in for the ai-server-sdk ServerClient.

    Scripts written against the external SDK create this object to
    authenticate and open an insight before using the engine classes. When
    the same script runs inside SEMOSS it is already executing on the server
    within an authenticated insight, so this class:
        - accepts and ignores all connection/credential arguments
        - exposes `cur_insight` resolved from the executing thread's payload
        - delegates `run_pixel` and friends to the internal TCP bridge
        - raises NotImplementedError for REST-only capabilities that have no
          in-platform equivalent (file transfer, async job polling, etc.)

    Example (identical to the SDK usage):

    ```python
    >>> from ai_server import ServerClient, ModelEngine
    >>> client = ServerClient(base="<url>", access_key="a", secret_key="b")
    >>> model = ModelEngine(engine_id="<id>", insight_id=client.cur_insight)
    ```
    """

    # class attribute the SDK uses to hold the singleton instance
    da_server = None

    def __init__(
        self,
        base: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        bearer_token_provider: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Accepts the same arguments as the SDK ServerClient but performs no
        authentication - the script is already running inside an
        authenticated SEMOSS session.

        Args:
            base (`Optional[str]`): Ignored. Kept so SDK scripts run unchanged.
            access_key (`Optional[str]`): Ignored.
            secret_key (`Optional[str]`): Ignored.
            bearer_token (`Optional[str]`): Ignored.
            bearer_token_provider (`Optional[str]`): Ignored.
            **kwargs: Ignored.
        """
        logger.info(
            "ServerClient is running inside SEMOSS - connection and credential "
            "arguments are ignored and the active insight context is used"
        )

        self.main_url: Optional[str] = base
        if self.main_url is not None and self.main_url.endswith("/"):
            self.main_url = self.main_url[:-1]

        # stored only so scripts that read these attributes back do not break
        self.access_key: Optional[str] = access_key
        self.secret_key: Optional[str] = secret_key
        self.bearer_token: Optional[str] = bearer_token
        self.bearer_token_provider: Optional[str] = bearer_token_provider

        # never populated in-platform; kept for SDK attribute parity
        self.required_headers: Dict = {}
        self.auth_headers: Dict = {}
        self.cookies = None

        # bridge into the insight this python process is executing within
        self._insight: Insight = Insight()
        self.cur_insight: Optional[str] = self._insight.insight_id

        self.open_insights = set()
        if self.cur_insight is not None:
            self.open_insights.add(self.cur_insight)

        # map to keep track of requests and responses outside of this class
        self.monitors: Dict = {}

        ServerClient.da_server = self

    # ----------------------------------------------------------------------
    # authentication / connection lifecycle - all no-ops in-platform
    # ----------------------------------------------------------------------

    def loginUserAccessKey(self) -> None:
        """No-op in-platform. The user is already authenticated."""
        logger.info("loginUserAccessKey skipped - already authenticated in SEMOSS")

    def loginBearerToken(self) -> None:
        """No-op in-platform. The user is already authenticated."""
        logger.info("loginBearerToken skipped - already authenticated in SEMOSS")

    def reconnect(self) -> None:
        """No-op in-platform. The TCP bridge manages its own connection."""
        logger.info("reconnect skipped - connection is managed by SEMOSS")

    def is_session_login(
        self, headers: Optional[Dict[str, str]] = None
    ) -> Tuple[None, bool]:
        """
        In-platform the session is always logged in.

        Returns:
            `Tuple[None, bool]`: (None, True). The SDK returns the raw
            `/config` response as the first element; there is no HTTP
            response in-platform so None is returned in its place.
        """
        return None, True

    def set_csrf_if_enabled(self) -> None:
        """No-op in-platform. CSRF only applies to REST calls."""
        pass

    def logout(self) -> None:
        """No-op in-platform. The session is managed by SEMOSS."""
        logger.info("logout skipped - session is managed by SEMOSS")

    # ----------------------------------------------------------------------
    # insight management
    # ----------------------------------------------------------------------

    def make_new_insight(self) -> Optional[str]:
        """
        In-platform the script already executes inside an insight, so instead
        of creating a new temporal workspace this returns the active insight.
        """
        if self.cur_insight is None:
            self.cur_insight = self._insight.get_thread_insight_id()
        if self.cur_insight is not None:
            self.open_insights.add(self.cur_insight)
        return self.cur_insight

    def get_open_insights(self) -> List[str]:
        """
        Retrieves a list of insight IDs open in the user's session.

        Returns:
            `List[str]`: List of insight IDs
        """
        open_insights = self.run_pixel(payload="MyOpenInsights();")

        # keep track of the open insights within the python object itself
        self.open_insights = set(open_insights)

        return open_insights

    def drop_insights(self, insight_ids: Union[str, List[str]]) -> None:
        """
        No-op in-platform. The active insight hosts this python process -
        dropping it (or sibling session insights) from within a script would
        tear down live state, so insight cleanup is left to the platform.
        """
        logger.warning(
            "drop_insights skipped - insights are managed by SEMOSS when "
            "running in-platform"
        )

    # ----------------------------------------------------------------------
    # pixel execution - delegated to the internal TCP bridge
    # ----------------------------------------------------------------------

    def run_pixel(
        self,
        payload: str,
        insight_id: Optional[str] = None,
        full_response: Optional[bool] = False,
    ) -> Union[Any, Dict]:
        """
        Execute a pixel expression against the server.

        Matches the SDK contract (raises on ERROR operations, returns either
        the full runPixel response or just the first output) but routes the
        call over the in-platform TCP bridge instead of the REST endpoint.

        Args:
            payload (`str`): DSL (Pixel) instruction on what specific action should be performed
            insight_id (`Optional[str]`): Unique identifier for the temporal workspace where actions are being isolated
            full_response (`Optional[bool]`): Indicate whether to return the full json response or only the actions output

        Returns:
            `Union[Any, Dict]`: The output object from the runPixel response or the entire runPixel response.
        """
        if insight_id is None:
            insight_id = self.cur_insight
            if insight_id is None:
                insight_id = self.make_new_insight()

        raw = self._insight.run_pixel(
            pixel=payload, insight_id=insight_id, raw=True
        )

        # the bridge returns a list whose first element is the runPixel
        # response dict that the REST endpoint would have returned
        response_dict = raw[0] if isinstance(raw, list) else raw

        if "ERROR" in response_dict["pixelReturn"][0]["operationType"]:
            raise Exception(response_dict["pixelReturn"][0]["output"])

        if full_response:
            return response_dict
        else:
            return self.get_pixel_output(response_dict)

    def get_pixel_output(self, response: Dict) -> Union[Any, List]:
        """
        Utility method to grab the output of a runPixel call.

        Args:
            response (`Dict`): The runPixel response from the Tomcat Server

        Returns:
            `Union[Any, List]`: The output object or a list of objects
        """
        main_output = response["pixelReturn"][0]["output"]

        if isinstance(main_output, list):
            output = main_output[0]
        else:
            output = main_output

        return output

    def send_request(self, payload_struct: Dict) -> None:
        """
        Constructs the pixel payload from a PayloadStruct for various server
        resources such as ModelEngine, StorageEngine and DatabaseEngine.

        Args:
            payload_struct (`Dict`): the actual payload being sent to the AI Server

        Returns:
            `None`
        """
        epoc = payload_struct["epoc"]

        input_payload_message = json.dumps(payload_struct, ensure_ascii=False)

        logger.info("Sending a PayloadStruct " + input_payload_message)

        output_payload_message = self.run_pixel(
            payload='RemoteEngineRun(payload="<e>' + input_payload_message + '</e>");',
            insight_id=payload_struct["insightId"],
        )

        if epoc in self.monitors:
            self.monitors[epoc] = output_payload_message

    # ----------------------------------------------------------------------
    # REST-only capabilities with no in-platform equivalent
    # ----------------------------------------------------------------------

    def get_auth_headers(self) -> Dict:
        """Not available in-platform - there are no REST auth headers."""
        raise NotImplementedError(
            "get_auth_headers is not available when running inside SEMOSS - "
            "the script is already executing in an authenticated session"
        )

    def get_openai_endpoint(self) -> str:
        """Get the corresponding openai endpoint for the AI server."""
        if self.main_url:
            return self.main_url + "/model/openai"
        raise NotImplementedError(
            "get_openai_endpoint requires the 'base' url to be passed to "
            "ServerClient - no REST connection exists when running inside "
            "SEMOSS"
        )

    def run_pixel_async(
        self, payload: str, insight_id: Optional[str] = None
    ) -> Union[Any, Dict]:
        """Not available in-platform - use run_pixel instead."""
        raise NotImplementedError(
            "run_pixel_async is not available when running inside SEMOSS - "
            "use run_pixel instead"
        )

    def get_partial_responses(self, job_id: str) -> None:
        """Not available in-platform - use run_pixel instead."""
        raise NotImplementedError(
            "get_partial_responses is not available when running inside "
            "SEMOSS - use run_pixel instead"
        )

    def import_data_product(
        self, project_id: str, insight_id: str, sql: str
    ) -> None:
        """Not available in-platform."""
        raise NotImplementedError(
            "import_data_product is not available when running inside SEMOSS"
        )

    def upload_files(
        self,
        files: List[str],
        project_id: Optional[str] = None,
        insight_id: Optional[str] = None,
        path: Optional[str] = None,
    ) -> None:
        """Not available in-platform - the script already runs on the server."""
        raise NotImplementedError(
            "upload_files is not available when running inside SEMOSS - files "
            "created by this script already live in the server's insight "
            "workspace, use standard file I/O instead"
        )

    def download_file(
        self,
        file: str,
        project_id: Optional[str] = None,
        insight_id: Optional[str] = None,
        custom_filename: Optional[str] = None,
    ) -> None:
        """Not available in-platform - the script already runs on the server."""
        raise NotImplementedError(
            "download_file is not available when running inside SEMOSS - files "
            "in the insight workspace are directly accessible, use standard "
            "file I/O instead"
        )
