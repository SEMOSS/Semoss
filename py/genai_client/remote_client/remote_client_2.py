from typing import Optional, Literal, Dict, Any
from pydantic import BaseModel, Field
from kazoo.client import KazooClient
import httpx
import json
import asyncio
from threading import Thread


class ModelStatus(BaseModel):
    status: Literal["cold", "warming", "active"] = Field(
        description="Current status of the model deployment"
    )
    message: str = Field(description="Descriptive message about the model status")
    cluster_ip: Optional[str] = Field(
        default=None, description="IP address of the cluster where model is deployed"
    )


class ModelDeploymentConfig(BaseModel):
    model_repo_name: str = Field(description="Name of the model repository")
    model_name: str = Field(description="Name of the model")
    model_id: str = Field(description="Unique identifier for the model")
    deployer_endpoint: str = Field(description="Endpoint for model deployment")
    is_dev: Optional[bool] = Field(
        default=False, description="Indicates if the deployment is for development"
    )
    model_config = {"protected_namespaces": ()}


class RemoteClient2:
    def __init__(self, host: str, config: ModelDeploymentConfig):
        self.config = config
        self.status: Optional[str] = None
        self.cluster_ip: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=300.0)

        self.zk = KazooClient(hosts=host)
        self.zk.start()
        print("Zookeeper client started.")

    def _get_model_url(self) -> Optional[str]:
        """
        Construct the URL using cluster IP if available
        """
        if not self.cluster_ip:
            print("No cluster IP available for the model")
            return None
        if self.config.is_dev:
            print("Using dev port forwarding endpoint...")
            return f"http://127.0.0.1:8888/api/generate"
        return f"http://{self.cluster_ip}:8888/api/generate"

    async def gaas_request(
        self, request_payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Make a request to the model endpoint with SSE handling
        """
        url = self._get_model_url()
        if not url:
            return None

        headers = {"Accept": "text/event-stream"}
        try:
            async with self.client.stream(
                "POST", url, json=request_payload, headers=headers
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str:
                            data = json.loads(data_str)
                            status = data.get("status")
                            message = data.get("message")

                            print(f"Status Update: {status} - {message}")

                            if status == "complete":
                                return data
                            elif status in ["error", "cancelled", "timeout"]:
                                print(f"Job {status}: {message}")
                                return None

            return None
        except httpx.HTTPError as e:
            print(f"HTTP request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    def _check_model_path(self, path: str, status: str) -> bool:
        """
        Check if the model path exists in Zookeeper on the given path (warming or active).

        Args:
            path: Zookeeper path to check
            status: Expected status of the model

        Returns:
            bool: True if model exists at path, False otherwise
        """
        if self.zk.exists(path):
            try:
                data, _ = self.zk.get(path)
                if data:
                    self.status = status
                    self.cluster_ip = data.decode("utf-8")
                    print(f"Found model at {path} with cluster IP: {self.cluster_ip}")
                    return True
            except Exception as e:
                print(f"Error checking path {path}: {e}")
        return False

    def _get_model_status(self) -> Literal["cold", "warming", "active"]:
        active_path = f"/models/active/{self.config.model_id}"
        warming_path = f"/models/warming/{self.config.model_id}"

        print(f"Checking active path: {active_path}")
        if self._check_model_path(active_path, "active"):
            return "active"

        print(f"Checking warming path: {warming_path}")
        if self._check_model_path(warming_path, "warming"):
            return "warming"

        print(f"Model {self.config.model_id} not found in either path.")
        return "cold"

    def _deploy_model(self) -> Dict[str, str]:
        """
        Initiate model deployment using a separate thread
        """
        url = f"{self.config.deployer_endpoint}/start"
        payload = self.config.model_dump(exclude={"deployer_endpoint", "is_dev"})

        def _deploy_thread():
            try:
                with httpx.Client() as client:
                    response = client.post(url, json=payload)
                    print(f"Deployment request sent: {response.status_code}")
            except Exception as e:
                print(f"Deployment request error (non-blocking): {e}")

        # Starting deployment in background thread
        thread = Thread(target=_deploy_thread)
        thread.daemon = True  # Thread will terminate when main program exits
        thread.start()

        return {
            "result": "initiated",
            "message": f"Model {self.config.model_name} deployment has been initiated",
        }

    def initialize_model(self) -> ModelStatus:
        """
        Check model status and initiate deployment if needed
        """
        model_status = self._get_model_status()

        if model_status == "cold":
            # Fire off deployment request in background thread.. I need to get this request through but don't care about results right now.. (It can take a few minutes..)
            deployment_result = self._deploy_model()
            return ModelStatus(
                status="cold",
                message="Model deployment has been initiated.",
                cluster_ip=None,
            )
        elif model_status == "warming":
            return ModelStatus(
                status="warming",
                message="Model is currently initializing.",
                cluster_ip=None,
            )

        return ModelStatus(
            status=model_status,
            message=f"Model {self.config.model_id} is {model_status}.",
            cluster_ip=self.cluster_ip if model_status == "active" else None,
        )

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

    def __del__(self):
        try:
            asyncio.run(self.close())
            self.zk.stop()
            print("Zookeeper client stopped.")
        except:
            pass
