import asyncio
import json
from typing import Optional
import websockets


class RemoteClientManager:
    def __init__(self, uri: str):
        self.uri = uri
        self.websocket = None

    async def connect(self, timeout: Optional[int] = 60):
        self.websocket = await websockets.connect(self.uri, timeout=timeout)

    async def send(self, message):
        await self.websocket.send(json.dumps(message))

    async def receive(self):
        return json.loads(await self.websocket.recv())

    async def close(self):
        await self.websocket.close()

    async def send_and_receive(self, message):
        await self.send(message)
        return await self.receive()

    async def gaas_request(self, request, chunked: Optional[bool] = True):
        try:
            await self.connect()
            await self.send(request)

            chunks = []
            total_chunks = None
            metadata = None

            while True:
                try:
                    response = await self.receive()
                    if "error" in response:
                        raise Exception(response["error"])

                    if "total_chunks" in response and "chunk_index" not in response:
                        metadata = response
                        total_chunks = response["total_chunks"]
                    elif "chunk_index" in response:
                        chunks.append(response["data"])

                        if len(chunks) == total_chunks:
                            print("All data chunks received. Reconstruction...")
                            chunked_var = "".join(chunks)
                            break
                    elif response.get("status") == "complete":
                        break
                    elif response.get("status") in ["processing", "waiting"]:
                        print(response.get("message", "Processing..."))

                except asyncio.TimeoutError:
                    print("Timeout error waiting for server response.")
                    raise

            if chunked:
                if not metadata or not chunks:
                    raise Exception("Incomplete data received from server.")
                return metadata, chunked_var
            else:
                if not metadata:
                    raise Exception("Incomplete data received from server.")
                return metadata

        except Exception as e:
            print(f"An error occurred: {e}")
            raise
        finally:
            await self.close()
