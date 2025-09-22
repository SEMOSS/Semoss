from gaas_server_proxy import ServerProxy
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class InputMap(BaseModel):
    input: str = Field(..., description="The input message to be processed")
    type: str = Field(..., description="The type of input (e.g., 'text')")
    embeddings: List[float] = Field(
        ..., description="The embeddings of the input message"
    )


def semantic_router(
    input_maps: List[InputMap], referenceTopics: List[str]
) -> List[Dict[str, Any]]:

    results = []

    for input_map in input_maps:
        payload = {
            "message": input_map.input,
            "scores": {topic: 0.0 for topic in referenceTopics},
            "decision": referenceTopics[0],
        }
        results.append(payload)

    return results
