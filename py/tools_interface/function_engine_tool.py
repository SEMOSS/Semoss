from pydantic import BaseModel, Field

class Map(BaseModel):
    lat: str = Field(..., description="The latitude of the location.")
    lon: str = Field(..., description="The longitude of the location.")

class FunctionEngineParameters(BaseModel):
    function_id: str = Field(..., description="The unique identifier for the function_engine provided in the description.")
    map: Map = Field(..., description="A JSON map containing latitude and longitude values.")

class FunctionEngineTool(BaseModel):
    type: str = Field("function", const=True)
    function: dict = Field(
        {
            "name": "function_engine",
            "description": "Function ID: 1563ddbb-2f77-4094-93f4-083431f15cbc. Use this function engine to call for the current weather based on latitude and longitude.",
            "parameters": FunctionEngineParameters.schema()
        }
    )
