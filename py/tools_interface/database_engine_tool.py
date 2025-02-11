from pydantic import BaseModel, Field
from typing import Literal, List

class SQLProperty(BaseModel):
    type: str = Field(
        "string"
    )
    description: str = Field(
        "The SQL query used for data retrieval adjusted for the user's query."
    )

class IDProperty(BaseModel,):
    type: str = Field(
        "string"
    )
    description: str = Field(
        "The unique identifier for the database. Only one option"
    )
    enum: List[str]

class PropertiesObject(BaseModel):
    database_id: IDProperty
    sql: SQLProperty

class Parameters(BaseModel):
    type:  str = Field(
        "object"
    )
    properties: PropertiesObject
    required: List[str] = ["database_id","sql"]

class DatabaseEngineObject(BaseModel):
    name: Literal["database_engine"] = "database_engine"
    description: str
    parameters: Parameters

class DatabaseToolObject(BaseModel):
    type: Literal["function"] = "function"
    function: DatabaseEngineObject

def createDatabaseTool(description: str, database_id: str):
    sqlProp = SQLProperty()
    idProp = IDProperty(enum=[database_id])
    props = PropertiesObject(database_id= idProp, sql=sqlProp)
    parameters = Parameters(properties=props)
    database_engine = DatabaseEngineObject(description=description, parameters=parameters)
    return DatabaseToolObject(function=database_engine)

# # Example usage
# description = "A database engine for analyzing housing market attributes. Key data attributes include: bedroom_count (number of bedrooms), net_sqm (total usable interior space), center_distance (distance from downtown), metro_distance (distance to nearest metro station), floor (level within the building), age (years since construction or renovation), and price (cost for purchasing or renting)."
# database_id = "eb90b424-502c-4793-aed8-944636c54ca5"
# tool = createDatabaseTool(description, database_id)
# print(tool.model_dump_json())