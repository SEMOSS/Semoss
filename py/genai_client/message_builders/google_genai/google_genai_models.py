from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel
from ...utils import StringEnum


class GoogleRoles(StringEnum):
    USER = "user"
    MODEL = "model"
