from gaas_server_proxy import ServerProxy
from typing import Optional, Callable, Any, Dict, List
import ast


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

    def get_insight_id(self):
        """
        This method is responsible for getting the insight id

        Returns:
            str: The insight id
        """
        return self.insight_id


class SemossParameterSchema:
    def __init__(self, fields: Dict[str, Dict[str, Any]]):
        self.fields = fields

    def __getitem__(self, key):
        return self.fields[key]

    def __iter__(self):
        return iter(self.fields)

    def items(self):
        return self.fields.items()

    def add(self, new_fields: Dict[str, Dict[str, Any]]):
        self.fields = {**self.fields, **new_fields}

    def __repr__(self):
        return f"ParameterSchema({self.fields})"

    def to_json_schema(self):
        return {
            "type": "object",
            "properties": {
                name: {"type": meta["type"], "description": meta["description"]}
                for name, meta in self.fields.items()
            },
            "required": list(self.fields.keys()),
        }


class BaseSemossTool:

    def __init__(
        self,
        func: Callable,
        name: str,
        description: str,
        arguments: SemossParameterSchema,
        returns: Optional[str],
    ):
        self.func = func
        self.name = name
        self.description = description
        self.arguments = arguments
        self.returns = returns

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def info(self):
        return {
            "name": self.name,
            "description": self.description,
            "arguments": self.arguments.fields,
            "json_schema": self.arguments.to_json_schema(),
            "returns": self.returns,
        }


class SemossTool:

    def __init__(
        self,
        name: str,
        description: str,
        arguments: SemossParameterSchema,
        returns: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.arguments = arguments
        self.returns = returns

    def __call__(self, func: Callable):
        return BaseSemossTool(
            func, self.name, self.description, self.arguments, self.returns
        )


class SemossToolParser:

    def get_function_info(self, filePath: str) -> List:
        """
        Flattens and pulls out all the methods as tools from a file

        Args:
            filePath: the path to the file

        Returns:
            A list of all the details around each method
        """
        with open(filePath, "r") as file:
            tree = ast.parse(file.read())

        functions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                function_name = node.name
                docstring = ast.get_docstring(node)
                tool = self.parse_docstring(node, function_name, docstring)
                if tool is not None:
                    functions.append(tool)
                else:

                    schema = SemossParameterSchema({})
                    for arg in node.args.args:
                        schema.add(
                            {
                                arg.arg: {
                                    "description": "",
                                    "type": "",
                                }
                            }
                        )

                    functions.append(
                        BaseSemossTool(
                            func=node,
                            name=function_name,
                            description=docstring,
                            arguments=schema,
                            returns=node.returns.id,
                        )
                    )

                    # decorators = []
                    # for decorator in node.decorator_list:
                    #     if isinstance(decorator, ast.Name):
                    #         decorators.append(decorator.id)
                    #     elif isinstance(decorator, ast.Attribute):
                    #         decorators.append(self._flatten_attr(decorator))

        return functions

    def parse_docstring(
        self, node: ast.FunctionDef, name: str, doc_string: str
    ) -> BaseSemossTool:
        try:
            from docstring_parser import parse
            from docstring_parser import ParseError

            doc = parse(doc_string)

            schema = SemossParameterSchema({})
            for param in doc.params:
                schema.add(
                    {
                        param.arg_name: {
                            "description": param.description,
                            "type": param.type_name,
                        }
                    }
                )

            tool = BaseSemossTool(
                func=node,
                name=name,
                description=doc.short_description,
                arguments=schema,
                returns=doc.returns,
            )

            return tool
        except ParseError as e:
            print(f"Docstring parse error in {name}: {e}")
        except Exception as e:
            print(f"Unexpected error parsing docstring in {name}: {e}")
        return None

    def _flatten_attr(self, node) -> str:
        """
        Flattens an ast.Attribute node into a string.

        Args:
            node: The ast.Attribute node.

        Returns:
            A string representation of the attribute.
        """
        if isinstance(node, ast.Attribute):
            return self._flatten_attr(node.value) + "." + node.attr
        elif isinstance(node, ast.Name):
            return node.id
        return ""
