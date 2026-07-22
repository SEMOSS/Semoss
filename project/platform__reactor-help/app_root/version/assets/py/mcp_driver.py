import json
from semoss import Insight


def runPixel(pixel: str) -> str:
    """
    Run an arbitrary pixel expression and return the full JSON output.
    Args:
        pixel (str): The pixel expression to execute.
    Returns:
        str: The JSON output from the pixel execution.
    """
    try:
        insight = Insight()
        response = insight.run_pixel(pixel)
        return json.dumps(response)
    except Exception as e:
        return f"Error: {e}"


def runPixelHelp(reactor_name: str) -> str:
    """
    Run the specified reactor with the --help flag and return the clean output.
    Args:
        reactor_name (str): The name of the reactor to run.
    Returns:
        str: The clean output from the reactor's help command, or an error message if something goes wrong.
    """
    try:
        insight = Insight()
        response = insight.run_pixel(f"{reactor_name} --help")

        # If response is already a string, try parsing it as JSON
        if isinstance(response, str):
            response = json.loads(response)

        # Navigate the nested structure to get the clean output
        pixel_return = response[0]["pixelReturn"]
        output = pixel_return[0]["output"]

        return output
    except Exception as e:
        return f"Error: {e}"
