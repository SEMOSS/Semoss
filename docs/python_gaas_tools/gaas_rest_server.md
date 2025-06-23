# GAAS REST Client for SEMOSS API (`gaas_rest_server.py`)

The `py/gaas_rest_server.py` module defines the `RESTServer` class. Contrary to what its name might imply, this class **is not an HTTP server**. Instead, it acts as an **HTTP client** designed to communicate with a running SEMOSS backend's existing REST API.

This client facilitates programmatic interaction with a SEMOSS instance from Python, allowing for authentication, insight management, and Pixel execution.

## `RESTServer` Class

*   **Purpose**: To provide a Python interface for making authenticated REST API calls to a SEMOSS server. This can be used by GAAS components or other Python scripts to leverage the capabilities of a full SEMOSS backend.
*   **Underlying Library**: Uses the `requests` Python library for HTTP communication.

### Initialization

The constructor `__init__(self, access_key=None, secret_key=None, base=None)`:
*   `access_key` (Optional[str]): The access key (typically username or API key ID) for authenticating with the SEMOSS API.
*   `secret_key` (Optional[str]): The secret key (typically password or API secret) for authentication.
*   `base` (Optional[str]): The base URL of the SEMOSS server (e.g., `http://localhost:8080/semoss`).
*   On initialization, it calls the `login()` method to authenticate and establish a session.
*   It maintains connection status (`self.connected`), cookies (`self.cookies`), and a dictionary for monitors (`self.monitors`) which seems related to handling asynchronous-like operations or callbacks if `send_request` is used in such a context.

### Key Methods and Functionality

*   **`login(self)`**:
    *   **Purpose**: Authenticates with the SEMOSS server using Basic Authentication.
    *   **Logic**:
        1.  Combines `access_key` and `secret_key` and base64 encodes them.
        2.  Makes a GET request to the `/auth/whoami` endpoint on the SEMOSS server with the Basic Auth header.
        3.  Stores the session cookies received from the server (`self.cookies`) for subsequent requests.

*   **`make_new_insight(self)`**:
    *   **Purpose**: Creates a new, empty insight on the SEMOSS server.
    *   **Logic**:
        1.  Executes the Pixel command `OpenEmptyInsight()` by calling `self.run_pixel()`.
        2.  Parses the JSON response to extract and return the `insightID` of the newly created insight.
    *   **Output**: The ID of the new insight (str).

*   **`run_pixel(self, payload=None, insight_id=None)`**:
    *   **Purpose**: Executes a given Pixel script on the SEMOSS server within a specified or new insight.
    *   **Inputs**:
        *   `payload` (Optional[str]): The Pixel script to execute.
        *   `insight_id` (Optional[str]): The ID of the insight in which to run the Pixel. If not provided, uses `self.cur_insight`. If `self.cur_insight` is also `None`, it calls `make_new_insight()` to create one and uses that.
    *   **Logic**:
        1.  Constructs a data payload with the `expression` (Pixel script) and `insightId`.
        2.  Makes a POST request to the `/engine/runPixel` endpoint on the SEMOSS server, including session cookies.
        3.  Parses the JSON response and extracts the relevant output using `self.get_pixel_output()`.
    *   **Output**: The processed output from the Pixel execution.

*   **`get_pixel_output(self, payload=None)`**:
    *   **Purpose**: Helper method to extract the core output from the JSON response of a `/engine/runPixel` call.
    *   **Logic**: Navigates the JSON structure: `payload["pixelReturn"][0]["output"]`. If this `output` is a list, it returns the `output` field of the first element of that list.

*   **`logout(self)`**:
    *   **Purpose**: Logs out from the SEMOSS server, invalidating the current session.
    *   **Logic**: Makes a GET request to the `/logout/all` endpoint. Clears local cookies and sets `self.connected` to `False`.

*   **`send_request(self, input_payload)`**:
    *   **Purpose**: Sends a more complex payload to the SEMOSS backend, seemingly designed for scenarios where a Python process needs to invoke a specific Java-side handler for GAAS-related operations (like `RemoteEngineRun`). This is likely part of a mechanism where the Python side acts as a client to Java services that might, in turn, call back to Python or perform other actions.
    *   **Inputs**:
        *   `input_payload` (dict): A dictionary containing details like `epoc` (a unique identifier), `insightId`, and other data to be processed by a `RemoteEngineRun` Pixel command on the server.
    *   **Logic**:
        1.  Serializes the `input_payload` dictionary into a JSON string, escaping quotes.
        2.  Wraps this JSON string within a `RemoteEngineRun(payload="<json_string>");` Pixel command.
        3.  Executes this Pixel command using `self.run_pixel()`.
        4.  The response from `run_pixel` is stored in `self.monitors` keyed by the `epoc`, potentially for retrieval by another part of the application that initiated the request and is waiting on this `epoc`.
    *   **Note**: The use of `self.monitors` suggests this method might be used in conjunction with a system that waits for responses related to specific `epoc` identifiers, possibly for asynchronous-like communication patterns where Python initiates a call to Java and expects a result later.

### Context and Usage

The `RESTServer` class is not a standalone GAAS tool that an LLM would directly invoke by name. Instead, it's a foundational client component that other Python GAAS tools or Python scripts running within the SEMOSS ecosystem (potentially those executed by `PyTranslator` or a local Python engine) would use to:
1.  Authenticate and establish a session with the main SEMOSS Java backend.
2.  Execute arbitrary Pixel scripts to leverage any backend functionality, including data access, reactor calls, or engine operations.
3.  Manage insights programmatically.
4.  Facilitate more complex interactions where the Python environment needs to trigger specific Java-side handlers (like `RemoteEngineRun`) as part of a larger GAAS workflow.

It essentially allows Python code to act as a remote client to the full SEMOSS platform via its REST API.
