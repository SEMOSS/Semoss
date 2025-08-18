# GAAS Server Proxy (`gaas_server_proxy.py`)

The `py/gaas_server_proxy.py` module defines the `ServerProxy` class. This class is crucial for enabling Python code running within the GAAS TCP server environment (handled by `gaas_tcp_server_handler.py`) to make calls *back* to the SEMOSS Java backend.

## `ServerProxy` Class

*   **Purpose**: The `ServerProxy` acts as a client or bridge from the Python side of GAAS to the main SEMOSS Java application. It allows Python functions or tools (like `DatabaseEngine`, `ModelEngine`, etc., defined in other `gaas_gpt_*.py` files) to invoke operations on the Java side, such as executing Pixel scripts (Reactors) or calling methods on specific Java `IEngine` instances.
*   **Context**: It assumes it's being used within the context of a `TCPServerHandler` instance, from which it gets a reference to the server (`self.server = TCPServerHandler.da_server`) to manage communication and synchronization.

### Initialization

The constructor `__init__(self)`:
*   Initializes a `threading.Condition()` object (`self.condition`) which is used for synchronizing threads when waiting for a response from the Java backend.
*   It retrieves a reference to the active `TCPServerHandler` instance via `TCPServerHandler.da_server`. This static attribute in `TCPServerHandler` is set when a handler instance is created, effectively making the `ServerProxy` aware of the current client connection handler.

### Key Methods and Functionality

*   **`get_next_epoc(self) -> str`**:
    *   **Purpose**: Generates a unique "epoc" ID (a string).
    *   **Logic**: Creates a random string prefixed with "py_" and followed by 17 random digits.
    *   **Usage**: This `epoc` ID is used to tag requests sent to the Java backend and to match incoming responses, enabling asynchronous-like communication where Python can send a request, wait, and then be notified when the specific response arrives.

*   **`comm(self, epoc: str, engine_type: str, engine_id: str, method_name: str, method_args: Optional[List[Any]] = [], method_arg_types: Optional[List[str]] = [], insight_id: Optional[str] = None, operation: str = "REACTOR")`**:
    *   **Purpose**: This is the core private communication method that sends a structured request to the SEMOSS Java backend via the `TCPServerHandler`.
    *   **Inputs**:
        *   `epoc` (str): The unique ID for this request.
        *   `engine_type` (str): The type of SEMOSS engine on the Java side to target (e.g., "model", "storage", "database", "vector"). Used when `operation` is "ENGINE".
        *   `engine_id` (str): The ID of the specific engine instance.
        *   `method_name` (str): The name of the Java method to invoke on the target engine.
        *   `method_args` (Optional[List[Any]]): Arguments for the Java method.
        *   `method_arg_types` (Optional[List[str]]): Java class names for the argument types, used for reflection on the Java side.
        *   `insight_id` (Optional[str]): The current insight context. If not provided, it attempts to get it from the `thread_local.payload` of the `TCPServerHandler`.
        *   `operation` (str, default: "REACTOR"): The type of operation being requested on the Java side (e.g., "REACTOR", "ENGINE").
    *   **Logic**:
        1.  Constructs a `payload` dictionary (mimicking Java's `PayloadStruct`) with all the provided arguments.
        2.  Registers `self.condition` in `self.server.monitors` (a dictionary in `TCPServerHandler`) using the `epoc` as the key. This is how the handler knows which condition to notify when a response with this `epoc` arrives.
        3.  Acquires `self.condition`.
        4.  Calls `self.server.send_request(payload)` to send the payload to the connected Java client (which is the SEMOSS backend).
        5.  Calls `self.condition.wait()`, causing the current Python thread to block until the Java backend sends a response with the matching `epoc` and the `TCPServerHandler` notifies this condition.
        6.  Releases `self.condition` after being awakened.
    *   **Note**: This method itself doesn't return the response directly; the response is expected to be retrieved from `self.server.monitors` by the calling methods (`callReactor` or `callEngine`) after the wait.

*   **`callReactor(self, epoc: str, pixel: str, insight_id: Optional[str] = None)`**:
    *   **Purpose**: To execute a Pixel script on the SEMOSS Java backend.
    *   **Inputs**:
        *   `epoc` (str): Unique ID for the request.
        *   `pixel` (str): The Pixel script to execute.
        *   `insight_id` (Optional[str]): The insight context.
    *   **Logic**:
        1.  Retrieves the original payload from `self.server.thread_local` to maintain context if needed for the new thread.
        2.  Defines an inner function `set_thread_local_payload` that sets `self.server.thread_local.payload` (important for nested calls or context preservation in the new thread) and then calls `self.comm(...)` with `operation="REACTOR"` and the Pixel script in `method_args`.
        3.  Starts a new `threading.Thread` to execute `set_thread_local_payload`. This makes the call to Java asynchronous from the perspective of the main Python thread that invoked `callReactor`, but the new thread itself blocks on `self.comm`.
        4.  `thread.join()`: Waits for the new thread (and thus the `self.comm` call) to complete.
        5.  Retrieves the response payload from `self.server.monitors.pop(epoc)`.
        6.  If the response contains an exception (`"ex"` key), it raises it. Otherwise, it returns the content of `new_payload_struct["payload"]`.
    *   **Output**: The result of the Pixel execution from the Java backend.

*   **`callEngine(self, epoc: str, engine_type: str, engine_id: str, method_name: str = "None", method_args: Optional[List[Any]] = [], method_arg_types: Optional[List[str]] = [], insight_id: Optional[str] = None)`**:
    *   **Purpose**: To call a specific method on a SEMOSS `IEngine` instance on the Java backend.
    *   **Logic**: Very similar to `callReactor`:
        1.  It also uses a new thread to call `self.comm(...)` with `operation="ENGINE"` and the engine-specific details.
        2.  Waits for the thread to complete.
        3.  Retrieves the response from `self.server.monitors`.
        4.  Handles exceptions or returns the payload.
    *   **Output**: The result of the engine method call from the Java backend.

### Relationship with Other GAAS Components

*   **`TCPServerHandler`**: `ServerProxy` is tightly coupled with `TCPServerHandler`. It relies on the handler's `da_server` static attribute to get a reference to the active handler instance, which provides the socket connection (`send_request`) and the `monitors` dictionary for response synchronization.
*   **GAAS Tools (`DatabaseEngine`, `ModelEngine`, etc.)**: These higher-level tool classes (like `gaas_gpt_database.DatabaseEngine`, `gaas_gpt_model.TomcatModelEngine`) inherit from `ServerProxy`. When these tools need to interact with the SEMOSS Java backend (e.g., to execute a query on a Java-managed database or run a Pixel script for a model), they use the `callReactor` or `callEngine` methods provided by `ServerProxy`.

In essence, `ServerProxy` is the mechanism that allows Python code running within the GAAS TCP server environment to "call out" to the main SEMOSS Java application, execute operations there, and receive results, effectively bridging the Python and Java components of SEMOSS.
