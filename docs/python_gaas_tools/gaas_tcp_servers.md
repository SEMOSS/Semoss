# GAAS TCP Server Infrastructure

The Python TCP server infrastructure for GAAS (Generative AI Agent Services) within SEMOSS is primarily defined by two files: `py/gaas_tcp_socket_server.py` and `py/gaas_tcp_server_handler.py`. Together, they provide a socket-based server that can receive commands from the SEMOSS Java backend, process them (often by executing Python code or interacting with other GAAS tools), and return results.

## 1. `gaas_tcp_socket_server.py` - The Main Server

This module contains the `Server` class, which sets up and manages the TCP socket server.

### `Server` Class (extends `socketserver.ThreadingTCPServer`)

*   **Purpose**: To create a multi-threaded TCP server that listens for incoming connections from the SEMOSS Java backend (typically via `ClientProcessWrapper.java`). Each incoming connection is handled in a separate thread by an instance of `TCPServerHandler`.
*   **Initialization `__init__(...)`**:
    *   `server_address` (Optional[Tuple[str, int]]): The address (host, port) to bind to. Defaults to `("localhost", self.port)`.
    *   `handler_class` (default: `TCPServerHandler`): The class to instantiate for each client connection.
    *   `port` (int, default: 81, but often overridden by command-line args or `9999` if run directly): The port number for the server to listen on.
    *   `max_count` (int, default: 1): The maximum number of concurrent client connections to handle before potentially waiting or refusing. If 1, it's in "user mode".
    *   `py_folder` (str, default: "."): Path to the Python scripts directory, added to `sys.path`.
    *   `insight_folder` (str, default: "."): Path to the insight's working directory, used for logging and potentially file operations.
    *   `prefix` (str, default: ""): A prefix string that might be used in communication or logging.
    *   `timeout` (int, default: 15 minutes): Server-level timeout. If no activity or new connections within this period and `cur_count == 0`, the server may shut down (`handle_timeout`).
    *   `start` (bool, default: `True`): Whether to start serving immediately upon initialization.
    *   `blocking` (bool, default: `False`): If `True`, the handler processes requests synchronously; otherwise, it spawns a new thread for each request's `get_final_output` method.
    *   `logger_level` (str, default: "INFO"): Logging level for the server.
*   **Key Methods & Functionality**:
    *   `__init__(...)`: Initializes the `socketserver.ThreadingTCPServer` and sets up custom attributes like port, timeout, and paths. It can also optionally clear environment variables except for a predefined list if `PY_SOCKET_ENV_VARS` is set.
    *   `serve_forever()`: Starts the server loop. It continuously handles requests up to `max_count`. If `max_count` is reached, it waits on a `threading.Condition` until a handler finishes.
    *   `handle_request()`: Handles a single incoming connection by instantiating the `handler_class`.
    *   `handle_timeout()`: Called if the server's main listening socket times out. If no clients are connected (`self.cur_count == 0`), it calls `stop_it()`.
    *   `remove_handler()`: Decrements `self.cur_count` when a handler finishes and notifies the main server loop if it was waiting.
    *   `stop_it()`: Stops the server loop and closes the server socket.
*   **Command-Line Interface**: The script can be run directly.
    *   `parse_args()`: Parses command-line arguments for port, max_count, py_folder, insight_folder, prefix, timeout, start, logger_level, and `userChrootFolder`.
    *   **Chroot**: If `userChrootFolder` is provided, the script attempts to `os.chroot()` to that directory and `os.chdir("/")` for security before starting the server. This sandboxes the server process.
*   **Usage**: Typically launched by `ClientProcessWrapper.java` from the SEMOSS backend, with parameters passed as command-line arguments.

## 2. `gaas_tcp_server_handler.py` - The Request Handler

This module contains the `TCPServerHandler` class, which processes individual client connections.

### `TCPServerHandler` Class (extends `socketserver.BaseRequestHandler`)

*   **Purpose**: An instance of this class is created for each connection to the `Server`. It handles reading requests from the client, processing them, and sending back responses.
*   **Class Attributes**:
    *   `da_server` (static): Holds a reference to the handler instance, seemingly for singleton-like access or inter-handler communication (though its usage in a multi-threaded server needs careful consideration).
    *   `thread_local` (static, `threading.local()`): Used to store thread-specific data, notably the current request `payload`.
*   **Initialization and Setup (`setup()`)**:
    *   Called for each new connection.
    *   Initializes instance variables: `stop` flag, `monitor` (threading.Condition), `monitors` (dict for response synchronization), paths (`insight_folder`, `prefix` from server), logging.
    *   Sets a timeout on the request socket based on `self.server.timeout_val`.
    *   Initializes `jsonpickle` (aliased as `self.serializier`, though `json` is also used) for potential complex object serialization (experimental, `self.try_jp`).
    *   Creates a `semoss_console.SemossConsole` instance to capture `stdout` and `stderr` and stream them back to the client.
*   **Key Methods & Functionality**:
    *   **`handle()` (Main Loop)**:
        *   Continuously listens for incoming messages on the client socket (`self.request`).
        *   **Message Protocol**: Reads messages that are prefixed with:
            1.  Size (4 bytes, big-endian): Integer indicating the length of the JSON payload.
            2.  Epoc (20 bytes, UTF-8 string): A unique request identifier generated by the Java client.
        *   Reads the JSON payload of the specified `size`.
        *   If `self.server.blocking` is `False` (default), it spawns a new thread to call `self.get_final_output(data, epoc)` to process the request asynchronously. Otherwise, calls it synchronously.
        *   If the connection is closed or an error occurs, it calls `self.stop_request()` to terminate the handler.
    *   **`get_final_output(self, data=None, epoc=None)`**:
        *   This is the core processing logic for a received message.
        *   Sets `self.thread_local.payload` with the deserialized JSON payload.
        *   Parses the `operation` and `payload` (command list) from the message.
        *   **Command Dispatching**:
            *   If `operation == "CMD"` and `command == "stop"`: Calls `self.stop_request()`.
            *   If `operation == "CMD"` and `command == "prefix"`: Updates `self.prefix`.
            *   If `operation == "CMD"` and `command == "CLOSE_ALL_LOGOUT<o>"`: Calls `self.stop_request()`.
            *   If `operation == "PYTHON"`: Calls `self.handle_python(command)`.
            *   If `payload["response"]` is `True`: Calls `self.handle_response()` (for responses to Python-initiated callbacks to Java).
            *   If `operation == "CMD"` (and not a special command): Calls `self.handle_shell()` (experimental shell command execution).
            *   Otherwise, sends an error back indicating an unsupported command.
        *   Handles exceptions by sending an error response back to the client, including the `epoc`.
    *   **`handle_python(self, command: str)`**:
        *   Sets `self.console.set_payload()` for context.
        *   Redirects `stdout` and `stderr` to `self.console`.
        *   If `command` ends with ".py" or starts with "smssutil" (legacy script execution): Attempts `eval()` then `exec()`.
        *   Otherwise (general Python code): Calls `self.execute_and_capture(command)`.
        *   Sends the output or exception back using `self.send_output()`.
    *   **`execute_and_capture(self, code: str) -> Tuple[str, bool]`**:
        *   Attempts to mimic Jupyter kernel behavior: `exec`s all but the last line of the code, then `eval`s the last line if it's a valid expression. If not, `exec`s the last line.
        *   Returns a tuple: `(output_string, is_exception_boolean)`.
    *   **`handle_response(self)`**:
        *   Called when Java sends a response to a callback initiated by Python (via `gaas_server_proxy`).
        *   Retrieves the `threading.Condition` object associated with the response's `epoc` from `self.monitors`.
        *   Updates `self.monitors` with the response payload.
        *   Notifies the waiting Python thread.
    *   **`send_output(self, output, operation="STDOUT", response=False, interim=False, exception=False)`**:
        *   Constructs a response payload dictionary (mirroring Java's `PayloadStruct`) including `epoc`, `payload` (the actual output), `operation`, `response` flag, `interim` flag (for streamed stdout), and `ex` (if an exception occurred).
        *   Serializes this payload to JSON (using `self.serializier`, which is `json` by default).
        *   Sends the message back to the client, prefixed with size and `epoc`.
    *   **`send_request(self, payload: Dict)`**: (Used by `gaas_server_proxy.py` for callbacks to Java)
        *   Serializes and sends a request payload (initiated by Python) to the Java client.
    *   **`handle_shell(self)`**: (Experimental)
        *   Handles basic shell commands like `cd`, `ls`, `cp`, `mv`, `git`, `mvn`, `rm`, `pwd`, `mkdir`.
        *   Manages current working directories per "mount" (derived from `insightId`).
        *   Attempts to sandbox `cd` operations within the original mount point.
        *   Uses `subprocess.Popen` to execute commands.
    *   **Logging**: Includes `logging_setup`, `custom_dev_logger`, `prod_logger` for file-based logging within the `insight_folder`.
    *   **`stop_request(self)`**: Closes the client connection, removes the handler from the server's count, and exits the handling thread.

### Relationship and Data Protocol

*   The `Server` (`gaas_tcp_socket_server.py`) listens for connections and creates a `TCPServerHandler` (`gaas_tcp_server_handler.py`) instance for each.
*   **Data Protocol**:
    *   Messages are exchanged over TCP/IP sockets.
    *   Each message is framed:
        1.  **Size (4 bytes)**: Integer, big-endian, length of the JSON payload.
        2.  **Epoc (20 bytes)**: UTF-8 string, unique request/response identifier.
        3.  **Payload (JSON string)**: A JSON object typically mirroring the structure of Java's `prerna.tcp.PayloadStruct`, containing fields like `epoc`, `payload` (list of strings/data), `operation` (e.g., "PYTHON", "CMD"), `response` (boolean), `interim` (boolean for streaming), `ex` (exception details).
*   **Serialization**: `json` is the primary serializer, with experimental support for `jsonpickle` mentioned. Pandas DataFrames and NumPy datetime64 objects have custom handlers when `jsonpickle` is used.

This TCP server infrastructure allows the SEMOSS Java backend to offload Python code execution and manage Python processes, enabling a wide range of Python-based functionalities within the platform.
