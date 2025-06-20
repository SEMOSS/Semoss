# Java to Python Communication

SEMOSS facilitates interoperability between its Java backend and Python scripts/environments. This communication is primarily achieved through a TCP-based client-server architecture, allowing Java to invoke Python functions, execute scripts, and exchange data.

## 1. Overview

### 1.1. Visual Flow / Diagram Placeholder

*(A visual diagram here would be beneficial to illustrate the interaction between Java's `PyTranslator`/`ClientProcessWrapper`, the TCP socket communication, and Python's `gaas_tcp_socket_server`/`gaas_tcp_server_handler`. The diagram should show the request flow from Java to Python, the execution of Python code, the streaming of stdout/stderr, the final response, and the callback mechanism via `gaas_server_proxy`.)*

The core mechanism involves:
*   A Java client component that initiates requests and sends data.
*   A Python TCP server that listens for these requests, processes them, executes Python code, and returns results.
*   A defined communication protocol (often involving JSON or serialized data) for message exchange.

This setup enables SEMOSS to leverage Python's rich ecosystem of libraries for data science, machine learning, and other specialized tasks, while managing the overall workflow and user interaction from the Java backend.

## 2. Java-Side Components

On the Java side, several classes collaborate to manage the Python process, establish communication, and send commands or data.

### 2.1. `prerna.ds.py.PyTranslator`

*   **Role**: This class is the primary high-level Java client for interacting with the Python TCP server. It provides a simpler API for other Java components (like `PandasFrame`, Python-specific reactors, or services needing Python execution) to run Python code and exchange data, abstracting the direct complexities of socket communication and data serialization.
*   **Functionality**:
    *   **Script Execution**:
        *   `runScript(String script)`: Sends a Python script string directly to the Python server for execution and retrieves the result. This is suitable for short, self-contained scripts or commands.
        *   `runEmptyPy(String... scriptLines)`: For more complex or multi-line scripts, this method writes the script lines to a temporary `.py` file (usually within the insight's `Py/Temp/` directory). It then instructs the Python server (via a command like `smssutil.run_empty_wrapper(filePath, globals())`) to execute this script file. The result is not directly captured from stdout by this method; it's for scripts that perform actions without a direct return value to Java or that write their own output.
        *   `runPyAndReturnOutput(String... scriptLines)`: Similar to `runEmptyPy`, it writes the script to a file. However, it also tells the Python wrapper (`smssutil.runwrapper`) to redirect Python's output (stdout/stderr) to a temporary text file. `PyTranslator` then reads this file to get the script's output. It also handles replacing absolute insight paths in the output with generic variables like `$IF`.
        *   `runSingle(String script, Insight insight)`: A synchronized method that sets up insight-specific paths (`ROOT`, `APP_ROOT`, `USER_ROOT`) as global variables in the Python environment before executing the provided script (via a temporary file and `smssutil.runwrapper_eval`). This is useful when Python scripts need to be aware of the current insight's file system context.
    *   **Data Type Conversion**: Provides `convertDataType(String pDataType)` to map Python data type strings (e.g., "int64", "float64") to SEMOSS's internal `SemossDataType` enum.
    *   **Socket Client Usage**: It holds a reference to a `prerna.tcp.client.SocketClient` instance (obtained from `ClientProcessWrapper`) and uses it to send `PayloadStruct` objects (with `OPERATION.PYTHON`) to the Python server.
    *   **Path Management**: Manages temporary file creation for scripts and outputs, often within the context of an `Insight`'s working directory.

### 2.2. Java TCP Client/Socket Management

*   **`prerna.om.ClientProcessWrapper`**:
    *   **Role**: This class is responsible for the lifecycle management of the external Python TCP server process and the Java-side `SocketClient` that connects to it. An instance of `ClientProcessWrapper` is often associated with a specific Python environment or a long-running Python service required by SEMOSS (e.g., for vector database operations or other persistent Python backends).
    *   **Functionality**:
        *   `createProcessAndClient(...)`:
            *   Determines an available network port using `prerna.util.PortAllocator`.
            *   Launches the Python TCP server as an external process. It can start a "native" Python server (e.g., `gaas_tcp_socket_server.py` via `prerna.util.Utility.startTCPServerNativePy`) or potentially a generic Java-based TCP server if `nativePyServer` is false (though native Python is typical for this use case).
            *   Supports starting the Python process within a chroot environment for isolation if a `SymlinkHelper` is provided.
            *   Configures logging for the Python server process by writing a `log4j.properties` file to its working directory.
            *   Instantiates and connects a `SocketClient` (either `prerna.tcp.client.NativePySocketClient` or `prerna.tcp.client.SocketClient`) to the newly started Python server.
        *   `shutdown(boolean cleanUpFolder)`: Gracefully stops the `SocketClient` (sending a shutdown command to the Python server) and terminates the external Python process. It can also optionally delete the temporary server directory.
        *   `reconnect()`: Provides a mechanism to restart the Python server process and re-establish the client connection if it's lost.
        *   Provides access to the managed `SocketClient` instance, which `PyTranslator` then uses for communication.

*   **`prerna.tcp.client.SocketClient` / `prerna.tcp.client.NativePySocketClient`**:
    *   **Role**: These classes handle the low-level TCP/IP socket communication with the Python server.
    *   **Functionality**:
        *   Establishing and maintaining the socket connection.
        *   Sending `PayloadStruct` objects (which encapsulate commands and data) to the Python server.
        *   Receiving `PayloadStruct` responses from the Python server.
        *   Handling I/O streams, message serialization/deserialization (likely of the `PayloadStruct` itself), and basic error detection on the communication channel.
        *   `NativePySocketClient` might have specific optimizations or handling for Python communication compared to a generic `SocketClient`.

In essence, `ClientProcessWrapper` sets up and tears down the entire Python server environment and the basic communication line, while `PyTranslator` uses that line to conduct specific conversations (i.e., send scripts and data) with Python.

## 3. Python-Side Components (`py/` folder)

The Python side consists of a TCP server that listens for requests from Java, a handler to process these requests, and a mechanism for Python to call back into Java if needed.

### 3.1. `gaas_tcp_socket_server.py`

*   **Role**: This script implements the main Python TCP server. It's responsible for listening for incoming connections from Java clients (like `PyTranslator` via `ClientProcessWrapper`).
*   **Functionality**:
    *   It uses the standard Python `socketserver.ThreadingTCPServer` to handle multiple client connections concurrently, each in its own thread.
    *   It's launched by the Java `ClientProcessWrapper`, receiving parameters like port, working directories (`py_folder`, `insight_folder`), and timeout settings via command-line arguments.
    *   For each incoming connection, it instantiates `gaas_tcp_server_handler.TCPServerHandler` to manage the communication with that specific Java client.
    *   Includes a timeout mechanism to shut down the server if it's idle (no active connections) for a specified period.
    *   Supports running in a `chroot` environment for enhanced security and isolation, based on parameters passed from Java.
    *   **Chroot Jail and Security**:
        *   The server can be started within a `chroot` jail if the `userChrootFolder` parameter is provided by the Java `ClientProcessWrapper`.
        *   **What is `chroot`?**: `chroot` (change root) is a Unix/Linux system call that changes the apparent root directory for the current running process and its children. This means the process cannot "see" or access files outside of this designated directory tree, effectively creating a sandboxed environment.
        *   **Purpose in SEMOSS**: This is primarily a security measure to isolate the Python server process. It limits the Python script's file system access to only the specified chroot directory, preventing it from potentially accessing or modifying sensitive system files outside its intended scope.
        *   **`prerna.util.SymlinkHelper`**: When `ClientProcessWrapper` starts the Python server in chroot mode (e.g., via `prerna.util.Utility.startTCPServerNativePyChroot`), it often utilizes `prerna.util.SymlinkHelper`.
            *   The `SymlinkHelper` is responsible for creating necessary symbolic links *within* the chroot jail. These symlinks might point to:
                *   Required Python libraries or packages.
                *   SEMOSS's own Python utility scripts (like those in the `py/` directory that the server needs).
                *   Specific insight or asset folders that the Python script needs to access for its operations.
            *   Without these symlinks, the chrooted Python process, being confined to its new root, wouldn't be able to find and load these essential dependencies.
        *   **Docker Context**: While Docker itself provides containerization (a stronger form of isolation), if SEMOSS is run within a Docker container, the `chroot` mechanism for the Python server can provide an additional layer of defense-in-depth, especially if the Python server handles code or data from potentially less trusted sources or if multiple users/tenants might indirectly cause Python scripts to execute. The Dockerfile might set up a base file system, and `chroot` further restricts specific Python server instances within that.
        *   The `gaas_tcp_socket_server.py` script itself performs `os.chroot(args.userChrootFolder)` and `os.chdir("/")` if the `userChrootFolder` argument is provided.

### 3.2. `gaas_tcp_server_handler.py`

*   **Role**: The `TCPServerHandler` class is the core request processing unit on the Python side. An instance is created for each connected Java client.
*   **Functionality**:
    *   **Message Handling**:
        *   In its `handle()` method, it continuously listens for messages from the Java client.
        *   It reads messages prefixed with their size (4 bytes) and an "epoc" ID (20 bytes, a unique request identifier generated by Java).
        *   Deserializes the message payload (typically JSON) into a Python dictionary. This dictionary structure mirrors the Java `PayloadStruct`.
    *   **Python Execution (`handle_python`)**:
        *   If the received payload's `operation` is "PYTHON", it extracts the Python script/command.
        *   It uses a `semoss_console.SemossConsole` instance to capture `stdout` and `stderr` from the executed Python code. This console streams captured output back to Java as "interim" messages.
        *   It executes the Python command using `eval()` or `exec()`. It includes a custom `execute_and_capture` method to try and return the value of the last expression in a script, similar to a Jupyter notebook cell.
        *   The final result of the execution (or any exception traceback) is then sent back to Java as a "response" message.
    *   **Response Handling (`handle_response`)**: If the incoming message from Java is a response to a request *initiated by Python* (via `gaas_server_proxy.py`), this method is triggered. It uses a `threading.Condition` object (stored in a shared `monitors` dictionary, keyed by the original `epoc`) to wake up the Python thread that made the callback to Java and deliver the response.
    *   **Output Formatting (`send_output`)**: Packages Python results (or errors) into a JSON payload (again, mirroring `PayloadStruct`), prefixes it with size and the original `epoc`, and sends it back over the socket to the Java client.
    *   **Shell Command Execution (`handle_shell`)**: Contains limited, experimental functionality to execute shell commands like `cd`, `ls`, `git` within a sandboxed environment related to the `insight_folder`. This is not the primary purpose of the server.

### 3.3. `gaas_server_proxy.py`

*   **Role**: This script provides the `ServerProxy` class, which enables Python code running within the `TCPServerHandler` to make calls *back* to the Java backend. This is crucial for scenarios where Python needs to leverage Java-side functionalities (e.g., query a SEMOSS engine, execute a Pixel script).
*   **Functionality**:
    *   **`comm(...)` method**:
        *   Constructs a request payload (dictionary) similar to the one Java sends to Python. This payload includes an `epoc` (a new unique ID for this Python-to-Java request), `engineType`, `engineId`, `methodName`, arguments (`payload`), argument types (`payloadClassNames`), `insightId`, and an `operation` (e.g., "REACTOR" or "ENGINE").
        *   It registers a `threading.Condition` in the `TCPServerHandler`'s `monitors` dictionary, associated with the `epoc` of this outgoing request.
        *   It then uses the `TCPServerHandler`'s `send_request()` method to send this payload to the connected Java client.
        *   The Python thread then calls `wait()` on the `Condition`, pausing until Java sends back a response with the same `epoc`.
    *   **`callReactor(...)` and `callEngine(...)`**: These are higher-level methods that simplify making specific types of calls (executing a Pixel reactor or an IEngine method) to Java by wrapping the `comm()` method. They manage the thread creation and waiting for the response.

## 4. Communication Protocol and Data Exchange

The communication between Java and Python relies on a TCP socket connection and a JSON-based message protocol.

*   **Message Structure**:
    *   Each message (in both directions) is prefixed with:
        1.  **Size (4 bytes)**: An integer indicating the length of the subsequent JSON payload, sent in big-endian byte order.
        2.  **Epoc (20 bytes)**: A string representing a unique identifier for the request. For responses, this `epoc` matches the `epoc` of the original request.
    *   The main part of the message is a **JSON string**, which, when deserialized, typically corresponds to the structure of Java's `prerna.tcp.PayloadStruct`. This structure generally includes:
        *   `epoc`: The unique request/response identifier.
        *   `payload`: A list containing the actual data or commands. For example, for a Python command, `payload[0]` would be the script string. For results, it would contain the Python output.
        *   `operation`: A string indicating the type of operation (e.g., "PYTHON", "CMD", "REACTOR", "ENGINE", "STDOUT").
        *   `response`: A boolean flag, true if the message is a response to a previous request.
        *   `interim`: A boolean flag, true if the message is a partial/streamed output (like stdout from Python).
        *   `ex`: Contains error/exception details if an error occurred.
        *   Other fields for specific operations (e.g., `insightId`, `methodName`, `engineId` for callbacks from Python to Java).

*   **Data Serialization**:
    *   **Java to Python**:
        *   `PyTranslator` serializes the `PayloadStruct` into a JSON string.
        *   Data intended for Python (e.g., for creating Pandas DataFrames) is often passed as strings within the script itself or by instructing Python to read from files prepared by Java.
    *   **Python to Java**:
        *   `TCPServerHandler` uses `json.dumps` (or `jsonpickle.encode`) to serialize Python objects (dictionaries, lists, primitive types, or custom objects with appropriate handlers) into a JSON string for the `payload` field of the response message.
        *   Pandas DataFrames might be converted to a dictionary format (e.g., `to_dict(orient="split")`) before JSON serialization. Special handling for `NaN` values (converted to "NaN" string) and `datetime64` (converted to string) exists.

*   **Error Handling**:
    *   Exceptions occurring during Python script execution are caught by `TCPServerHandler`. The traceback is converted to a string and placed in the `ex` field of the response payload sent back to Java.
    *   Java's `PyTranslator` checks the `ex` field in the received `PayloadStruct` and throws a `SemossPixelException` if an error is present.

*   **Streaming Output (Stdout/Stderr)**:
    *   The `semoss_console.SemossConsole` class in Python captures `stdout` and `stderr`.
    *   It sends this captured output back to Java via `TCPServerHandler.send_output()` with `operation="STDOUT"` and `interim=True`.
    *   The Java side (`SocketClient` or a listener) receives these interim messages and can process them (e.g., log them or display them in a console). A final "D.O.N.E" marker in the stream often indicates the end of stdout/stderr for a command.

This bidirectional JSON-over-TCP protocol allows SEMOSS to integrate Java and Python execution environments effectively, enabling complex workflows that leverage the strengths of both languages.
