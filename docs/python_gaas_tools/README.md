# GAAS (Generative AI Agent Services) Python Tools

This directory contains documentation for the Python tools used by the Generative AI Agent Services (GAAS) within the SEMOSS platform. These tools provide various functionalities that enable AI agents to interact with data, execute code, and access knowledge.

## Overview

The GAAS tools are designed to be modular components that can be invoked by an AI agent or orchestration layer to perform specific tasks. They often leverage the `genai_client` package for interactions with large language models and other AI services.

## Available Tools and Components

The following documents detail the specific GAAS tools and server components:

- **[Database Interaction (`gaas_gpt_database.py`)]](./gaas_database.md)**: Describes the `DatabaseEngine` class, a Python proxy for executing queries and data operations on SEMOSS database engines.
- **[Model Interaction (`gaas_gpt_model.py`)]](./gaas_model.md)**: Details the core component for interacting with generative models for various tasks.
- **[Function Execution (`gaas_gpt_function.py`)]](./gaas_function.md)**: Details the `FunctionEngine` class, a Python proxy for executing pre-defined SEMOSS `FUNCTION` engines.
- **[Storage Access (`gaas_gpt_storage.py`)]](./gaas_storage.md)**: Details the `StorageEngine` class, a Python proxy for performing operations on SEMOSS `STORAGE` engines.
- **[Vector Database (`gaas_gpt_vector.py`)]](./gaas_vector.md)**: Details the `VectorEngine` class, a Python proxy for adding documents to and performing similarity searches on SEMOSS `VECTOR` engines.

Additional GAAS components include:
- [**Prompt Guard (`gaas_prompt_guard.py`)**](./gaas_prompt_guard.md): For input/output validation and security.
- [**REST Client for SEMOSS API (`gaas_rest_server.py`)**](./gaas_rest_server.md): A Python client to interact with the SEMOSS backend REST API.
- [**Server Proxy (`gaas_server_proxy.py`)**](./gaas_server_proxy.md): Enables Python GAAS components to call back to the SEMOSS Java backend.
- [**TCP Servers (`gaas_tcp_server_handler.py`, `gaas_tcp_socket_server.py`)**](./gaas_tcp_servers.md): Components for TCP-based communication.

*(Note: Links will become active as documentation for each component is completed.)*

## Integration

These tools are typically orchestrated by a higher-level agent or service that determines which tool to use based on the user's request or a predefined workflow. Understanding these tools is key to extending the capabilities of SEMOSS's AI agents.
