# SEMOSS Backend Documentation

Welcome to the SEMOSS Backend Documentation. This collection of documents aims to provide developers with a comprehensive understanding of the SEMOSS platform's architecture, core concepts, services, engine integrations, and development practices.

## Table of Contents

### I. Getting Started

1.  **[Introduction to SEMOSS](./00_introduction_to_semoss.md)**
    *   What is SEMOSS?
    *   Core Capabilities
2.  **[Backend Architecture Overview](./01_backend_architecture_overview.md)**
    *   Key Backend Components
    *   Data Storage and Interaction
    *   High-Level API Request Flow

### II. Core Backend Concepts

1.  **[Pixel: The SEMOSS Query Language](./concepts/pixel_language.md)**
    *   Core Purposes, Syntax Basics, Data Types, Control Flow, Execution Model
    *   Pixel and Reactor Interaction
2.  **[Reactor Framework](./concepts/reactor_framework.md)**
    *   Core Interfaces/Classes (`IReactor.java`, `AbstractReactor.java`)
    *   Key Reactor Examples and Roles
    *   Reactor Inputs and `NounStore`
    *   Reactor Outputs and `NounMetadata`
    *   Reactor Results and UI Interaction
3.  **[Engine Abstraction in SEMOSS](./concepts/engine_abstraction.md)**
    *   The `IEngine` Interface
    *   Engine Management (Configuration, Registration, Instantiation, Usage)
4.  **[DataFrames and QueryStructs](./concepts/data_frames_and_query_struct.md)**
    *   `ITableDataFrame` and `AbstractTableDataFrame`
    *   Key Frame Implementations
    *   `SelectQueryStruct` Overview
    *   Interaction between Engines and DataFrames
5.  **[The Insight Object](./concepts/insight_object.md)**
    *   Purpose and Key Responsibilities
    *   Key Information Held
    *   How Insights are Used

### III. Engine Implementations Deep Dive

(Located in the `./engines/` directory)

1.  **[`DATABASE` Engines](./engines/database_engines.md)**
    *   Core Concepts, `SelectQueryStruct` Deep Dive, Examples, Extension Guide
2.  **[`STORAGE` Engines](./engines/storage_engines.md)**
    *   Core Concepts, Examples, Extension Guide
3.  **[`MODEL` Engines](./engines/model_engines.md)**
    *   Core Concepts, Examples, Extension Guide
4.  **[`VECTOR` Engines](./engines/vector_engines.md)**
    *   Core Concepts, Document Ingestion Flow, Custom Processors, Examples, Extension Guide
5.  **[`FUNCTION` Engines](./engines/function_engines.md)**
    *   Core Concepts, Examples, Extension Guide
6.  **[`PROJECT` Engines](./engines/project_engines.md)**

### IV. Platform Services

1.  **[SEMOSS Internal Databases](./platform_services/internal_databases.md)**
    *   Overview, `LocalMasterDatabase`, `PromptDatabase`, `ModelInferenceLogsDatabase`, Configuration/Access.
2.  **[Authentication and Authorization](./platform_services/authentication_and_authorization.md)**
    *   Core Security Objects, AuthN/AuthZ Flows, Key Utilities.
3.  **[Java-Python Communication](./platform_services/java_python_communication.md)**
    *   Overview & Visual Flow Placeholder
    *   Java-Side Components
    *   Python-Side Components (including chroot & SymlinkHelper details)
    *   Communication Protocol

### V. Integrations

This section describes how SEMOSS interacts with other related systems or components.

- [**SEMOSS Monolith Interaction**](./integrations/monolith_interaction.md): Explains the relationship and data flow between the core SEMOSS platform and the `semoss/monolith` web application, including details on the `runPixel` API endpoint.

### VI. Deployment and CI/CD

1.  **[Overview of SEMOSS in Cloud/Cluster Environments](./cloud_and_cluster/README.md)**
    *   Rationale: Scalability, High Availability, Shared Access
    *   Key Components: Central Storage, ZooKeeper Synchronization
2.  **[Centralized Cloud Storage for Assets](./cloud_and_cluster/central_cloud_storage.md)**
    *   `CentralCloudStorage.java`: Purpose and Design
    *   Storage Backend Abstraction (S3, Azure, GCP, etc.)
    *   Asset Synchronization (Engines, Projects, Insights)
    *   Locking and Coordination
3.  **[Cluster Synchronization with ZooKeeper](./cloud_and_cluster/cluster_synchronization.md)**
    *   `ClusterUtil.java` and `ClusterSynchronizer.java`
    *   Role of ZooKeeper: Change Notification, State Coordination
    *   Synchronization Workflow and Configuration
4.  **[Docker Deployment Guide](./cloud_and_cluster/docker_deployment.md)**
    *   Introduction to Dockerized SEMOSS
    *   Details on Core Dockerfiles
    *   Build Process, Environment Variables, Volume Mounts, Example Usage
5.  **[Docker Configuration Details](./deployment/docker_configuration.md)**
    *   Configuring SEMOSS via Docker, `semoss-artifacts`, Environment Variables.
6.  **[GitHub Actions Workflows for CI/CD](./cloud_and_cluster/github_actions_workflows.md)**
    *   CI, Python/Tomcat Builders, Application Image Builders.

### VII. Python Packages

#### GenAI Client Library (`py/genai_client/`)

The `genai_client` Python package provides a standardized interface for interacting with various generative AI models and services, including text generation, embedding generation, and tokenization.

- **[GenAI Client Package Documentation](./python_genai_client/README.md)**
    - Details on:
        - Core Files (`constants.py`, `utils.py`)
        - [Model Keys](./python_genai_client/model_keys.md) (`ModelKeysEnum`)
        - [Model Limits](./python_genai_client/model_limits.md) (Context Windows, Max Tokens)
        - [Client Initializers & Wrappers](./python_genai_client/clients_overview.md)
        - [Embedders Overview](./python_genai_client/embedders_overview.md)
        - [Text Generation Clients Overview](./python_genai_client/text_generation_overview.md)
        - [Tokenizers Overview](./python_genai_client/tokenizers_overview.md)

#### GAAS (Generative AI Agent Services) Tools & Components

This collection of Python modules provides the tools and underlying server infrastructure for SEMOSS's Generative AI Agent Services. It includes engines for database, model, function, storage, and vector interactions, as well as server components and security utilities.

- [**GAAS Tools & Components Documentation**](./python_gaas_tools/README.md)

### VIII. Development Guides

*   **[Development Guides](./development_guides/README.md)**
    *   Overview of configurations, onboarding, etc.
```
