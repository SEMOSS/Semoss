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

### V. Cloud and Clustered Deployments

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

### VI. Python GenAI Client (`py/genai_client/`)

1.  **[Overview of Python GenAI Client](./python_genai_client/README.md)**
    *   Purpose, Key Sub-modules, Core `constants.py` and `utils.py`
2.  **[Model Keys](./python_genai_client/model_keys.md)** (`ModelKeysEnum`)
3.  **[Model Limits](./python_genai_client/model_limits.md)** (Context Windows, Max Tokens)
4.  **[Clients Overview](./python_genai_client/clients_overview.md)** (`client_initializer.py`, `google_clients.py`)
5.  **[Embedders Overview](./python_genai_client/embedders_overview.md)** (`AbstractEmbedder`, OpenAI, Bedrock examples)
    *   *(Further details for Azure, Local, Textgen, Vertex embedders to be added)*
6.  **Text Generation Overview** *(Placeholder - to be created)*
7.  **Tokenizers Overview** *(Placeholder - to be created)*

### VII. Development Guides

1.  **[SEMOSS Configuration and Environment](./development_guides/configuration_and_environment.md)**
    *   Core Configuration Files, Build Environment (`pom.xml`), Deployment.
2.  **[Java Developer Onboarding Guide](./development_guides/java_developer_onboarding.md)**
    *   Getting Started, Running Locally, Key Modules, Contributions.
```
