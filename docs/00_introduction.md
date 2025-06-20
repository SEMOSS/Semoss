# Introduction to SEMOSS

## What is SEMOSS?

SEMOSS (Semantic Open Source Software) is a versatile web application designed for building and deploying custom data-driven solutions. It provides a lightweight yet powerful framework that facilitates connectivity to a wide array of data sources, including:

*   Relational databases
*   RDF triple stores (semantic web databases)
*   Vector stores for AI/ML applications
*   Large Language Models (LLMs)
*   Cloud and local storage providers
*   Custom functions and APIs

SEMOSS features a robust Business Intelligence (BI) layer, enabling users to:

*   Explore complex data relationships.
*   Develop tailored data products and dashboards.
*   Execute custom algorithms for advanced analytics.
*   Utilize data virtualization capabilities to merge and transform data from disparate sources into a unified analytical view.

Originally conceived as a visualization and analytics tool for RDF data, SEMOSS has significantly evolved into a comprehensive, general-purpose platform catering to diverse data integration and analysis needs.

## High-Level Java Architecture

The Java backend of SEMOSS is the core of the platform, orchestrating data processing, user requests, and interactions with various services. Key Java components include:

*   **Pixel Query Engine:** At the heart of SEMOSS is a powerful engine (leveraging components from `src/prerna/sablecc2`) responsible for parsing and executing "Pixel," a custom query and scripting language. This allows for complex data manipulations and workflow definitions.
*   **Reactor Framework:** SEMOSS employs a Reactor pattern (`src/prerna/reactor`) enabling extensible and modular functionality. Reactors are Java classes that handle specific commands or operations, making it easy to add new features.
*   **Engine Abstraction (`IEngine`):** The `src/prerna/engine/api/IEngine.java` interface provides a standardized way for the core system to interact with various data sources and execution environments (like RDBMS, RDF stores, Python, Spark). Different implementations of `IEngine` handle the specifics of each backend.
*   **Data Source Layer (`src/prerna/ds`):** This layer contains abstractions and wrappers that allow SEMOSS to work seamlessly with different types of data sources, often in conjunction with the `IEngine` implementations.
*   **Authentication and Authorization (`src/prerna/auth`):** Manages user identity, access control, and security policies within the platform.

## Data Storage and Interaction (Java Perspective)

SEMOSS's Java components interact with several types of data stores:

*   **Primary Data Sources:** These are the databases and data stores that users connect to for their analytics, accessed via the `IEngine` interface (e.g., external RDBMS, RDF stores, data lakes).
*   **Internal Databases (`db/`):**
    *   **LocalMasterDatabase:** Likely stores metadata about SEMOSS projects, engines, user insights, and configurations. Java components in `src/prerna/masterdatabase` manage this.
    *   **PromptDatabase:** Used for storing and managing prompts, particularly for interactions with Large Language Models (LLMs), accessible via Java services.
    *   **UserTrackingDatabase:** Logs user activity and system events for auditing and analytics.
    *   **SecurityDatabase:** Stores user credentials, roles, and permissions.
    *   **Other specific-purpose databases:** For caching, themes, etc.

The Java backend reads configurations (e.g., from `config.properties`, `RDF_Map.prop`, and database-specific property files in `db/`) to manage these connections and behaviors.
