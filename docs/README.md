# SEMOSS Java Backend Documentation

Welcome to the documentation for the SEMOSS Java backend. This collection of documents aims to provide developers with an understanding of the architecture, core components, configuration, and development practices for the Java portion of the SEMOSS platform.

## Table of Contents

1.  **[Introduction to SEMOSS](./00_introduction.md)**
    *   What is SEMOSS?
    *   High-Level Java Architecture
    *   Data Storage and Interaction (Java Perspective)

2.  **[Java Backend Deep Dive](./01_java_backend.md)**
    *   Pixel: The SEMOSS Query Language & Execution Engine (`src/prerna/sablecc2`)
    *   Reactor Framework (`src/prerna/reactor`)
    *   Engine Abstraction (`src/prerna/engine`) - Overview of `IEngine` and Engine Management.
    *   Data Source Layer (`src/prerna/ds`)
    *   Authentication and Authorization (`src/prerna/auth`)
    *   High-Level API Request Flow

3.  **[Engine Implementations Deep Dive](./06_engine_implementations.md)**
    *   `DATABASE` Engines (Core Concepts, RDBMS, RDF, Neo4j, JanusGraph examples, Extension Guide)
    *   `STORAGE` Engines (Core Concepts, Local FS, S3 examples, Extension Guide)
    *   `MODEL` Engines (Core Concepts, OpenAI, Bedrock, Embedded examples, Extension Guide)
    *   `VECTOR` Engines (Core Concepts, FAISS, ChromaDB examples, Extension Guide)
    *   `FUNCTION` Engines (Core Concepts, Local Python, REST examples, Extension Guide)
    *   `PROJECT` Engines (AppEngine example)

4.  **[Java Interaction with SEMOSS Internal Databases](./02_java_databases.md)**
    *   Overview of Internal Databases
    *   `LocalMasterDatabase`
    *   `PromptDatabase`
    *   `ModelInferenceLogsDatabase`
    *   Database Configuration and Access

5.  **[Java Configuration and Build Environment](./03_java_configuration_environment.md)**
    *   Core Configuration Files (`config.properties`, `RDF_Map.prop`, `log4j2.properties`, `social.properties`)
    *   Build Environment (`pom.xml`)
    *   Deployment Environment

6.  **[Java Developer Onboarding Guide](./04_java_developer_onboarding.md)**
    *   Getting Started
    *   Running SEMOSS Locally
    *   Key Java Modules
    *   Contribution Guidelines

7.  **[Java to Python Communication](./05_java_python_communication.md)**
    *   Overview & Visual Flow Placeholder
    *   Java-Side Components
    *   Python-Side Components (including chroot & SymlinkHelper details)
    *   Communication Protocol
```
