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
        *   Core Reactor Interfaces/Classes (`IReactor.java`, `AbstractReactor.java`)
        *   Key Reactor Examples and Roles
    *   Engine Abstraction (`src/prerna/engine`)
        *   `IEngine.java`: The Core Engine Interface
        *   Engine Implementations (`src/prerna/engine/impl/`) - Including DATABASE, STORAGE, MODEL, VECTOR, FUNCTION, and PROJECT engines.
        *   Engine Management
    *   Data Source Layer (`src/prerna/ds`)
        *   `ITableDataFrame` and `AbstractTableDataFrame`
        *   Key Frame Implementations (`TinkerFrame`, `H2Frame`, `NativeFrame`, `PandasFrame`, `RDataTable`, `SparkDataFrame`)
        *   Interaction with `IEngine` and `QueryStruct`
    *   Authentication and Authorization (`src/prerna/auth`)
    *   High-Level API Request Flow

3.  **[Java Interaction with SEMOSS Internal Databases](./02_java_databases.md)**
    *   Overview of Internal Databases
    *   `LocalMasterDatabase` (Metadata for projects, engines, insights)
    *   `PromptDatabase` (Storage for LLM prompts)
    *   `ModelInferenceLogsDatabase` (Tracking LLM interactions and usage)
    *   Database Configuration and Access

4.  **[Java Configuration and Build Environment](./03_java_configuration_environment.md)**
    *   Core Configuration Files
        *   `config.properties`
        *   `RDF_Map.prop`
        *   `log4j2.properties`
        *   `social.properties` (SSO and Email Configuration)
    *   Build Environment: `pom.xml` (Maven)
    *   Deployment Environment (Briefly: Docker, Tomcat)

5.  **[Java Developer Onboarding Guide](./04_java_developer_onboarding.md)**
    *   Getting Started (Prerequisites, Setup, Build)
    *   Running SEMOSS Locally
    *   Key Java Modules/Packages to Understand First
    *   Contribution Guidelines (Git Workflow, Coding Conventions, Testing, Documentation)

```
