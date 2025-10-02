# Java Developer Onboarding Guide

Welcome to SEMOSS development! This guide provides essential information for new Java developers joining the project.

## 1. Getting Started

### 1.1. Prerequisites

*   **Java Development Kit (JDK)**: SEMOSS Java backend is built using Java. Check the `pom.xml` file for the specific Java version required (e.g., `<maven.compiler.source>1.8</maven.compiler.source>` would indicate Java 8). Ensure you have a compatible JDK installed.
*   **Apache Maven**: Maven is used for building the Java project, managing dependencies, and running tests. Install Maven from [maven.apache.org](https://maven.apache.org/).
*   **Integrated Development Environment (IDE)**: An IDE like Eclipse, IntelliJ IDEA, or VS Code with Java support is highly recommended.
*   **Git**: For version control.

### 1.2. Setting up the Development Environment

1.  **Clone the Repository**:
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```
2.  **Import into IDE**:
    *   Import the project into your IDE as a Maven project. The IDE should automatically detect the `pom.xml` and set up the project structure and dependencies.
3.  **Build the Project**:
    *   Open a terminal or use your IDE's Maven integration to build the project:
        ```bash
        mvn clean install
        ```
    *   To skip tests during the build (e.g., for a quicker build, though running tests is generally recommended):
        ```bash
        mvn clean install -DskipTests
        ```
    *   A successful build will compile all Java classes and download necessary dependencies, typically producing a `.war` file in the `target/` directory (if the packaging is WAR).

### 1.3. Running SEMOSS Locally

*   **Tomcat Setup**: The Java backend is a web application typically deployed on Apache Tomcat.
    *   Download and install Apache Tomcat (a version compatible with the Servlet API version specified in `pom.xml`).
    *   Configure your IDE to deploy the built SEMOSS `.war` file to your Tomcat server, or manually deploy it to Tomcat's `webapps` directory.
*   **Configuration**:
    *   Ensure necessary configuration files like `RDF_Map.prop` and `config.properties` are correctly set up in the appropriate location for your local deployment (often the root of the web application or a directory specified by Tomcat).
    *   Internal databases (H2) will typically be created in the `db/` directory within your SEMOSS home/working directory upon first run if they don't exist.
*   **Start Tomcat**: Start your Tomcat server. SEMOSS should be accessible via `http://localhost:<tomcat_port>/<semoss_context_path>`.

### 1.4. Key Java Modules/Packages to Understand First

To get up to speed with the Java backend, focus on these core packages within `src/prerna/`:

*   **`prerna.sablecc2`**: Understands how Pixel scripts (the core language of SEMOSS) are parsed and the initial stages of execution. (See `docs/01_java_backend.md#1-pixel-the-semoss-query-language--execution-engine-srcprernasablecc2`)
*   **`prerna.reactor`**: This is the heart of SEMOSS's command execution. Learn about `IReactor`, `AbstractReactor`, and how different reactors implement specific functionalities. (See `docs/01_java_backend.md#2-reactor-framework-srcprernareactor`)
*   **`prerna.engine`**: Focus on `IEngine` and the various implementations in `prerna.engine.impl`. This will help you understand how SEMOSS connects to and interacts with different data sources and services. (See `docs/01_java_backend.md#3-engine-abstraction-srcprernaengine`)
*   **`prerna.ds`**: Learn about `ITableDataFrame`, `AbstractTableDataFrame`, `QueryStruct`, and specific frame implementations like `TinkerFrame` or `H2Frame`. This is key to understanding how data is represented and manipulated in memory. (See `docs/01_java_backend.md#4-data-source-layer-srcprernads`)
*   **`prerna.auth`**: Familiarize yourself with `User`, `AccessToken`, `AuthProvider`, and the utility classes in `prerna.auth.utils` to understand security and permissions. (See `docs/01_java_backend.md#5-authentication-and-authorization-srcprernaauth`)

## 2. Contribution Guidelines

### 2.1. Git Workflow

*   Follow standard Git practices: create feature branches, commit regularly with clear messages, and create Pull Requests (PRs) for review.
*   Refer to the Pull Request template at `.github/PULL_REQUEST_TEMPLATE.md`.
*   Pay attention to any commit message guidelines outlined in `hooks/README.md`.

### 2.2. Coding Conventions

*   **Code Style**: While not explicitly documented, try to follow the existing code style in the modules you are working on.
*   **PMD**: SEMOSS uses PMD for static code analysis. The rules are defined in `pmd.pmd` (at the root of the project). It's good practice to run PMD checks locally or ensure your code doesn't introduce new PMD violations. Your IDE might have a PMD plugin, or you can run it via Maven (`mvn pmd:check`).

### 2.3. Testing

*   **Unit Tests**: Java unit tests are primarily located in the `test/src/prerna/` directory (and potentially `test_legacy/src/prerna/` for older tests).
*   **Running Tests**:
    *   Use Maven to run all tests:
        ```bash
        mvn test
        ```
    *   Run tests for a specific class or method using your IDE's test runner.
*   **Writing Tests**: When adding new features or fixing bugs, write corresponding unit tests to ensure correctness and prevent regressions.

### 2.4. Documentation

*   For significant changes or new modules, update or create relevant documentation in the `docs/` directory.

By following these guidelines and exploring the key packages, you'll be well on your way to contributing effectively to the SEMOSS Java backend.
