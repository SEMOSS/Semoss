# SEMOSS Configuration and Environment

This document outlines key configuration files and aspects of the build environment relevant to the SEMOSS Java backend.

## 1. Core Configuration Files

Several properties files control the behavior of the SEMOSS Java backend.

### 1.1. `config.properties` (Root Directory)

*   **Purpose**: This file, intended to be at the root of the SEMOSS deployment (though the provided example might be from a development setup), stores default credentials and connection parameters for various external services and databases that SEMOSS might integrate with.
*   **Key Properties**:
    *   `postgres_host`, `postgres_port`, `postgres_username`, `postgres_password`: Default connection details for PostgreSQL.
    *   `mysql_password`: Default password for MySQL.
    *   `redis_host`, `redis_port`, `redis_password`, `redis_timeout`: Connection details for a Redis server.
    *   `jdbc_password`: A generic JDBC password.
    *   `general_username`, `general_password`: Generic username/password.
    *   `clean_up_apps_reactor_password`: A specific password required for the `CleanUpAppsReactor`, likely as a safety measure for a potentially destructive operation.
    *   `sql_url`, `sql_user`, `sql_password`: Generic SQL connection parameters.
*   **Java Access**: These properties would be loaded by SEMOSS at startup. Java components needing to connect to these services would then retrieve the necessary credentials, potentially falling back to these defaults if specific engine configurations do not provide overrides.

### 1.2. `RDF_Map.prop` (Root Directory)

*   **Purpose**: This is a master configuration file for the SEMOSS Java backend, controlling a wide range of functionalities including engine discovery, runtime behavior for R and Python, default settings, feature flags, UI elements, and RDF processing. It's loaded by `prerna.util.DIHelper` and its properties are accessible throughout the application.
*   **Key Property Groups & Examples**:
    *   **Engine Watchers**:
        *   Defines file system watchers (e.g., `SMSSWebWatcher`, `SMSSStorageWatcher`) that monitor specific directories (`_DIR`) for `.smss` files. When new or updated SMSS files appear, these watchers (Java classes like `prerna.util.SMSSWebWatcher`) can dynamically load or reload the corresponding engines. This allows for hot deployment/update of engine configurations.
        *   Example: `SMSSWebWatcher_DIR=C:\workspace\Semoss\db`, `SMSSWebWatcher_EXT=.smss`, `SMSSWebWatcher_ETYPE=DATABASE`.
    *   **Base Paths & Directories**:
        *   `BaseFolder`: Root directory for SEMOSS assets (e.g., `C:\workspace\Semoss` - typically overridden in production).
        *   `INSIGHT_CACHE_DIR`, `EMAIL_TEMPLATES`: Paths to cache and email template directories.
    *   **R and Python Integration**:
        *   `NETTY_R`, `NETTY_PYTHON`: Flags to enable/disable Netty-based communication for R/Python.
        *   `USE_R`, `USE_PYTHON`: Flags to enable/disable R and Python integration.
        *   `R_CONNECTION_JRI`, `R_MEM_LIMIT`, `PY_WORKER`: Specific settings for how R and Python processes are managed and interacted with.
    *   **Default Behaviors**:
        *   `DEFAULT_FRAME_TYPE`: (e.g., `GRID`) Sets the default frame type for data operations.
        *   `DEFAULT_GRID_TYPE`: (e.g., `H2_DB`) Sets the default backend for grid frames.
        *   `DEFAULT_SCRIPTING_LANGUAGE`: (e.g., `R`) Default language for scripting in analytics.
    *   **Feature Flags**:
        *   `USER_TRACKING_ENABLED`, `MODEL_INFERENCE_LOGS_ENABLED`, `PROMPT_DB_ENABLED`: Toggle various features on or off.
        *   `ADMIN_ONLY_*`: A series of flags (e.g., `ADMIN_ONLY_DB_ADD`, `ADMIN_ONLY_PROJECT_SET_PUBLIC`) to restrict certain administrative actions to users with admin privileges.
    *   **UI & Playsheet Configuration**:
        *   `PLAYSHEETS_DEFINED`: A semicolon-separated list of available playsheet types (e.g., `Grid;Graph;Line`).
        *   `<PlaysheetName> = <JavaClassPath>`: Maps playsheet names to their implementing Java classes (e.g., `Grid=prerna.ui.components.playsheets.GridPlaySheet`).
        *   `<PlaysheetName>_HINT = <HintText>`: Provides hint text for playsheets.
        *   `<PlaysheetName>_DATAMAKER = <JavaClassPath>`: Defines the default data maker for a playsheet.
    *   **RDF & Semantic Web**:
        *   `SEMOSS_URI`: Base URI for SEMOSS-generated RDF resources.
        *   SPARQL query templates (e.g., `NEIGHBORHOOD_OBJECT_QUERY`).
    *   **SQL Keywords**: Lists of reserved SQL keywords for different database dialects (H2, SQLite, MySQL, SQL Server) to aid in query generation and identifier quoting.
    *   **Legacy UI Listeners**: A large section defines listeners and controllers, likely for an older UI framework.
*   **Java Access**: Primarily accessed via `DIHelper.getInstance().getProperty(String key)` after being loaded at startup.

This file is critical for tailoring the SEMOSS environment and enabling/disabling various features.

### 1.3. `log4j2.properties` (Root or `src/` Directory)

*   **Purpose**: Configures the Apache Log4j 2 logging framework for the Java backend. It controls how log messages are generated, filtered, formatted, and where they are output.
*   **Key Settings**:
    *   **Appenders**:
        *   `appender.console.type = Console`: Defines a console appender named `STDOUT` for logging to standard output.
        *   `appender.rolling.type = RollingFile`: Defines a rolling file appender named `ROLLINGFILE`.
            *   `appender.rolling.fileName = logs/semossLog.log`: Specifies the main log file.
            *   `appender.rolling.filePattern = logs/$${date:yyyy-MM-dd}/semossLog-%d{yyyy-MM-dd}-%i.log`: Defines the pattern for archived log files (rolled over daily or when they reach a size limit).
            *   `appender.rolling.policies.size.size = 250MB`: Sets the size for rollover.
            *   `appender.rolling.strategy.max = 1000`: Maximum number of archived log files to keep.
    *   **Layout**:
        *   `appender.console.layout.pattern = [%-5level] %d{yyyy-MM-dd HH:mm:ss} %c{1.}:%L %m%n`: Defines the format for log messages (level, timestamp, class name, line number, message).
    *   **Loggers**:
        *   `logger.app.name = prerna`: Defines a logger specifically for the `prerna` package (SEMOSS's main package).
            *   `logger.app.level = info`: Sets the default log level for `prerna` packages to INFO. Messages at DEBUG level or lower from these packages will not be logged unless overridden.
            *   `logger.app.appenderRef.console.ref = STDOUT` and `logger.app.appenderRef.file.ref = ROLLINGFILE`: Assigns both console and file appenders to this logger.
        *   Specific loggers for sub-packages or external libraries can be defined to control their verbosity independently (e.g., `logger.rdfwrapper.level = info`, `logger.quartz.level = warn`).
    *   **Root Logger**:
        *   `rootLogger.level = warn`: Sets the default log level for all other classes not covered by a specific logger to WARN.
        *   `rootLogger.appenderRef.stdout.ref = STDOUT`: Sends root logger output to the console.
*   **Impact**: Administrators can modify this file to adjust logging levels for troubleshooting (e.g., setting `logger.app.level = debug` for more detailed SEMOSS logs) or to change log output destinations.

### 1.4. `social.properties` (Root Directory)

*   **Purpose**: This file is central to configuring user authentication methods, particularly for Single Sign-On (SSO) with various external identity providers (IdPs), and for email server settings. It dictates which login options are available to users and how SEMOSS interacts with IdPs.
*   **Key Property Groups & Examples**:
    *   **Global Settings**:
        *   `redirect`: The URL to redirect users to after a successful login.
    *   **Login Provider Enablement**:
        *   A series of boolean flags enable or disable specific authentication providers, e.g.:
            *   `native_login=true` (enables built-in username/password authentication)
            *   `google_login=false`
            *   `ldap_login=true`
            *   `siteminder_login`, `adfs_login`, `okta_login`, `cac_login`, etc.
    *   **Native Login Configuration**:
        *   `native_registration=true`: Allows new users to register.
        *   `native_access_keys_allowed=true`: Allows native users to generate API access keys.
        *   `native_display_name`: Text for the native login button.
    *   **External Identity Provider Configuration (OAuth/OIDC/SAML-like)**:
        *   For each enabled external provider (e.g., Google, Microsoft, Okta, ADFS, generic OIDC), a set of properties defines the connection parameters:
            *   `*_client_id`: Client ID obtained from the IdP.
            *   `*_secret_key`: Client secret obtained from the IdP.
            *   `*_redirect_uri`: The URI SEMOSS expects the IdP to redirect to after authentication.
            *   `*_auth_url`: The IdP's authorization endpoint.
            *   `*_token_url`: The IdP's token endpoint.
            *   `*_userinfo_url`: The IdP's user info endpoint (for OIDC).
            *   `*_scope`: The requested scopes (permissions) from the IdP.
            *   `*_auto_add=true`: If true, users successfully authenticated via this IdP are automatically provisioned in SEMOSS's user database.
            *   `*_access_keys_allowed=false`: Whether users from this IdP can create API keys in SEMOSS.
            *   `*_display_name`: Text for the login button for this provider.
    *   **LDAP Configuration**:
        *   `ldap_provider_url`, `ldap_principal_template`, `ldap_search_context_name`, etc.: Detailed settings for connecting to an LDAP directory for authentication and user attribute lookup.
    *   **Email (SMTP) Server Configuration**:
        *   `smtp_enabled=true`: Enables email sending capabilities.
        *   `smtp_mail.smtp.host`, `smtp_mail.smtp.port`: SMTP server host and port.
        *   `smtp_mail.smtp.auth=true`: Whether authentication is required for the SMTP server.
        *   `smtp_username`, `smtp_password`: Credentials for the SMTP server.
        *   `smtp_sender`: Default "from" address for emails sent by SEMOSS.
*   **Java Access**:
    *   These properties are loaded at application startup, likely by classes in the `prerna.auth` package.
    *   The `prerna.auth.AuthProvider` enum likely maps to the enabled providers.
    *   Authentication servlets or filters use these properties to initiate SSO flows, validate responses from IdPs, and configure email services.
    *   `prerna.util.SocialPropertiesUtil` or similar classes might provide convenient access to these properties.

This file is crucial for integrating SEMOSS into an organization's existing identity infrastructure and for enabling communication features.

## 2. Build Environment: `pom.xml`

The `pom.xml` file at the root of the repository indicates that SEMOSS's Java components are built using Apache Maven.

*   **Project Structure**: Defines the project's group ID, artifact ID, version, and packaging (likely WAR for a web application).
*   **Dependencies**:
    *   Lists all external Java libraries (JAR files) that SEMOSS depends on. This includes libraries for:
        *   Web server functionalities (e.g., Servlets, JSP).
        *   Database connectivity (JDBC drivers for various databases like H2, PostgreSQL, etc.).
        *   RDF processing (e.g., Apache Jena).
        *   Graph databases (e.g., TinkerPop, Neo4j client).
        *   JSON processing (e.g., Gson).
        *   HTTP clients, utility libraries (Apache Commons), security libraries, etc.
    *   Maven automatically downloads and manages these dependencies during the build process.
*   **Build Process**:
    *   Defines build plugins (e.g., for compiling Java code, running tests, packaging the application).
    *   Specifies compiler versions and other build settings.
    *   Commands like `mvn clean install` or `mvn package` are used to compile the code, run tests (if configured), and produce the final deployable artifact (e.g., a `.war` file).
*   **Profiles**: May define different build profiles for various environments (e.g., development, testing, production).
*   **Modules**: If SEMOSS is a multi-module Maven project, the root `pom.xml` would define the sub-modules, each potentially having its own `pom.xml`.

## 3. Deployment Environment (Briefly)

*   **Docker**: The `docker/` directory contains Dockerfiles, suggesting that SEMOSS can be deployed using Docker containers.
    *   `docker/Dockerfile.tomcat`: Likely used to build a Docker image containing Apache Tomcat with the SEMOSS web application deployed.
    *   `docker/Dockerfile.java` (or similar): Might be a base Java image or for specific Java services if any are run separately.
*   **Tomcat**: The primary deployment target for the Java web application seems to be Apache Tomcat, as indicated by typical web application structure and potentially Tomcat-specific configurations if any.

Understanding these configuration files and the Maven build process is essential for developers working on the SEMOSS Java backend, as well as for administrators deploying and managing the application.
