# Docker Configuration for SEMOSS

This document describes how SEMOSS is configured when running within Docker containers, with a particular focus on the role of the `SEMOSS/semoss-artifacts` repository and the use of environment variables to customize application properties.

## Overview

SEMOSS Docker images, typically built using Dockerfiles like `docker/Dockerfile.tomcat` or `docker/Dockerfile.ubuntu22.04` found in the `semoss/semoss` repository, leverage the `SEMOSS/semoss-artifacts` repository for essential setup and default configurations.

The overall strategy involves:
1.  **Tomcat Environment Setup**: `semoss-artifacts` provides scripts and base configuration files (`server.xml`, `web.xml`, `context.xml`) to set up a standardized Apache Tomcat environment.
2.  **SEMOSS Application Deployment**: The `update_latest_dev.sh` script from `semoss-artifacts` manages the deployment of a specific version of the SEMOSS application (the Monolith WAR and related components). This script also places default application property files into the Docker image.
3.  **Runtime Configuration via Environment Variables**: The SEMOSS Java application (Monolith) is designed to read environment variables at runtime. These environment variables can override the default values present in the property files, allowing for flexible configuration without modifying the core image.

## Role of `SEMOSS/semoss-artifacts`

The `SEMOSS/semoss-artifacts` repository is crucial for:
- **Tomcat Setup**: Supplying scripts like `install_java.sh` (for Java installation) and `config.sh` (for `context.xml` selection), along with standard Tomcat `server.xml` and `web.xml` templates. These are typically found in `semoss-artifacts/artifacts/dockerBuilder_scripts/`.
- **SEMOSS Versioning and Deployment**: The `semoss-artifacts/artifacts/scripts/update_latest_dev.sh` script handles:
    - Determining which version of SEMOSS to deploy. This can be the latest version from Sonatype OSSRH or a specific version defined by the `SEMOSS_VERSION` build-time environment variable.
    - Deploying the SEMOSS application WAR (e.g., `Monolith.war` to `\$TOMCAT_HOME/webapps/Monolith`) and other necessary web components (e.g., to `\$TOMCAT_HOME/webapps/SemossWeb`).
    - Deploying supporting files and directories to `/opt/semosshome/`.
- **Default Application Properties**: Providing the baseline versions of critical SEMOSS application configuration files. These are typically located in the `semoss-artifacts/x/` directory and are copied by `update_latest_dev.sh` to `/opt/semosshome/` within the Docker image. The main property files include:
    - `RDF_Map.prop`
    - `social.properties`
    - `log4j2.properties`

## Configuration via Environment Variables

While `semoss-artifacts` provides the *default* property files, the primary way to customize a SEMOSS Docker deployment for a specific environment is by setting **environment variables** when running the Docker container (e.g., using `docker run -e VARIABLE_NAME=value ...`).

The SEMOSS Java application (running as the Monolith WAR inside Tomcat) reads these environment variables at startup and uses their values to:
- Replace placeholders found in the default property files (e.g., `<AZUREKEY>`, `<GOOGLECLIENTID>`).
- Override default values for properties that don't have explicit placeholders but are designed to be configurable.

**Important Note**: The exact list of all supported environment variables and their precise mapping to internal application properties is ultimately determined by the SEMOSS Java application's source code. The information below is based on analysis of the default property files from `semoss-artifacts/x/` and common conventions. Users should consult official SEMOSS documentation (if available) or the application's source code for a definitive list.

### Potential Environment Variables for `RDF_Map.prop`

The following properties from `RDF_Map.prop` are strong candidates for being overridden by environment variables. Suggested environment variable names are inferred (actual names might vary, possibly prefixed with `SEMOSS_`).

| Property Key                      | Default Value (from `semoss-artifacts/x/RDF_Map.prop`) | Suggested Environment Variable(s)                   | Description                                                                 |
|-----------------------------------|--------------------------------------------------------|-----------------------------------------------------|-----------------------------------------------------------------------------|
| `AZ_KEY`                          | `<AZUREKEY>`                                           | `AZUREKEY`, `SEMOSS_AZ_KEY`                           | Azure Storage Account Key.                                                  |
| `AZ_NAME`                         | `<AZURENAME>`                                          | `AZURENAME`, `SEMOSS_AZ_NAME`                         | Azure Storage Account Name.                                                 |
| `STORAGE`                         | `LOCAL`                                                | `SEMOSS_STORAGE_TYPE`                               | Default storage type (e.g., `LOCAL`, `AZURE`).                              |
| `AZ_CONN_STRING`                  | `DefaultEndpointsProtocol=...;AccountName=<AZURENAME>;AccountKey=<AZUREKEY>` | `SEMOSS_AZ_CONN_STRING`                             | Full Azure Storage connection string (may override individual key/name).    |
| `INSIGHT_CACHE_DIR`               | `/opt/semosshome/InsightCache`                          | `SEMOSS_INSIGHT_CACHE_DIR`                          | Directory for insight caching.                                              |
| `BaseFolder`                      | `/opt/semosshome`                                       | `SEMOSS_BASE_FOLDER`                                | Base directory for SEMOSS operations.                                       |
| `LOG4J`                           | `/opt/semosshome/log4j.prop`                            | `SEMOSS_LOG4J_PATH`                                 | Path to Log4j configuration file.                                           |
| `SOCIAL`                          | `/opt/semosshome/social.properties`                     | `SEMOSS_SOCIAL_PROPERTIES_PATH`                     | Path to social properties file.                                             |
| `EMAIL_TEMPLATES`                 | `/opt/semosshome/emailTemplates/`                       | `SEMOSS_EMAIL_TEMPLATES_DIR`                        | Directory for email templates.                                              |
| `PY_SERVER_USER`                  | (blank)                                                | `PY_SERVER_USER`                                    | User for running the Python server.                                         |
| `PYTHONHOME`                      | `/usr`                                                 | `PYTHONHOME`                                        | Path to Python installation.                                                |
| `PYTHONHOME_SITE_PACKAGES`        | (blank)                                                | `PYTHONHOME_SITE_PACKAGES`                          | Path to Python site-packages.                                               |
| `SCHEDULER_ENDPOINT`              | `http://localhost:8080/Monolith`                       | `SEMOSS_SCHEDULER_ENDPOINT`                         | URL for the scheduler service.                                              |
| `SCHEDULER_KEYSTORE`              | (blank)                                                | `SEMOSS_SCHEDULER_KEYSTORE`                         | Path to scheduler keystore.                                                 |
| `SCHEDULER_KEYSTORE_PASSWORD`     | (blank)                                                | `SEMOSS_SCHEDULER_KEYSTORE_PASSWORD`                | Password for scheduler keystore.                                            |
| `WHITE_LIST_DOMAINS`              | (blank)                                                | `SEMOSS_WHITE_LIST_DOMAINS`                         | Comma-separated list of allowed domains for certain operations.             |
| `LOCAL_DEPLOYMENT`                | `false`                                                | `SEMOSS_LOCAL_DEPLOYMENT`                           | Set to `true` if running locally (e.g., on a laptop).                       |
| `SAMESITE_COOKIE`                 | `none`                                                 | `SEMOSS_SAMESITE_COOKIE`                            | Value for SameSite cookie attribute (`strict`, `lax`, `none`).                |
| `PM_SEMOSS_EXECUTE_SQL_ENCRYPTION_PASSWORD` | `password123`                                  | `SEMOSS_SQL_ENCRYPTION_PASSWORD`                  | Password for SQL execution encryption (sensitive, should be changed).       |
| `USER_MEM_LIMIT`                  | `2` (GB)                                               | `SEMOSS_USER_MEM_LIMIT`                             | Memory limit per user in GB.                                                 |
| `RESERVED_JAVA_MEM`               | `12` (GB)                                              | `SEMOSS_RESERVED_JAVA_MEM`                          | Reserved Java memory in GB.                                                 |
| **Secrets Management**            |                                                        |                                                     |                                                                             |
| `SECRET_STORE_ENABLED`            | `false`                                                | `SEMOSS_SECRET_STORE_ENABLED`                       | Enable/disable secret store usage.                                          |
| `SECRET_STORE_TYPE`               | (blank)                                                | `SEMOSS_SECRET_STORE_TYPE`                          | Type of secret store (e.g., `HASHICORP_VAULT`, `AZURE_KEYVAULT`).           |
| `SECRETS_DB_PATH`                 | (blank)                                                | `SEMOSS_SECRETS_DB_PATH`                            | Base path in secret store for database secrets.                             |
| ... (other `SECRETS_*_PATH`) ...  | (blank)                                                | `SEMOSS_SECRETS_..._PATH`                           | Base paths for other types of secrets.                                      |
| `VAULT_ADDR`                      | (blank)                                                | `VAULT_ADDR`                                        | Address of HashiCorp Vault server.                                          |
| `VAULT_TOKEN`                     | (blank)                                                | `VAULT_TOKEN`                                       | Token for HashiCorp Vault authentication.                                   |
| `AZURE_AUTHENTICATE_MODE`         | (blank)                                                | `AZURE_AUTHENTICATE_MODE`                           | Azure authentication mode for KeyVault.                                     |
| `AZURE_KEYVAULT_NAME`             | (blank)                                                | `AZURE_KEYVAULT_NAME`                               | Name of the Azure KeyVault.                                                 |
| `AZURE_CLIENT_ID`                 | (blank)                                                | `AZURE_CLIENT_ID`                                   | Azure Client ID for KeyVault.                                               |
| `AZURE_CLIENT_SECRET`             | (blank)                                                | `AZURE_CLIENT_SECRET`                               | Azure Client Secret for KeyVault.                                           |
| `AZURE_TENANT_ID`                 | (blank)                                                | `AZURE_TENANT_ID`                                   | Azure Tenant ID for KeyVault.                                               |
| **Virus Scanning**                |                                                        |                                                     |                                                                             |
| `VIRUS_SCANNING_ENABLED`          | `false`                                                | `SEMOSS_VIRUS_SCANNING_ENABLED`                     | Enable/disable virus scanning.                                              |
| `CLAMAV_SCANNING_PORT`            | `3310`                                                 | `SEMOSS_CLAMAV_PORT`                                | Port for ClamAV service.                                                    |
| `CLAMAV_SCANNING_ADDRESS`         | (blank)                                                | `SEMOSS_CLAMAV_ADDRESS`                             | Address of ClamAV service.                                                  |
| **Admin-Only Restrictions**       |                                                        |                                                     |                                                                             |
| `ADMIN_ONLY_PROJECT_ADD`          | `false`                                                | `SEMOSS_ADMIN_ONLY_PROJECT_ADD`                     | Example admin restriction (many others follow this pattern).                |
| **Activity Tracking**             |                                                        |                                                     |                                                                             |
| `USER_TRACKING_ENABLED`           | `false`                                                | `SEMOSS_USER_TRACKING_ENABLED`                      | Enable/disable user activity tracking.                                      |
| `MODEL_INFERENCE_LOGS_ENABLED`    | `false`                                                | `SEMOSS_MODEL_INFERENCE_LOGS_ENABLED`               | Enable/disable model inference logging.                                     |
| `PROMPT_DB_ENABLED`               | `false`                                                | `SEMOSS_PROMPT_DB_ENABLED`                          | Enable/disable prompt database usage.                                       |
| **CHROOT Settings**               |                                                        |                                                     |                                                                             |
| `CHROOT_SUDO`                     | (blank)                                                | `CHROOT_SUDO`                                       | Sudo command for chroot operations.                                         |
| `CHROOT_DEBOOTSTRAP_DIR`          | (blank)                                                | `CHROOT_DEBOOTSTRAP_DIR`                            | Directory for debootstrap.                                                  |
| `CHROOT_DIR`                      | (blank)                                                | `CHROOT_DIR`                                        | Base directory for chroot environments.                                     |
| `CHROOT_ENABLE`                   | (blank)                                                | `CHROOT_ENABLE`                                     | Enable/disable chroot functionality.                                        |
| **External Permission/DB Mgmt**   |                                                        |                                                     |                                                                             |
| `EXTERNAL_PERMISSION_MANAGEMENT_URL` | (blank)                                             | `SEMOSS_EXT_PERM_URL`                               | URL for external permission management service.                             |
| ... (many related properties) ... | (blank)                                                | `SEMOSS_EXT_PERM_...`, `SEMOSS_EXT_DB_...`            | Various settings for external permission and database management services.  |

### Potential Environment Variables for `social.properties`

This file primarily configures authentication providers and SMTP settings. Nearly all properties use `<PLACEHOLDER>` syntax. The environment variable name is likely the placeholder itself (e.g., `NATIVE_ENABLE`, `GOOGLECLIENTID`) or a prefixed version (e.g., `SEMOSS_GOOGLECLIENTID`).

**General:**
- `REDIRECT`: Global redirect URI. (Env: `REDIRECT` or `SEMOSS_REDIRECT`)

**Login Providers (Examples - pattern applies to all providers like GitHub, Microsoft, Okta, LDAP, etc.):**
- `native_login`: `<NATIVE_ENABLE>` (Env: `NATIVE_ENABLE`)
- `native_registration`: `<NATIVE_REGISTRATION_ENABLE>` (Env: `NATIVE_REGISTRATION_ENABLE`)
- `google_login`: `<GOOGLE_ENABLE>` (Env: `GOOGLE_ENABLE`)
- `google_client_id`: `<GOOGLECLIENTID>` (Env: `GOOGLECLIENTID`)
- `google_secret_key`: `<GOOGLESECRETKEY>` (Env: `GOOGLESECRETKEY`)
- `google_redirect_uri`: `<GOOGLEREDIRECT>` (Env: `GOOGLEREDIRECT`)
- `ldap_provider_url`: (blank) (Env: `LDAP_PROVIDER_URL` or `SEMOSS_LDAP_PROVIDER_URL`)
- `ldap_principal_template`: (blank) (Env: `LDAP_PRINCIPAL_TEMPLATE`)
... and so on for all properties of each configured authentication provider.

**SMTP (Email) Settings:**
- `smtp_enabled`: `<SMTPENABLED>` (Env: `SMTPENABLED` or `SEMOSS_SMTP_ENABLED`)
- `smtp_mail.smtp.host`: `<SMTPMAILHOST>` (Env: `SMTPMAILHOST` or `SEMOSS_SMTP_HOST`)
- `smtp_mail.smtp.port`: `<SMTPMAILPORT>` (Env: `SMTPMAILPORT` or `SEMOSS_SMTP_PORT`)
- `smtp_mail.smtp.socketFactory.class`: `<SMTPMAILSOCKETFACTORYCLASS>` (Env: `SMTPMAILSOCKETFACTORYCLASS` or `SEMOSS_SMTP_SOCKET_FACTORY_CLASS`)
- `smtp_mail.smtp.auth`: `<SMTPMAILAUTH>` (Env: `SMTPMAILAUTH` or `SEMOSS_SMTP_AUTH`)
- `smtp_mail.smtp.starttls.enable`: `<SMTPMAILSTARTTLSENABLE>` (Env: `SMTPMAILSTARTTLSENABLE` or `SEMOSS_SMTP_STARTTLS_ENABLE`)
- `smtp_username`: `<SMTPUSERNAME>` (Env: `SMTPUSERNAME` or `SEMOSS_SMTP_USERNAME`)
- `smtp_password`: `<SMTPPASSWORD>` (Env: `SMTPPASSWORD` or `SEMOSS_SMTP_PASSWORD`)
- `smtp_sender`: `<SMTPSENDER>` (Env: `SMTPSENDER` or `SEMOSS_SMTP_SENDER`)

## Dockerfile Integration

The Dockerfiles in the `semoss/semoss` repository (e.g., `docker/Dockerfile.tomcat`, `docker/Dockerfile.ubuntu22.04`) manage the build process:
- They clone the `SEMOSS/semoss-artifacts` repository.
- They execute scripts from `semoss-artifacts` (like `install_java.sh`, `config.sh`, and importantly `update_latest_dev.sh`).
- `update_latest_dev.sh` is responsible for deploying the SEMOSS WAR and the *default* property files (from `semoss-artifacts/x/`) into the image.
- While these Dockerfiles set up system-level environment variables (like `JAVA_HOME`, `PATH`), they generally do **not** explicitly set a wide range of SEMOSS application-specific environment variables using the `ENV` instruction. This is because such variables are intended to be provided at **runtime** using `docker run -e ...`.

## Best Practices for Configuration

- **Use Environment Variables**: For any property listed above, or any other property you need to customize, the recommended approach is to use environment variables when starting your Docker container.
- **Consult SEMOSS Documentation**: For a definitive list of supported environment variables and their exact names, refer to any official SEMOSS documentation or contact SEMOSS support.
- **Secrets Management**: For sensitive values (API keys, passwords), use Docker secrets, HashiCorp Vault, or Azure KeyVault integration if your SEMOSS version and environment support it (as indicated by the `RDF_Map.prop` settings). Pass the necessary tokens or connection details for these secret managers via environment variables.
- **Version Control Defaults**: The default property files in `semoss-artifacts/x/` serve as a good reference for available settings. Avoid hardcoding sensitive information directly in custom Docker images if possible; use environment variables instead.

By understanding this layered approach ( `semoss-artifacts` providing defaults, runtime environment variables providing overrides processed by the application), users can flexibly and securely configure their SEMOSS Docker deployments.
