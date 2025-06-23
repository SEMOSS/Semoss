# Authentication and Authorization in SEMOSS

The `src/prerna/auth/` package and its sub-packages are responsible for managing user identity, authentication, and access control throughout the SEMOSS platform.

## Core Security Objects

*   **`User.java`**: Represents an authenticated user within SEMOSS. It typically stores user identifiers (from various authentication providers), profile information, and potentially a collection of their permissions or roles.
*   **`AccessToken.java`**: Represents a security token (e.g., a JWT or an opaque token) issued to a user upon successful authentication. This token is then used to authenticate subsequent API requests and manage user sessions. `ReadOnlyAccessToken` might be a specialized version for read-only operations.
*   **`AuthProvider.java`**: An enumeration that defines the different methods by which a user can be authenticated (e.g., `NATIVE` for users stored in SEMOSS's own database, `LDAP`, `SAML`, `OIDC` for integration with external identity providers).
*   **`AccessPermissionEnum.java`**: Defines the various levels of access rights a user can have on a resource (e.g., `READ_ONLY`, `EDIT`, `OWNER`).

## Authentication Process (High-Level)

1.  **Login Request**: A user initiates a login request, typically providing credentials or being redirected from an external Identity Provider (IdP).
2.  **Provider Determination**: SEMOSS identifies the `AuthProvider` being used for the login attempt.
3.  **Credential Validation**:
    *   For `NATIVE` users, `prerna.auth.utils.SecurityNativeUserUtils` likely handles the validation of credentials against a user store within SEMOSS's security database.
    *   For external providers (LDAP, SAML, OIDC), SEMOSS would interact with the respective IdP according to the protocol's specifications. This might involve validating assertions or tokens provided by the IdP.
4.  **User Object Creation**: Upon successful authentication, a `User` object is created or retrieved, populating it with identity information.
5.  **Token Issuance**: `prerna.auth.utils.SecurityTokenUtils` is responsible for generating an `AccessToken` for the authenticated `User`. This token encapsulates the user's authenticated state.
6.  **Session Management**: The `AccessToken` is used to manage the user's session, typically sent with each subsequent request to the backend.

## Authorization Process (High-Level)

SEMOSS employs a role-based or permission-based access control model, primarily managed through its security database.

1.  **Resource Access Request**: A user, identified by their `AccessToken` and associated `User` object, attempts to access or modify a resource (e.g., an Engine, Project, Insight, or perform a specific action).
2.  **Permission Check**:
    *   Utility classes within `prerna.auth.utils/` are invoked to check permissions. For example:
        *   `SecurityEngineUtils.userCanViewEngine(User user, String engineId)`
        *   `SecurityProjectUtils.userCanEditProject(User user, String projectId)`
    *   These methods query the security database, which stores relationships between users (or groups they belong to) and resources, along with the `AccessPermissionEnum` level granted.
3.  **Decision**: Based on the permissions found in the security database, the system either grants or denies access to the resource or action. Both direct user permissions and permissions derived from group memberships are typically considered.
4.  **External Authorization**: The `prerna.auth.external.ExternalAuthorizationHelper` class suggests that SEMOSS can also integrate with external systems for making authorization decisions, potentially augmenting or overriding its internal permission model.

## Key Utility Packages/Classes

*   **`prerna.auth.utils`**: This package is central to security operations.
    *   `AbstractSecurityUtils`: Provides base functionality and access to the security database (often an H2 database instance).
    *   `SecurityEngineUtils`, `SecurityProjectUtils`, `SecurityInsightUtils`: Manage permissions and metadata for Engines, Projects, and Insights, respectively. They handle storing and retrieving these entities along with their associated user/group permissions from the security database.
    *   `SecurityAdminUtils`: Provides functions for administrative security tasks.
    *   `SecurityGroup*Utils`: A set of classes for managing security groups and their permissions on various resources.

The authentication and authorization mechanisms are designed to be comprehensive, supporting both native user management and integration with enterprise identity systems, while providing granular control over access to SEMOSS resources.
