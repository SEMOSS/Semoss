# `PROJECT` Engines

Engines of the `PROJECT` catalog type in SEMOSS have a special role. Unlike other engine types that connect to external data sources or services, a `PROJECT` engine typically represents a SEMOSS project itself. It acts as a container, an organizational unit, or a high-level orchestrator for other assets like data engines, model engines, insights, and datasets that constitute a specific analytical endeavor or application built within SEMOSS.

## Core Concepts for Project Engines

*   **Purpose**: To encapsulate and manage the metadata and constituent parts of a SEMOSS project.
*   **Interaction**: They don't usually involve direct data querying in the way database engines do. Instead, interactions might involve:
    *   Listing the assets (engines, insights, files) associated with the project.
    *   Managing project-level settings or metadata.
    *   Potentially, providing access to a project-specific context or environment.
*   **`IEngine` Implementation**: They implement the basic `IEngine` interface for consistency in how SEMOSS manages all engine types (e.g., for loading via SMSS, identification, and security).

## Example Implementation

### `prerna.engine.impl.app.AppEngine`
*   **Purpose**: This is the primary implementation for a `PROJECT` engine. It represents a SEMOSS "Project" (which in some older contexts or internal code might be referred to as an "App").
*   **Implementation Highlights**:
    *   Its `open()` method loads the project's metadata from its `.smss` file. This metadata includes the project's ID, name, and potentially references to other engines (database, model, storage, etc.) that are part of this project, as well as insights and other assets.
    *   It likely interacts heavily with `prerna.auth.utils.SecurityProjectUtils` to retrieve and manage project-specific metadata and user permissions related to the project.
    *   It might also use `prerna.masterdatabase.utility.MasterDatabaseUtility` if project asset lists are stored centrally.
    *   Methods on this engine would allow other parts of SEMOSS to discover the components of the project (e.g., "list engines in this project", "list insights in this project").
*   **SMSS Configuration**: The `.smss` file for an `AppEngine` is critical and typically contains:
    *   `PROJECT_ID` (or `ENGINE` if following the general engine pattern): The unique ID of the project.
    *   `PROJECT_NAME` (or `ENGINE_ALIAS`): The user-friendly name of the project.
    *   Paths or references to the SMSS files of other engines that are scoped or associated with this project.
    *   Information about user and group access permissions for the project itself.
    *   Version information for the project.
    *   Other project-specific settings or metadata.

While not directly involved in data processing like other engine types, `AppEngine` (and the `PROJECT` catalog type) is fundamental to how SEMOSS organizes, secures, and manages collections of analytical assets.
```
