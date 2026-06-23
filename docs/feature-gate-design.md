# Feature Gate Design — App-Scoped

## Core Concept

Applications built on SEMOSS can define feature flags scoped to their app. Users are assigned a numeric **version** within each flag. Versions are "buckets" that group users; flags are enabled when a user's version meets or exceeds the flag's `minVersion` threshold, enabling gradual rollout.

```
Application (Project)
  └─ Feature Flags     (e.g. "dark-mode", "new-dashboard")
       └─ Version Buckets   (v0=disabled, v1=beta, v2=release, v3=stable)
            └─ User Assignments per Bucket
                 └─ Evaluated per User at runtime → true / false
```

---

## Version Model

Version is a plain integer assigned per user **per flag per app** (e.g. `1`, `2`, `3`).

- A flag specifies a **minimum version** (`minVersion`) — any user at that version or above sees the flag as enabled.
- A flag specifies a **default version** (`defaultVersion`) — assigned to users with no explicit assignment.
- This keeps comparisons simple: `userAssignedVersion >= minVersion`.
- Version 0 is conventionally "disabled"; unassigned users default to `defaultVersion`.
- **Per-flag scoping prevents data collision**: If flag A is deleted and flag B is created (even with the same key), they have separate version buckets and user assignments.

---

## Data Model

Tables in the **Security DB** (H2/RDB).

```sql
-- Flags defined per application
APP_FEATURE_FLAG (
  FLAG_ID       VARCHAR  PRIMARY KEY,      -- UUID; unique per flag
  APP_ID        VARCHAR,                   -- references existing Project
  FLAG_KEY      VARCHAR,                   -- e.g. "new-dashboard"; human-readable name
  MIN_VERSION   INTEGER,                   -- feature enabled for version >= this
  DEFAULT_VERSION INTEGER,                 -- assigned to users with no explicit version
  CREATED_BY    VARCHAR,
  CREATED_AT    TIMESTAMP
)

-- Version buckets with descriptions; per-flag per-app scoped
APP_VERSION_BUCKET (
  APP_ID      VARCHAR,
  FLAG_ID     VARCHAR,                     -- scopes bucket to this flag
  VERSION     INTEGER,
  DESCRIPTION VARCHAR,
  PRIMARY KEY (APP_ID, FLAG_ID, VERSION)
)

-- User's assigned version for each flag in each app
APP_USER_VERSION (
  APP_ID    VARCHAR,
  FLAG_ID   VARCHAR,                       -- scopes assignment to this flag
  USER_ID   VARCHAR,
  VERSION   INTEGER,
  PRIMARY KEY (APP_ID, FLAG_ID, USER_ID)
)
```

**Why flagId, not flagKey?**
Using UUID (flagId) as the storage key prevents data collision: if a flag is deleted and recreated with the same key, it gets a new flagId, so old version buckets and user assignments are not reused. Clients still see human-readable `flagKey` in responses.

---

## Components

### Flag Management Reactors
App owners use these to create and manage flags. All live under `prerna/reactor/featuregate/`.

| Reactor | Pixel Call | Description |
|---|---|---|
| `CreateAppFeatureFlagReactor` | `CreateAppFeatureFlag(app, key, description)` | Define a new flag for an app (returns flagId) |
| `UpdateAppFeatureFlagReactor` | `UpdateAppFeatureFlag(app, flagId, minVersion, defaultVersion)` | Set which version enables the flag and default for unassigned users |
| `DeleteAppFeatureFlagReactor` | `DeleteAppFeatureFlag(app, flagId)` | Remove a flag and cascade to version buckets and user assignments |
| `GetAppVersionBucketsReactor` | `GetAppVersionBuckets(app, flagId)` | List all version buckets for a flag with descriptions and user lists |

### Version Bucket Management Reactors

| Reactor | Pixel Call | Description |
|---|---|---|
| `CreateAppVersionBucketReactor` | `CreateAppVersionBucket(app, flagId, version, description?)` | Create an empty bucket for a specific version (description optional) |
| `UpdateAppVersionBucketReactor` | `UpdateAppVersionBucket(app, flagId, version, description)` | Update a bucket's description |
| `DeleteAppVersionBucketReactor` | `DeleteAppVersionBucket(app, flagId, version)` | Delete a bucket definition (users assigned to it remain in that version) |

### User Assignment Reactors

| Reactor | Pixel Call | Description |
|---|---|---|
| `SetUserAppVersionReactor` | `SetUserAppVersion(app, flagId, users[], version)` | Assign one or more users to a version for this flag |
| `GetUserAppVersionReactor` | `GetUserAppVersion(app, flagId, user)` | Get a user's current version assignment for this flag |
| `RemoveUserFromFeatureFlagReactor` | `RemoveUserFromFeatureFlag(app, flagId, user)` | Remove a user from a flag; they fall back to the flag's defaultVersion |

### Evaluation Reactors

| Reactor | Pixel Call | Description |
|---|---|---|
| `CheckFeatureFlagReactor` | `CheckFeatureFlag(app, flagId)` | Evaluate: does current user have flag enabled? Returns boolean |
| `GetUserFeatureFlagsReactor` | `GetUserFeatureFlags(app)` | Returns all flags (by flagKey) with evaluation details for the current user, including `enabled`, `userVersion`, `defaultVersion`, `effectiveVersion`, and `minVersion` |

Example response shape:

```json
{
  "new-dashboard": {
    "flagId": "uuid",
    "enabled": true,
    "userVersion": 5,
    "defaultVersion": 0,
    "effectiveVersion": 5,
    "minVersion": 3
  }
}
```

`effectiveVersion` is the version actually used for evaluation. If the user has an explicit version assignment for the flag, it matches `userVersion`. Otherwise it falls back to `defaultVersion`.

### Evaluation Logic — `AppFeatureFlagUtils.java`

```
prerna/auth/utils/AppFeatureFlagUtils.java

evaluate(appId, flagId, userId):
  1. Look up flag: fetch minVersion and defaultVersion
  2. Look up user's assigned version in APP_USER_VERSION for this (app, flag, user)
  3. If no assignment found, use flag's defaultVersion
  4. Compare: userVersion >= minVersion
  5. Return true if comparison holds, else false
```

**Per-flag scoping:** Every query includes both `flagId` and `appId` in the WHERE clause, ensuring users from one flag don't leak into another.

---

## Evaluation Flow

```
App's Pixel script
  └─ CheckFeatureFlag(app, flagId)
       └─ AppFeatureFlagUtils.evaluate(appId, flagId, currentUser)
            ├─ Lookup flag (minVersion, defaultVersion)
            ├─ Lookup user's version for this flag (APP_USER_VERSION)
            ├─ Compare: userVersion >= minVersion
            └─ Return boolean → app shows or hides the feature
```

---

## Example Usage

```
// --- Setup phase 1: Create the flag ---

CreateAppFeatureFlag(
  app         = "myApp",
  key         = "new-dashboard",
  description = "Redesigned dashboard UI"
)
// Returns: flagId = "550e8400-e29b-41d4-a716-446655440000"


// --- Setup phase 2: Define version buckets (optional; auto-created on first assignment) ---

CreateAppVersionBucket(
  app         = "myApp",
  flagId      = "550e8400-e29b-41d4-a716-446655440000",
  version     = 1,
  description = "Beta testers"
);

CreateAppVersionBucket(
  app         = "myApp",
  flagId      = "550e8400-e29b-41d4-a716-446655440000",
  version     = 2,
  description = "General release"
);


// --- Setup phase 3: Configure flag evaluation ---

UpdateAppFeatureFlag(
  app            = "myApp",
  flagId         = "550e8400-e29b-41d4-a716-446655440000",
  minVersion     = 2,              // flag enabled for v2 and above
  defaultVersion = 0               // unassigned users see v0 (disabled)
);


// --- Setup phase 4: Assign users to versions ---

// Jsmith is a beta tester (v1)
SetUserAppVersion(
  app    = "myApp",
  flagId = "550e8400-e29b-41d4-a716-446655440000",
  users  = ["jsmith"],
  version = 1
);

// Everyone else gets v2 (released)
SetUserAppVersion(
  app    = "myApp",
  flagId = "550e8400-e29b-41d4-a716-446655440000",
  users  = ["user1", "user2", "user3"],
  version = 2
);


// --- Runtime: Check if feature is enabled for current user ---

if ( CheckFeatureFlag(app="myApp", flagId="550e8400-e29b-41d4-a716-446655440000") ) {
  // jsmith: v1 >= 2? No → feature OFF
  // user1:  v2 >= 2? Yes → feature ON
  // Render new dashboard
} else {
  // Render legacy dashboard
}


// --- Later: Update a bucket description ---

UpdateAppVersionBucket(
  app         = "myApp",
  flagId      = "550e8400-e29b-41d4-a716-446655440000",
  version     = 1,
  description = "Early access (closed beta)"
);


// --- Cleanup: Delete unused bucket ---

DeleteAppVersionBucket(
  app    = "myApp",
  flagId = "550e8400-e29b-41d4-a716-446655440000",
  version = 1
);
// Users in v1 still exist; just the definition is gone


// --- Or: Remove a user from the flag entirely ---

RemoveUserFromFeatureFlag(
  app    = "myApp",
  flagId = "550e8400-e29b-41d4-a716-446655440000",
  user   = "jsmith"
);
// jsmith's assignment is deleted; they now use the flag's defaultVersion
```

---

## Permissions

| Action | Who can do it |
|---|---|
| Create / update / delete flags | App owner or SEMOSS admin |
| Assign user versions | App owner or SEMOSS admin |
| Evaluate a flag (`CheckFeatureFlag`) | Any authenticated user with app access |

Enforced via existing `SecurityProjectUtils` — no new permission infrastructure needed.

---

## What's Reused from SEMOSS

| Need | Reuse |
|---|---|
| DB persistence | Security DB (H2/RDB) via existing `AbstractSecurityUtils` patterns |
| Auth context | `User` object already on thread via `AccessToken` |
| Reactor pattern | Same `AbstractReactor` all operations use |
| Permission checks | `SecurityProjectUtils` for app owner validation |
| Group membership | Existing security group tables |

---

## Implementation Status

**Phase 1 — Core Version-Based Bucketing (✅ Complete)**
- [x] Data model: APP_FEATURE_FLAG, APP_VERSION_BUCKET, APP_USER_VERSION (all flagId-scoped)
- [x] Flag management: Create, Update, Delete
- [x] Version bucket management: Create, Update, Delete
- [x] User assignment: SetUserAppVersion, GetUserAppVersion
- [x] Evaluation: CheckFeatureFlag, GetUserFeatureFlags
- [x] Per-flag data isolation via flagId

**Removed — Rule-Based System**
- ❌ APP_FEATURE_FLAG_RULE table (never queried in production; replaced by explicit version assignment)
- ❌ USER and GROUP rule types (replaced by direct SetUserAppVersion calls)

**Not Planned**
- Percentage-based rollout (can be simulated with explicit version assignment)
- Time-window activation (can be managed externally)
