---
name: permissions
description: Use when writing code in an app that checks the current user's access level, gates UI by role, lists a project's or engine's members, shares a project or engine with other users, edits or removes member permissions, or approves/denies pending access requests. Covers getUserProjectPermission, getUserEnginePermission, getProjectUsers, getEngineUsers, addProjectUserPermissions, editProjectUserPermissions, removeProjectUserPermissions (and the engine variants), approveProjectUserAccessRequest, denyProjectUserAccessRequest, propagateUserPermissions, and getProjectUsersNoCredentials/getEngineUsersNoCredentials from @semoss/sdk. Do not use for login and auth bootstrap (see app-bootstrap) or for creating/deleting rooms (see room).
---

# Permissions and sharing

The platform's access model has two resource kinds an app can manage:

- **Projects** — apps and skill/workspace projects, identified by `projectId`
- **Engines** — models, databases, vector stores, storage, identified by `engineId`

Every helper is a plain async REST function imported straight from `@semoss/sdk` — these are **not** pixels, so there is no `runPixel` envelope and no `errors` array. They resolve with typed data or **throw** on failure; wrap them in try/catch.

The permission levels (`Role` type):

| Role | Meaning |
| --- | --- |
| `OWNER` | Full control, can manage members |
| `EDIT` | Can modify content |
| `READ_ONLY` | Can view/use |
| `DISCOVERABLE` | Sees it exists, can request access |

Every function takes an `admin` boolean (default `false`) right after the id. `true` routes through `/api/auth/admin/...` and requires the caller to be a platform admin — leave it `false` in normal app code.

## Gate UI by the current user's role

```typescript
import { getUserProjectPermission, getUserEnginePermission } from "@semoss/sdk";
import type { Role } from "@semoss/sdk";

const role: Role = await getUserProjectPermission(projectId);
const canManageMembers = role === "OWNER";
const canEdit = role === "OWNER" || role === "EDIT";

// same idea for an engine (e.g. hide storage-write buttons without EDIT)
const engineRole: Role = await getUserEnginePermission(engineId);
```

Check once after init and store the result; the role does not change mid-session unless someone edits it.

## List members

```typescript
import { getProjectUsers, getEngineUsers } from "@semoss/sdk";

const { totalMembers, members } = await getProjectUsers(
  projectId,
  false,      // admin
  searchTerm, // optional filter
  undefined,  // optional permission filter, e.g. "OWNER"
  limit,      // optional paging
  offset,
);
// members: Array<{ id, name, permission, email?, type?, date_added? }>
```

`getEngineUsers` has the same shape. Note the third argument is a **search term** matched against users, not an exact user id, for both functions. Use `totalMembers` with `limit`/`offset` to page a long member list (see the pagination skill).

## Share with new users

Finding people who do *not* yet have access, then granting it:

```typescript
import {
  getProjectUsersNoCredentials,
  addProjectUserPermissions,
} from "@semoss/sdk";
import type { PostUser } from "@semoss/sdk";

// 1. search users without access (returns User[])
const candidates = await getProjectUsersNoCredentials(projectId, false, searchTerm, 20, 0);

// 2. grant access — PostUser is { userid, permission }
const grants: PostUser[] = [{ userid: candidates[0].id, permission: "READ_ONLY" }];
const ok: boolean = await addProjectUserPermissions(projectId, grants);
```

`addEngineUserPermissions` / `getEngineUsersNoCredentials` are the engine equivalents.

## Change or revoke access

```typescript
import {
  editProjectUserPermissions,
  removeProjectUserPermissions,
} from "@semoss/sdk";

await editProjectUserPermissions(projectId, [{ userid, permission: "EDIT" }]);
await removeProjectUserPermissions(projectId, [userid]);
```

All mutation helpers return a `boolean` success flag. Engine variants: `editEngineUserPermissions`, `removeEngineUserPermissions`.

## Access requests

Users with `DISCOVERABLE` visibility can request access; owners approve or deny:

```typescript
import {
  approveProjectUserAccessRequest,
  denyProjectUserAccessRequest,
} from "@semoss/sdk";
import type { UserAccessRequest } from "@semoss/sdk";

// approve — each request carries the requestid you received with the pending request
const requests: UserAccessRequest[] = [
  { requestid, userid, permission: "READ_ONLY" },
];
await approveProjectUserAccessRequest(projectId, requests);

// deny takes the request ids
await denyProjectUserAccessRequest(projectId, [requestid]);
```

## Propagate project access to its dependencies

An app is usually only usable if its members can also reach the engines it depends on (its model, database, vector, storage selections). After sharing a project, push the same grants down:

```typescript
import { propagateUserPermissions } from "@semoss/sdk";

await propagateUserPermissions(projectId, [{ userid, permission: "READ_ONLY" }]);
```

This is the common follow-up to `addProjectUserPermissions` — without it, a newly added member may open the app and hit engine-permission errors on every pixel.

## Error handling

```typescript
try {
  await addProjectUserPermissions(projectId, grants);
} catch (e) {
  // surfaces server-side rules: only OWNERs can manage members,
  // a user cannot lower their own permission, etc.
  showToast(e instanceof Error ? e.message : "Sharing failed");
}
```

Do not pre-validate rules the server owns — attempt the call and surface its message. The one check worth doing up front is `getUserProjectPermission` to hide member-management UI from non-owners entirely.
