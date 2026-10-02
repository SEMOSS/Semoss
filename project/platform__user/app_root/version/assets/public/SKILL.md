---
name: user
description: Use when writing code in an app that needs to know who the current user is (name, email, username, groups, admin status), greet them, branch on group membership, store per-user preferences that survive across sessions, or show platform notifications (badge counts, notification lists, mark-as-read). Covers GetCurrentUser(), GetUserInfo(), SetUserMetadata(), GetUserMetadata(), GetUserMetakeyOptions(), PollNotifications(), FetchNotifications(), MarkNotificationRead(), and DeleteNotification() via @semoss/sdk's runPixel, and explains why the REST identity endpoints are unreliable for email. Do not use for login/logout (see app-bootstrap), roles on a specific project or engine (see permissions), or per-user file storage (see app-data).
---

# Current user, preferences, and notifications

All via `runPixel` from `@semoss/sdk`.

## Who is the current user?

```typescript
import { runPixel } from "@semoss/sdk";

const r = await runPixel(`GetCurrentUser();`, insightId);
const info = r.pixelReturn[0].output as {
  isAdmin: boolean;
  [provider: string]: {
    id: string;
    name: string;
    username: string;
    email: string;
    lastLogin?: string;
    groupInfo?: { groupType: string; groups: string[] };
  } | boolean;
};

// the map is keyed by auth provider (NATIVE, LDAP, an OAuth provider, ...)
const provider = Object.keys(info).find((k) => k !== "isAdmin");
const me = info[provider];
```

Points that matter:

- **This pixel is the reliable identity source.** The REST alternatives fall short: `/api/config` `loginDetails` carries only `{id, name}` (no email), and `/api/auth/userinfo/{provider}` round-trips to the external identity provider - for native/LDAP logins it soft-fails with HTTP 200 and an `errorMessage` body. Do not build identity on those.
- A user can be logged into multiple providers; iterate the keys rather than assuming one.
- `isAdmin` is platform admin - for per-project/per-engine roles use the permissions skill instead.
- `GetUserInfo()` is a near-equivalent returning the same provider-keyed map; `GetCurrentUser` is preferred (it includes `isAdmin`).

Call once after init and cache it in app state - identity does not change mid-session.

## Per-user preferences that survive reloads

The user metadata store is a durable server-side key/value map per user - the right home for theme choice, defaults, onboarding flags, and anything else small. No SQL table, no files.

```typescript
// write (merges into the user's existing metadata)
await runPixel(
  `SetUserMetadata(meta=[${JSON.stringify({ "app-theme": "dark", "onboarded": "true" })}]);`,
  insightId,
);

// read (defaults to the current user; filter with metaKeys)
const prefs = await runPixel(`GetUserMetadata(metaKeys=["app-theme","onboarded"]);`, insightId);
```

- Values are strings - JSON-stringify anything structured.
- Keys may be governed: `GetUserMetakeyOptions()` returns the allowed keys/options when the install restricts them; if a `SetUserMetadata` errors on an unknown key, namespace yours (e.g. `myapp-theme`) or use user asset files (see app-data) instead.
- For large or structured per-user data, prefer a database table keyed by the user id from `GetCurrentUser`.

## Platform notifications (read-only)

Apps can consume the platform notification feed but **cannot create notifications** - they are emitted server-side (access-request flows, shares). The feature can also be disabled per install, and anonymous users are rejected; treat errors as "hide the bell icon".

```typescript
// badge count
const unread = await runPixel(`PollNotifications();`, insightId);
const count = unread.pixelReturn[0].output as number;

// the list (paged)
const list = await runPixel(`FetchNotifications(limit=[20], offset=[0]);`, insightId);
// each entry: notification_id, notification_title, notification_message,
//   notification_actiontype, notification_actiontarget, notification_isread,
//   notification_priority, notification_type, catalog_id,
//   notification_createddate, notification_readdate, notification_source, notification_createdby

await runPixel(`MarkNotificationRead(notificationId=["${id}"]);`, insightId);
await runPixel(`DeleteNotification(notificationId=["${id}"]);`, insightId); // omit notificationId to delete ALL - never call it bare from UI code
```

Poll `PollNotifications` on an interval (e.g. 60s) for the badge; fetch the list only when the user opens the panel. Note `FetchNotifications` has a side effect: it resets each returned notification's action type.

## Related self-service flows

When a user lacks access to something the app needs, let them request it instead of dead-ending: `RequestEngine(engine, permission, comment)` and `RequestInsight(project, id, permission, comment)` file an access request (which notifies the owners through this same notification system). Pending state: `GetEngineUserAccessRequest(engine)` / `GetInsightUserAccessRequest(project, id)`. Approvals happen on the owner side via the permissions skill.
