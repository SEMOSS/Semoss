# Monolith Webhooks and Change Notifications

Monolith receives GitHub repository events and Microsoft Graph mailbox/calendar notifications. This guide describes the receivers and the endpoints used to configure them in the current source tree.

All paths below include the default `/Monolith` context. Preserve any additional deployment prefix when constructing public URLs. GitHub uses the dedicated `/github/*` servlet and Microsoft Graph uses `/msgraph/*`; both are registered in [Monolith's web.xml](https://github.com/SEMOSS/Monolith/blob/dev/WebContent/WEB-INF/web.xml).

## Inbound endpoints

| Method and endpoint | Purpose | Delivery verification | Normal response |
| --- | --- | --- | --- |
| `POST /Monolith/github/webhook` | Receive GitHub App events and synchronize linked projects on pushes | `X-Hub-Signature-256` using the configured app's webhook secret | `200` acknowledgment |
| `POST /Monolith/msgraph/notifications/messages` | Receive mailbox change notifications | Recorded subscription ID and matching `clientState` | `202` acknowledgment |
| `POST /Monolith/msgraph/notifications/events` | Receive calendar change notifications | Recorded subscription ID and matching `clientState` | `202` acknowledgment |

Providers call these receivers without a SEMOSS browser session. The management endpoints below perform their own session, administrator, project-owner, or subscription-owner checks. The Microsoft receivers also handle subscription validation: a POST with a nonempty `validationToken` query parameter returns that token as `text/plain` with status `200`.

## GitHub

### Configure the app and project link

An administrator creates the deployment's GitHub App through the manifest flow. The generated manifest registers the webhook URL, installation and user-authorization callbacks, read access to repository contents and metadata, and the `push` event. The callback stores the app configuration in the security database. The current service uses one configured app per deployment.

A project owner then installs or selects an installation, authorizes their GitHub account, and links a repository to the SEMOSS project. Repository choices are scoped to what that GitHub user can access. The saved link includes the repository ID, installation, tracked branch, and optional monorepo subdirectory.

All routes in the following tables are beneath **`/Monolith/github`**. Parameters are query/form parameters read by the service, unless described as callback parameters.

| Method and path | Parameters | Purpose / access |
| --- | --- | --- |
| `GET /available` | None | Logged-in user; return whether a GitHub App is configured |
| `GET /manifest/new` | Optional `org`, `name`, `public` | Administrator; begin app creation in the browser |
| `GET /manifest/callback` | GitHub supplies `code`, `state` | Administrator session and matching state; save app configuration |
| `GET /manifest/apps` | None | Administrator; list app configuration with secrets removed |
| `GET /manifest/projects` | None | Administrator; list project/repository links |
| `DELETE /manifest/app` | Optional `appId` | Administrator; remove local app configuration and its project links; does not delete the GitHub registration |

| Method and path | Parameters | Purpose |
| --- | --- | --- |
| `GET /install/app` | `projectId` | Begin the browser installation flow |
| `GET /install/callback` | GitHub supplies `installation_id`, `setup_action` | Complete installation/linking, including user authorization when needed |
| `GET /user/authorize` | `projectId` | Begin the user's GitHub authorization flow |
| `GET /user/callback` | GitHub supplies `code`, `state` | Complete the state-checked authorization flow |
| `GET /install/installations` | `projectId` | List installations accessible to the authorized GitHub user |
| `GET /install/repos` | `projectId`, `installationId` | List accessible repositories |
| `GET /install/branches` | `projectId`, `installationId`, `repoFullName` | List branches for an accessible repository |
| `POST /install/select` | `projectId`, `installationId`, `repoId`; optional `branch`, `subdir` | Save or replace a project's repository link |
| `GET /project/link` | `projectId` | Read the project's link and tracked branch |
| `POST /project/branch` | `projectId`, `branch` | Update the tracked branch |
| `DELETE /project/link` | `projectId` | Disconnect the project; does not uninstall the GitHub App |

The repository picker, explicit authorization entry point, and project-link operations require SEMOSS project-owner access. Use the browser installation and callback flow so its pending project and authorization state are retained. Missing or expired GitHub authorization in picker operations returns `401` with `needsAuth: true`.

When `branch` is omitted from `/install/select`, the service uses the repository's default branch, falling back to `main`. Saving a link or changing its branch updates configuration; those endpoints do not immediately synchronize files.

### Delivery and synchronization

GitHub posts a JSON event body with `X-GitHub-Event` and `X-Hub-Signature-256`. The handler verifies the HMAC-SHA256 signature over the received body before processing it. Preserve the body through proxies and relays. Missing or invalid signatures return `401` with `reason: "invalid signature"`.

Verified events other than `push`, including the initial `ping`, return `200` with an ignored-event message. For pushes, the handler reads the repository ID, branch ref, commit information, and pusher from the standard GitHub payload. It finds every project linked to that repository ID and submits background synchronization. Projects tracking a different branch are skipped; legacy links without a saved branch fall back to the local checked-out branch.

Full-repository synchronization resets the project's local repository to the remote branch, replacing uncommitted or divergent local work. A link with `subdir` copies that remote directory into the project's assets and records the update in local Git history. Successful synchronization invokes the configured project cloud-push path.

The `200` response with `message: "Push event processed"` acknowledges receipt and dispatch. It also covers pushes with no linked project and does not confirm that background synchronization succeeded. The executor and pending sync tasks live in the receiving process; check application logs for completion or failure.

### Delivery diagnostics

Run the following Pixel expression in an authenticated SEMOSS context:

```pixel
GitHubWebhookDeliveries(project=["<project-id>"], limit=[30]);
```

The caller needs view access to the linked project. Omitting `project` requires administrator access. The result contains `count` and `deliveries`, with event, HTTP status, time, installation, and repository information. `limit` defaults to `30` and is capped at `100`.

The reactor queries GitHub's recent delivery feed and filters the returned page by the project's installation. Results may include other repositories in that installation, and this feed is not a durable synchronization audit log. Pair it with the project's saved branch and Monolith sync logs.

## Microsoft Graph

### Subscription management

The subscription service watches mailbox and calendar resources, such as `me/messages` and `me/events`, using the caller's delegated Microsoft identity. It chooses the corresponding receiver URL and generates a per-subscription `clientState` value internally.

All routes below are beneath **`/Monolith/msgraph`**. Send parameters as query parameters or form fields. Except for the availability preflight, management operations require a valid SEMOSS session and a linked Microsoft login. Renewal and deletion require ownership under that Microsoft identity.

| Method and path | Parameters | Result |
| --- | --- | --- |
| `GET /available` | None | Availability, Microsoft sign-in state, configured scope checks, notification base URL, and explanatory reasons |
| `POST /subscribe` | Required `resource`; optional `changeType`, `minutes` | Create a subscription and return `subscriptionId`, resource, receiver URL, and expiration |
| `GET /subscriptions` | None | Return `recorded` subscriptions for the user and the provider's `atMicrosoft` result |
| `POST /renew` | Required `subscriptionId`; optional `minutes` | Renew an owned subscription and update its recorded expiration |
| `DELETE /subscription` | Required `subscriptionId` | Delete an owned subscription at Microsoft, then remove the local record |

For example, send an authenticated, form-encoded request with this body to `POST /Monolith/msgraph/subscribe`:

```text
resource=me%2Fmessages&changeType=created&minutes=60
```

Use `resource=me%2Fevents` for calendar events. `changeType` defaults to `created`; requested change types are passed to Graph. The client clamps `minutes` to its configured resource lifetime range and uses the configured maximum when the value is omitted or nonpositive. Use the returned `expirationDateTime` to schedule renewal. **There is no automatic renewal service in this path.**

The preflight checks configured `ms_scope` values: `Mail.Read` or `Mail.ReadWrite` for messages, and `Calendars.Read` or `Calendars.ReadWrite` for events. Update the Microsoft scopes in `social.properties` and sign in to Microsoft again when changing them. Availability is a configuration check, not a network probe or proof of tenant consent; subscription creation must still succeed at Microsoft.

Successful management calls return `200`. Missing sessions return `401`; a missing Microsoft identity or required input returns `400`; renewal/deletion of an unknown or non-owned subscription returns `404`. Provider failures can return `500`. Listing can return `200` with `atMicrosoft` describing an unavailable provider result while still returning recorded subscriptions.

### Validation and delivery

During subscription creation, Graph calls the selected receiver with `validationToken`. The receiver echoes it as plain text before looking up a subscription. The public route must be reachable for this handshake to complete.

Normal deliveries contain a JSON `value` array. Each notification identifies a `subscriptionId`, `clientState`, `changeType`, and changed resource ID in `resourceData.id`. The receiver looks up the subscription in the security database, checks the client state, and dispatches accepted items to a background worker. Unknown subscriptions and mismatched client states are skipped.

Malformed JSON returns `400` with `reason: "malformed body"`. An empty batch returns `202` with `message: "Nothing to do"`; a processed batch returns `202` with `message: "Accepted"` and the accepted `count`, which can be zero. Acceptance records dispatch, not successful resource retrieval.

**Current behavior:** the worker fetches the changed message or event using the subscriber's stored Microsoft credentials and logs a summary. Deleted resources are logged without fetching them. This path does not currently submit an agent run, send a reply, or create a scheduled workflow.

### Persistence and execution

[MicrosoftGraphSubscriptionRegistry](../../src/prerna/io/connector/ms/MicrosoftGraphSubscriptionRegistry.java) stores subscription identity, client state, expiration, and subscriber credentials in the security database's `MS_GRAPH_SUBSCRIPTION` table. Credential refresh writes updated tokens back to that record. Nodes receiving notifications need access to the same security database and compatible Microsoft configuration.

Subscription records survive process restarts through the database. The notification executor is process-local; its queued work is not a durable agent run or job queue. Use `/subscriptions` to compare recorded state with Microsoft's state, renew before expiration, and check logs for background fetch failures.

## Public URLs and deployment

The URL helpers start from `Utility.getApplicationUrl()`, which resolves the deployment's application origin, optional route prefix, and context path. With a base of `https://semoss.example.com/Monolith`, the receivers are:

```text
https://semoss.example.com/Monolith/github/webhook
https://semoss.example.com/Monolith/msgraph/notifications/messages
https://semoss.example.com/Monolith/msgraph/notifications/events
```

| Core configuration property | Effect |
| --- | --- |
| `GH_PUBLIC_ORIGIN_OVERRIDE` | Replace the public origin for GitHub browser callbacks and the default webhook URL, preserving the application path |
| `GH_WEBHOOK_URL_OVERRIDE` | Replace the complete GitHub webhook URL, for example when using a local development relay |
| `MS_PUBLIC_ORIGIN_OVERRIDE` | Replace the public origin for Microsoft notification URLs, preserving the application path |

These are core properties read through `Utility.getDIHelperProperty`, commonly supplied in `RDF_Map.prop`; they are not automatically container environment variable names. Use an origin without the application path for the origin overrides. Microsoft subscription creation requires a public HTTPS receiver; configure a reachable HTTPS origin when testing through a local tunnel.

Ensure the ingress forwards `/github/*` and `/msgraph/*` under the application context, including provider POSTs and browser callbacks. Verify the effective container `web.xml`: [Monolith's local Docker workflow](https://github.com/SEMOSS/Monolith#quick-start-with-docker) preserves the descriptor from the base image. See [complete deployment examples](https://github.com/SEMOSS/SEMOSS-deployment) for infrastructure configuration.

## Implementation references

- GitHub: [GitHubApplication](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/app/GitHubApplication.java), [GitHubService](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/github/GitHubService.java), [AdminGitHubService](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/github/AdminGitHubService.java).
- GitHub core helpers: [GitHubAppClient](../../src/prerna/io/connector/github/GitHubAppClient.java), [GitHubProjectSync](../../src/prerna/io/connector/github/GitHubProjectSync.java), [GitHubWebhookDeliveriesReactor](../../src/prerna/io/connector/github/GitHubWebhookDeliveriesReactor.java).
- Microsoft: [MicrosoftGraphApplication](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/app/MicrosoftGraphApplication.java), [MicrosoftGraphNotificationService](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/ms/MicrosoftGraphNotificationService.java), [MicrosoftGraphSubscriptionService](https://github.com/SEMOSS/Monolith/blob/dev/src/prerna/semoss/web/ms/MicrosoftGraphSubscriptionService.java).
- Shared persistence and Graph client: [SecurityExternalConnectorsUtils](../../src/prerna/auth/utils/SecurityExternalConnectorsUtils.java), [MicrosoftGraphSubscriptionClient](../../src/prerna/io/connector/ms/MicrosoftGraphSubscriptionClient.java).
- Related guides: [Monolith integration](monolith_interaction.md), [internal databases](../platform_services/internal_databases.md), and [agent runs](../agents/agent_runs.md).
