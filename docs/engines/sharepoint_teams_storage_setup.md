# SharePoint and Teams Storage Setup

Follow these steps in Microsoft 365 before creating a `SHAREPOINT` or `MICROSOFT_TEAMS` storage catalog in SEMOSS.

Both catalog types connect through Microsoft Graph as an application, so every read and write runs under one service identity and catalog access in SEMOSS controls who can use the files.

## Who does what

| Step | Who |
| --- | --- |
| 1 and 2: Register the app and add permissions | A user allowed to register apps, such as an Application Administrator |
| 3: Grant admin consent | Privileged Role Administrator or Global Administrator |
| 4: Grant site access (`Sites.Selected` only) | SharePoint Administrator or higher who is also a member of the target site |
| 5: Create the catalog | A SEMOSS user who can create catalogs |

## Choose an access model

| What the catalog needs | Catalog type | Application permissions | Step 4 |
| --- | --- | --- | --- |
| Specific sites only (recommended) | Microsoft SharePoint | `Sites.Selected` | Required |
| One team, without tenant wide access | Microsoft SharePoint, pointed at the team's site | `Sites.Selected` | Required |
| Any site in the tenant | Microsoft SharePoint | `Sites.ReadWrite.All` | Skip |
| A team or channel, looked up by name | Microsoft Teams | `Team.ReadBasic.All`, `Channel.ReadBasic.All`, `Files.ReadWrite.All` | Skip |

For a read only catalog, use `Sites.Read.All` instead of `Sites.ReadWrite.All`, `Files.Read.All` instead of `Files.ReadWrite.All`, and the `read` role in step 4.

`Sites.Selected` limits the app to exactly the sites you grant, while `Sites.ReadWrite.All` and `Files.ReadWrite.All` apply across the tenant.

## Step 1: Register the app

1. Sign in to the [Microsoft Entra admin center](https://entra.microsoft.com).
2. Go to **Entra ID > App registrations > New registration**.
3. Enter a name. Under **Supported account types**, select the single tenant option. Leave **Redirect URI** empty.
4. Select **Register**.
5. On the **Overview** page, copy the **Application (client) ID** and the **Directory (tenant) ID**.
6. Go to **Certificates & secrets > Client secrets > New client secret**.
7. Choose an expiry and select **Add**. The portal allows up to 24 months.
8. Copy the secret's **Value** now. It is only shown once, and the **Secret ID** will not work in its place.

## Step 2: Add permissions

1. On the app registration, go to **API permissions > Add a permission > Microsoft Graph > Application permissions**.
2. Select the permissions for your access model.
3. Select **Add permissions**.
4. Confirm that the **Type** column shows **Application** for each one. The default delegated `User.Read` entry is not needed and can be removed.

## Step 3: Grant admin consent

1. On the **API permissions** page, select **Grant admin consent** and confirm with **Yes**.
2. Confirm that the **Status** column shows **Granted** for each permission.

An administrator can also grant consent at this URL:

```
https://login.microsoftonline.com/<tenant id>/adminconsent?client_id=<client id>
```

## Step 4: Grant the app access to a site

Do this step if you chose `Sites.Selected`, and repeat it for each site. The grant is made through Microsoft Graph.

1. Make sure the account you will use has the SharePoint Administrator role or higher and is a member of the target site. For a team's site, add the account to the team. For a private channel's site, add it to the channel.
2. Open [Graph Explorer](https://developer.microsoft.com/graph/graph-explorer) and sign in with that account.
3. Select your avatar at the top right, then **Consent to permissions**. Consent to `Sites.Read.All` and `Sites.FullControl.All`. If Graph Explorer asks for admin approval, have a Cloud Application Administrator or Privileged Role Administrator consent on behalf of the organization, then sign out and back in.
4. Run this request, using the site's name:

   ```
   GET https://graph.microsoft.com/v1.0/sites?search=<site name>
   ```

5. Find the entry whose `webUrl` matches the site, and copy its `id` exactly as returned.
6. Run this request, using that `id` and the client ID from step 1. Use `read` instead of `write` for a read only catalog. A successful grant returns `201 Created`.

   ```
   POST https://graph.microsoft.com/v1.0/sites/<site id>/permissions

   {
     "roles": ["write"],
     "grantedToIdentities": [{
       "application": {
         "id": "<client id>",
         "displayName": "<app name>"
       }
     }]
   }
   ```

7. Run this request and confirm that the app is listed:

   ```
   GET https://graph.microsoft.com/v1.0/sites/<site id>/permissions
   ```

PnP PowerShell can also make this grant, using its own app registration in the tenant:

```powershell
Grant-PnPEntraIDAppSitePermission -AppId <client id> -DisplayName <app name> -Site <site url> -Permissions Write
```

The grant applies to the whole site, and the **Document Library** and **Folder** fields in step 5 root the catalog at the library or folder you choose.

## Step 5: Create the catalog

In SEMOSS, add a storage catalog, choose **Microsoft SharePoint** or **Microsoft Teams**, and enter these values.

### Microsoft SharePoint

| Form field | SMSS key | Value |
| --- | --- | --- |
| Tenant | `SP_TENANT` | Directory (tenant) ID |
| Client ID | `SP_CLIENT_ID` | Application (client) ID |
| Client Secret | `SP_CLIENT_SECRET` | The secret's **Value** |
| Site | `SP_SITE` | Site URL, such as `https://contoso.sharepoint.com/sites/Marketing` |
| Document Library | `SP_LIBRARY` | Optional. The library's display name. Leave empty for the site's default library. |
| Folder | `SP_FOLDER` | Optional. A folder within the library to use as the catalog's root. |
| On Name Conflict | `MS_CONFLICT_BEHAVIOR` | `replace` (default), `rename` or `fail` |

### Microsoft Teams

| Form field | SMSS key | Value |
| --- | --- | --- |
| Tenant | `MS_TEAMS_TENANT` | Directory (tenant) ID |
| Client ID | `MS_TEAMS_CLIENT_ID` | Application (client) ID |
| Client Secret | `MS_TEAMS_CLIENT_SECRET` | The secret's **Value** |
| Team | `MS_TEAMS_TEAM` | Team display name, or the team ID if more than one team shares the name. The ID is the `groupId` value in the team's **Get link to team** URL. |
| Channel | `MS_TEAMS_CHANNEL` | Optional. Channel display name or channel ID. Leave empty to use the team's whole document library. |
| On Name Conflict | `MS_CONFLICT_BEHAVIOR` | `replace` (default), `rename` or `fail` |

### A team's files through the SharePoint type

The **Microsoft SharePoint** type can scope a catalog to a single team or channel with `Sites.Selected`.

1. In Teams, open the channel's files tab (**Files** or **Shared**, depending on the Teams version) and select **Open in SharePoint**.
2. Read the site URL and folder name from the page that opens.
3. Make the step 4 grant on that site.
4. Fill in the fields for the channel type:

| Channel type | Site | Document Library | Folder |
| --- | --- | --- | --- |
| Standard | The team's site | Leave empty | The channel's folder, such as `General` |
| Private or shared | The channel's own site, which is separate from the team's site | Leave empty | Leave empty, unless the site has a folder that holds the channel's files |

## Troubleshooting

SEMOSS checks the connection when a catalog is created and reports what to fix, after the text `Failed to create storage engine. Error:`. Once it is fixed, create the catalog again with the same name.

| Message | Cause | Fix |
| --- | --- | --- |
| `Unable to acquire an app-only Microsoft Graph token` | The tenant, client ID or secret is wrong, or the secret has expired | Recheck the values, and make sure the secret is the **Value** and not the **Secret ID** |
| `The access token carries no application permissions` | Admin consent was not granted | Do step 3 |
| `The app holds Sites.Selected` | The site grant is missing | Do step 4 for this site |
| `The token carries [...], which should be sufficient` | A permission is missing from the list in the message, or SharePoint's own permissions block the target | For a Teams catalog, check that all three permissions are listed. Otherwise check the library or folder for broken permission inheritance, and grant the app access there if it is broken. |
| `403` on the `POST` in step 4 | The account is missing a requirement | Check that `Sites.FullControl.All` is consented, the account has the SharePoint Administrator role or higher, and the account is a member of the site |
| `No document library named` | The library name does not match | Use one of the names the message lists |
| `More than one Microsoft Team is named` | The team name is not unique | Enter the team ID instead |

To check whether a consent took effect in Graph Explorer, copy the token from the **Access token** tab into [jwt.ms](https://jwt.ms) and look for the permission in the `scp` claim. If it is missing, sign out and back in, since consent only applies to newly issued tokens.

## After setup

- **Permission changes:** Restart SEMOSS after changing permissions or site grants for an existing catalog, so it picks up a new token.
- **Secret rotation:** Before the secret expires, create a new one and update `SP_CLIENT_SECRET` or `MS_TEAMS_CLIENT_SECRET` on the catalog.

## Related

- [Storage engines](storage_engines.md)
- [SharePointStorageEngine](../../src/prerna/engine/impl/storage/SharePointStorageEngine.java)
- [MicrosoftTeamsStorageEngine](../../src/prerna/engine/impl/storage/MicrosoftTeamsStorageEngine.java)
- [Grant tenant wide admin consent](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/grant-admin-consent)
- [Create a site permission](https://learn.microsoft.com/en-us/graph/api/site-post-permissions?view=graph-rest-1.0)
- [Private channel SharePoint sites](https://learn.microsoft.com/en-us/microsoftteams/private-channels)
