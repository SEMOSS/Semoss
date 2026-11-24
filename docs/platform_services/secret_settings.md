# Secret settings

SEMOSS keeps the secrets in engine, project and RDF_Map settings out of what it shows and sends: the smss editor, the RDF_Map editor, log files, the audit trail, and error text.

## Which settings are secret

One rule, [SensitiveKeys](../../src/prerna/security/SensitiveKeys.java), decides from the key name. Names are compared without case or separators, so `SECRET_KEY`, `secretKey` and `secret-key` are the same name. A key is secret when:

- it is a listed name, such as `PASSWORD`, `API_KEY`, `OPEN_AI_KEY`, `AZ_PRIMARY_KEY`, `HEADERS` or `AUTHORIZATION`;
- it ends with `PASSWORD`, `PASSWD`, `PWD`, `SECRET`, `TOKEN`, `API_KEY`, `ACCESS_KEY`, `SECRET_KEY`, `PRIVATE_KEY`, `ACCOUNT_KEY`, `SERVICE_ACCOUNT_KEY`, `SERVICE_ACCOUNT_JSON`, `CREDENTIALS`, `CREDS`, `CONN_STRING`, `CONNECTION_STRING`, `SAS_URL` or `SESSION_ID`, so `SMTP_PASSWORD`, `SP_CLIENT_SECRET`, `MCP_AUTH_TOKEN` and `1_PASSWORD` are all secret; or
- it is named in the RDF_Map `SENSITIVE_KEYS` setting, a comma separated list:

```
SENSITIVE_KEYS	VENDOR_SIGNING_SEED,PARTNER_LICENSE
```

When you add a setting that holds a secret to an engine, give it a name the rule covers, such as `VENDOR_API_KEY`.

## Where secret values are hidden

| Where | What the user sees |
| --- | --- |
| smss editor (`GetEngineSMSS`, `GetProjectSMSS`, `GetDatabaseSMSS` and the admin versions) | Each secret key keeps its line, with `********` as the value, including values held in the secret store and values that run over several lines. Saving keeps the current value for a line that still shows `********` and saves any new value typed over it. |
| RDF_Map editor | `********` for every secret key, and for the secret store and keystore settings. |
| Log files (the `%maskMsg` pattern) | `********` for the value in `key=value`, `"key":"value"` and `?key=value` when the key is secret. |
| Audit trail | `[REDACTED]` for values under secret keys and for the secret shapes listed below. |
| Error text | `[REDACTED]` in place of each secret, as described next. |

## Editing an smss

An engine's or project's owners, and admins in admin mode, edit its smss in the smss editor. The read reactors return the masked text (`smss`) and `smssHash`, the SHA-256 of the smss file's bytes and of the secret store's values in key order ([SmssEditCopy](../../src/prerna/engine/impl/SmssEditCopy.java)). With a secret store, a save changes the store and not the file, so the hash covers both. The server works the hash out from what is stored, never from the text the editor sends back, so the masking does not change it.

The save endpoints (`/e-{engineId}/updateSmssFile`, `/project-{projectId}/updateSmssFile` and the admin `auth/admin/engine/updateSmssFile` and `auth/admin/project/updateSmssFile`) take the edited text and that `smssHash`. Under the engine's or project's lock, the save goes through only while the smss is still as the editor read it, and its answer carries the new `smssHash`:

| The save | The answer |
| --- | --- |
| Made from the smss as it is now | 200 with `success` and the new `smssHash` |
| Made after someone else saved the smss, or after its secret store values changed | 409 with `errorMessage` and `errorCode` `SMSS_CHANGED`; the smss is not changed |
| Sent without `smssHash` | 400, asking to load the smss again |

The editor shows a 409 as "This SMSS changed since you opened it", keeps the user's text, and offers Load Latest, which first warns that the unsaved changes will be lost.

## Error text

Error text is built from internal state: a provider's error body, a Python traceback, or a failed HTTP call that names its URL. SEMOSS removes secrets from it before it leaves the server, with two checks in [SecretRedactor](../../src/prerna/security/SecretRedactor.java):

1. **Recorded values.** When an engine or project opens, it records the values of its secret settings, from its smss and the secret store. Any recorded value in error text is replaced, along with its JSON escaped and URL encoded forms, the token after `Bearer `, and the secret values inside a JSON setting such as `HEADERS`. These are the exact strings the engine loaded, so this works for any provider and any wording. Values shorter than 8 characters are not recorded, since they match ordinary words; the second check still finds them in a secret shape.
2. **Secret shapes.** Bearer tokens, `key=value` and `key: value` pairs with a secret key, JDBC URLs, credentials in a URL (`user:password@host`), and private keys.

Both checks run on:

- the error or warning result of every pixel, whichever way it is returned (`/runPixel`, websockets, MCP, the OpenAI compatible endpoints, Python calling a pixel) and in the insight recipe;
- task errors raised while rows are streamed;
- job error output, which includes the warnings and errors reactors log;
- agent run errors and tool errors;
- Monolith error responses built with `WebUtility.getResponse` (status 400 or more), which the REST resources' error helpers use, and the MCP and A2A JSON-RPC errors.

Normal results, such as file contents, query results and model answers, are what the user asked for from resources they can open, and are sent as they are.

## Model provider errors

When a model provider returns an error, the user gets its status code, what the code means, and a reference:

```
Error code: 403. The model provider denied access. Reference: 0190f6c2-5d1e-7b3a-9c41-2f8e6a0d7b15
```

The provider's own text can name accounts, users, resources and the policies that refused the call, so it goes to the server log under the same reference, with the client, model and traceback:

```
Model error 0190f6c2-5d1e-7b3a-9c41-2f8e6a0d7b15: an error occurred in the ... client with status code 403 for model ...
```

| Status code | What the user is told |
| --- | --- |
| 400 | The model provider rejected the request. |
| 401, 403 | The model provider denied access. |
| 404 | The model provider could not find the model. |
| 408, 504 | The model provider timed out. Try again in a moment. |
| 413 | The request is too large for the model provider. |
| 429 | The model provider is over its rate limit. Try again in a moment. |
| 5xx | The model provider failed. Try again in a moment. |
| Context overflow | The request is longer than the model can take. |

The message starts with `Error code: <code>.`, which the collaboration chat reads to show its one line summary of a failed turn.

## Project reactors

Custom reactors in a project run in a sandbox: [SemossClassLoader](../../src/prerna/util/SemossClassLoader.java) for the project's compiled classes, [SemossProjectJarClassLoader](../../src/prerna/util/SemossProjectJarClassLoader.java) for its jars, and [SemossJarClassLoader](../../src/prerna/util/SemossJarClassLoader.java) for its Maven dependencies. All three apply the same rules; the [project reactor sandbox](project_reactor_sandbox.md) page lists every rule with the test that guards it. Along with the sandbox's limits on reflection, files, environment variables, processes and the network, these keep platform state and secrets out of reach:

- `DIHelper`, `SystemEngineRegistry`, `SecretRedactor`, the secret store connectors in `prerna.io.connector.secrets`, `EngineCloneUtility`, which copies an engine with its stored secrets, and [InternalUtility](../../src/prerna/util/InternalUtility.java) cannot be used, directly or through `Class.forName`. `InternalUtility` holds the helpers that act with the platform's own authority, including everything that reads or changes what `DIHelper` keeps, which only `prerna.util` can reach: the RDF_Map settings as a whole, the smss paths and ids of the engines and projects a server has registered, and the values the desktop tools keep in memory. It also holds loading and registering projects and engines from their smss, loading any user's asset project by id, changing values in properties files, syncing engine metadata to the local master database, reading an engine's smss through the socket server, compiling project Java, downloading a URL to a file, and changing folder permissions.
- Every platform class that requires an admin cannot be used: the classes whose name starts with `Admin`, such as the admin reactors, and `SecurityAdminUtils`.
- `Utility.getDIHelperProperty(key)` returns an RDF_Map setting, and null for every key the RDF_Map editor hides.
- `Utility.loadProperties` reads a file under the same path rule as `new File(...)`: inside the user's folder when `CHROOT_ENABLE` is on.
- An engine's settings (`getSmssProp()`, `getOrigSmssProp()`, `getSmssFilePath()`, `getProperty(key)`, `getConnectionUrl()`, `getDataSource()`) need `OWNER` on the engine, from a custom reactor as from every other caller, and a permission an engine gives an app or another engine never reaches them. A custom reactor that needs one harmless setting, such as a model's name, calls the getter for it. The query utility an RDBMS engine hands out builds SQL, and its user name, password and connection URL stay with the engine. See [engine access](engine_access.md).
- A method reference to a method the sandbox rewrites or refuses, such as `File::getAbsolutePath` or `System::getenv`, behaves exactly like the call it stands for. A method handle constant to one is refused when the class loads.

## Python in a model engine

`EnginePy` runs Python inside a Python model engine's own process, where the engine's settings are filled in. It requires the user to own the engine, the same as reading the engine's smss.
