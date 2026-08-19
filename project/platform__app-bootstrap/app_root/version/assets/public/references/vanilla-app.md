# A platform app with no framework

Two complete starting points that use only `@semoss/sdk`, no React and no UI library:

- **No build** - a single `index.html` in `portals/`, importing the SDK from a CDN. Nothing to
  compile; publish and it runs.
- **Bundled** - a `client/` folder with TypeScript and Vite, built into `portals/`.

Pick no-build for a tool, a dashboard, or an MCP tool UI. Pick bundled when you want types, npm
dependencies, or more than a couple of source files.

---

## 1. No-build HTML app

The whole app is `assets/portals/index.html`. There is no `client/` folder. Import the SDK as an
ESM module from the CDN:

```js
import { Insight } from "https://cdn.jsdelivr.net/npm/@semoss/sdk@latest/+esm";
```

A bare specifier (`import { Insight } from "@semoss/sdk"`) does not resolve in a browser without
a bundler or an import map, so the full URL is required here.

`@latest` tracks the newest published build. That is the right default while developing. Pin the
version (`@semoss/sdk@1.0.0-beta.43`) before anything ships to users, so a future SDK release
cannot change the app under them.

### assets/portals/index.html

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>My App</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 2rem; }
      [hidden] { display: none !important; }
      .error { color: #b00020; }
    </style>
  </head>
  <body>
    <!-- one element per gate state; exactly one is visible at a time -->
    <div id="loading">Loading...</div>

    <div id="error" class="error" hidden></div>

    <form id="login" hidden>
      <h2>Sign in</h2>
      <input id="username" placeholder="Username" autocomplete="username" />
      <input id="password" type="password" placeholder="Password" autocomplete="current-password" />
      <button type="submit">Sign in</button>
      <p id="login-error" class="error" hidden></p>
    </form>

    <main id="app" hidden>
      <button id="logout">Sign out</button>
      <h2>Models</h2>
      <ul id="models"></ul>
    </main>

    <script type="module">
      import { Insight, runPixel, waitForEmbedAuth }
        from "https://cdn.jsdelivr.net/npm/@semoss/sdk@latest/+esm";

      // No Env.update here. The semoss-env tag the server injects on publish
      // supplies APP and MODULE, and initialize() reads it.

      const insight = new Insight();

      // One wrapper for every pixel: always pass the insight id, always check errors.
      const pixel = async (expression) => {
        const { errors, pixelReturn } = await runPixel(expression, insight.insightId);
        if (errors.length) {
          throw new Error(errors.join(", "));
        }
        return pixelReturn[0].output;
      };

      const show = (id) => {
        for (const el of ["loading", "error", "login", "app"]) {
          document.getElementById(el).hidden = el !== id;
        }
      };

      // The gate. Called after initialize, after login, and after logout.
      const render = async () => {
        if (insight.error) {
          document.getElementById("error").textContent = insight.error.message;
          return show("error");
        }
        if (!insight.isInitialized) return show("loading");
        if (!insight.isAuthorized) return show("login");
        if (!insight.isReady) return show("loading");

        show("app");
        await loadAppData();
      };

      // Only ever called once isReady is true, so the insight exists and is bound.
      const loadAppData = async () => {
        try {
          const models = await pixel(`MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`);
          const list = document.getElementById("models");
          list.replaceChildren(
            ...models.map((m) => {
              const li = document.createElement("li");
              li.textContent = m.engine_display_name || m.engine_name;
              return li;
            }),
          );
        } catch (e) {
          // Surface it. An empty catch here is why apps render blank with a clean console.
          document.getElementById("error").textContent = e.message;
          show("error");
        }
      };

      document.getElementById("login").addEventListener("submit", async (event) => {
        event.preventDefault();
        const err = document.getElementById("login-error");
        err.hidden = true;

        // actions.login, not the bare login() - the action rebuilds the insight on success.
        const ok = await insight.actions.login({
          type: "native",
          username: document.getElementById("username").value,
          password: document.getElementById("password").value,
        });

        if (!ok) {
          err.textContent = "Sign in failed";
          err.hidden = false;
          return;
        }

        await render();
      });

      document.getElementById("logout").addEventListener("click", async () => {
        await insight.actions.logout();
        await render();
      });

      // Drop the server-side insight when the tab goes away.
      window.addEventListener("beforeunload", () => {
        insight.destroy();
      });

      // Bootstrap. Embed auth first - it resolves immediately unless this page is
      // embedded with SMSS_EMBED_AUTH=true, and skipping it sends the first
      // request out unauthenticated in that case.
      await waitForEmbedAuth();
      await insight.initialize();
      await render();
    </script>
  </body>
</html>
```

### Publishing

There is nothing to compile, so publish the assets directly:

```
PublishProject(project="<PROJECT_ID>", release=true)
```

Publish is also what injects the `semoss-env` tag into this file, which is where `APP` and
`MODULE` come from. Until the first publish the page has no environment and `initialize()` fails
with `module is required`.

### Local development without a publish

For a no-build app the simplest dev loop is to publish and reload. If you want to iterate against
a local server without publishing, add the tag yourself **temporarily** and delete it before
committing, because publish will overwrite it anyway:

```html
<script id="semoss-env" type="application/json">{"APP":"<project-id>","MODULE":"/Monolith"}</script>
```

Serving that page from a different origin than the server will fail CORS. Either serve it from
the server, or use the bundled setup below, whose dev server proxies `MODULE` for you.

---

## 2. Bundled TypeScript app

Same structure as any platform client app, minus React.

```
assets/
  client/
    .env                 committed: ENDPOINT, MODULE
    .env.local           gitignored: APP, VITE_ACCESS_KEY, VITE_SECRET_KEY
    package.json
    tsconfig.json
    vite.config.ts
    src/
      index.html
      main.ts            bootstrap + gate
      pixel.ts           the one pixel wrapper
      views/             render functions
  portals/               build output
```

### client/package.json

```json
{
  "private": true,
  "scripts": {
    "build": "vite build",
    "dev": "vite"
  },
  "dependencies": {
    "@semoss/sdk": "^1.0.0-beta.43"
  },
  "devDependencies": {
    "typescript": "^5.9.3",
    "vite": "^7.3.1"
  }
}
```

No `react`, no `react-dom`. React is an optional peer dependency of the SDK and nothing on the
core entry touches it.

### client/.env and client/.env.local

```
# .env - committed, no secrets
ENDPOINT="http://localhost:9090/"
MODULE="/Monolith"
```

```
# .env.local - gitignored
APP="<project-id>"
VITE_ACCESS_KEY="<dev access key>"
VITE_SECRET_KEY="<dev secret key>"
```

Note the name: `APP`, not `VITE_APP`. `vite.config.ts` calls `loadEnv(mode, cwd, "")` with an
empty prefix and maps `env.APP` onto `import.meta.env.APP`, so a `VITE_APP` entry is read into
`env` but never defined and reaches the app as `undefined`.

### client/vite.config.ts

```ts
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "") as {
    ENDPOINT: string;
    MODULE: string;
    APP: string;
  };

  return {
    root: "src",
    base: "./",
    envDir: "../",
    define: {
      "import.meta.env.ENDPOINT": JSON.stringify(env.ENDPOINT),
      "import.meta.env.MODULE": JSON.stringify(env.MODULE),
      "import.meta.env.APP": JSON.stringify(env.APP),
    },
    server: {
      // proxies MODULE to the server so dev requests are same-origin
      proxy: {
        [env.MODULE]: {
          target: env.ENDPOINT,
          changeOrigin: true,
          secure: false,
        },
      },
    },
    build: {
      outDir: "../../portals",
      emptyOutDir: true,
    },
  };
});
```

`base: "./"` matters: the portal is served from a project-scoped path, so absolute asset URLs
break.

### client/src/index.html

No `semoss-env` tag - the server injects it into `portals/index.html` on publish.

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>My App</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/main.ts"></script>
  </body>
</html>
```

### client/src/pixel.ts

```ts
import { type Insight, runPixel } from "@semoss/sdk";

/**
 * Build the app's single pixel runner. Every call passes the insight id and
 * checks the envelope, so no caller has to remember to.
 */
export const createPixelRunner = (insight: Insight) => {
  return async <T>(expression: string): Promise<T> => {
    const { errors, pixelReturn } = await runPixel<[T]>(
      expression,
      insight.insightId,
    );

    if (errors.length) {
      throw new Error(errors.join(", "));
    }

    return pixelReturn[0].output;
  };
};

export type PixelRunner = ReturnType<typeof createPixelRunner>;
```

### client/src/main.ts

```ts
import { Env, Insight, waitForEmbedAuth } from "@semoss/sdk";
import { createPixelRunner } from "./pixel";
import { renderApp, renderError, renderLoading, renderLogin } from "./views";

// Module scope, before any Insight exists. In a published build initialize()
// overwrites APP and MODULE from the semoss-env tag; the access keys are empty
// because .env.local never ships.
Env.update({
  MODULE: import.meta.env.MODULE || "",
  APP: import.meta.env.APP || "",
  ACCESS_KEY: import.meta.env.VITE_ACCESS_KEY || "",
  SECRET_KEY: import.meta.env.VITE_SECRET_KEY || "",
});

const root = document.getElementById("root") as HTMLElement;
const insight = new Insight();
const pixel = createPixelRunner(insight);

/**
 * The gate. The three flags are read in order; each one has its own screen.
 * Call this after initialize, after login, and after logout.
 */
const render = async (): Promise<void> => {
  if (insight.error) {
    return renderError(root, insight.error);
  }
  if (!insight.isInitialized) {
    return renderLoading(root);
  }
  if (!insight.isAuthorized) {
    return renderLogin(root, insight, render);
  }
  if (!insight.isReady) {
    return renderLoading(root);
  }

  // isReady is true, so the insight exists and is bound to the project.
  return renderApp(root, insight, pixel, render);
};

window.addEventListener("beforeunload", () => {
  insight.destroy();
});

const bootstrap = async () => {
  // Resolves immediately unless embedded with SMSS_EMBED_AUTH=true.
  await waitForEmbedAuth();

  await renderLoading(root);
  await insight.initialize();
  await render();
};

bootstrap();
```

### client/src/views/index.ts

```ts
export { renderApp } from "./render-app";
export { renderError } from "./render-error";
export { renderLoading } from "./render-loading";
export { renderLogin } from "./render-login";
```

### client/src/views/render-login.ts

```ts
import type { Insight } from "@semoss/sdk";

/**
 * Renders the login form. On success the action rebuilds the insight, so the
 * caller re-runs the gate rather than navigating by hand.
 */
export const renderLogin = async (
  root: HTMLElement,
  insight: Insight,
  render: () => Promise<void>,
): Promise<void> => {
  root.replaceChildren();

  const form = document.createElement("form");
  const username = document.createElement("input");
  const password = document.createElement("input");
  const submit = document.createElement("button");
  const error = document.createElement("p");

  username.placeholder = "Username";
  password.placeholder = "Password";
  password.type = "password";
  submit.type = "submit";
  submit.textContent = "Sign in";
  error.hidden = true;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    submit.disabled = true;
    error.hidden = true;

    try {
      const ok = await insight.actions.login({
        type: "native",
        username: username.value,
        password: password.value,
      });

      if (!ok) {
        error.textContent = "Sign in failed";
        error.hidden = false;
        return;
      }

      await render();
    } finally {
      submit.disabled = false;
    }
  });

  form.append(username, password, submit, error);
  root.append(form);
};
```

`insight.system?.config.availableProviders` lists the configured login options, each with
`provider`, `name`, and `isOauth`. Render a button per provider and call
`insight.actions.login({ type: "oauth", provider })` for the oauth ones. That call opens a popup,
so it has to run inside the click handler or the browser blocks it.

### client/src/views/render-app.ts

```ts
import type { Insight } from "@semoss/sdk";
import type { PixelRunner } from "../pixel";

interface Model {
  engine_id: string;
  engine_name: string;
  engine_display_name: string;
}

/**
 * Renders the signed-in app. Only reached once isReady is true.
 */
export const renderApp = async (
  root: HTMLElement,
  insight: Insight,
  pixel: PixelRunner,
  render: () => Promise<void>,
): Promise<void> => {
  root.replaceChildren();

  const logout = document.createElement("button");
  logout.textContent = "Sign out";
  logout.addEventListener("click", async () => {
    await insight.actions.logout();
    await render();
  });

  const list = document.createElement("ul");
  root.append(logout, list);

  try {
    const models = await pixel<Model[]>(
      `MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`,
    );

    list.replaceChildren(
      ...models.map((model) => {
        const item = document.createElement("li");
        item.textContent = model.engine_display_name || model.engine_name;
        return item;
      }),
    );
  } catch (e) {
    // Surface it rather than leaving an empty list behind.
    const message = document.createElement("p");
    message.textContent = (e as Error).message;
    root.append(message);
  }
};
```

### Building

```
BuildAndPublishApp(project="<PROJECT_ID>")
```

It compiles `client/src` into `portals/`, publishes, and injects the `semoss-env` tag. Do not run
`vite build`, `pnpm build`, or `node` from Bash - node is not available in the sandbox.

---

## Where React would have gone

Nothing above needs a framework. If you later add one, these are the only pieces that change:

| Vanilla | React equivalent |
| --- | --- |
| `new Insight()` at module scope | `<InsightProvider>` |
| `insight.isReady` etc. read directly | `useInsight()` |
| `render()` called after each action | re-render on state change |
| `createPixelRunner(insight)` | the same runner in a context, or `usePixel` |
| `new InsightWebSocket(id)` | `useWebSocket(id)` |
| manual `limit`/`offset` paging | `useIteratorPixel` |

See `references/react-app.md`. The SDK core is identical in both.
