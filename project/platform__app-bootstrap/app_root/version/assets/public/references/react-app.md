# A platform app with React

The concrete file set for a new React app, with the contents that matter. Everything here is the
plumbing described in SKILL.md; the app's own features go on top of it.

**React is optional.** `@semoss/sdk/react` is a thin binding over the same core covered in
SKILL.md - `InsightProvider` creates one `Insight` and calls `initialize()`, `useInsight()` reads
its getters, `usePixel` wraps `runPixel`. Use this file when the app already uses React or wants
a component tree. For an app with no framework, see `references/vanilla-app.md`; nothing in the
SDK requires a framework.

## File map

```
assets/
  client/
    .env                     committed: ENDPOINT, MODULE
    .env.local               gitignored: APP, VITE_ACCESS_KEY, VITE_SECRET_KEY
    package.json
    tsconfig.json
    vite.config.ts
    src/
      index.html
      index.tsx
      index.css
      App.tsx
      declarations.d.ts
      routes.constants.ts
      contexts/
        AppContext.tsx
        index.ts
      pages/
        Router.tsx
        HomePage.tsx
        LoginPage.tsx
        ErrorPage.tsx
        layouts/
          InitializedLayout.tsx
          AuthorizedLayout.tsx
          index.ts
        index.ts
      components/
      hooks/
      lib/
  portals/                   build output, do not edit
```

Every folder gets an `index.ts` barrel with explicit named re-exports.

## client/.env

Committed. No secrets.

```
ENDPOINT="http://localhost:9090/"
MODULE="/Monolith"
```

## client/.env.local

Gitignored. The app id is per developer and per server, so it never belongs in `.env`.

```
APP="<project-id>"
VITE_ACCESS_KEY="<dev access key>"
VITE_SECRET_KEY="<dev secret key>"
```

Note the name: `APP`, not `VITE_APP`. `vite.config.ts` calls `loadEnv(mode, cwd, "")` with an
empty prefix and maps `env.APP` onto `import.meta.env.APP`, so a `VITE_APP` entry is read into
`env` but never defined and reaches the app as `undefined`.

## client/vite.config.ts

Two things this file exists to do: define the unprefixed env vars, and proxy `MODULE` to
`ENDPOINT` so dev requests reach the server without CORS.

```ts
import { resolve } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
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
    resolve: { alias: { "@": resolve(__dirname, "./src") } },
    define: {
      "import.meta.env.ENDPOINT": JSON.stringify(env.ENDPOINT),
      "import.meta.env.MODULE": JSON.stringify(env.MODULE),
      "import.meta.env.APP": JSON.stringify(env.APP),
    },
    server: {
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
    plugins: [react(), tailwindcss()],
  };
});
```

`base: "./"` matters: the portal is served from a project-scoped path, so absolute asset URLs
break.

## client/src/declarations.d.ts

Without this, `import.meta.env.APP` is a type error.

```ts
interface ImportMetaEnv {
  readonly ENDPOINT: string;
  readonly MODULE: string;
  readonly APP: string;
  readonly VITE_ACCESS_KEY: string;
  readonly VITE_SECRET_KEY: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

declare module "*.svg" {
  const content: string;
  export default content;
}
```

## client/src/index.html

No `semoss-env` tag. The server injects it into `portals/index.html` on publish.

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>My App</title>
    <link rel="icon" type="image/x-icon" href="/assets/favicon.svg" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/index.tsx"></script>
  </body>
</html>
```

## client/src/App.tsx

```tsx
import { Env } from "@semoss/sdk";
import { InsightProvider } from "@semoss/sdk/react";
import { Toaster } from "sonner";
import { AppContextProvider } from "./contexts";
import { Router } from "./pages";

Env.update({
  MODULE: import.meta.env.MODULE || "",
  APP: import.meta.env.APP || "",
  ACCESS_KEY: import.meta.env.VITE_ACCESS_KEY || "", // undefined in production
  SECRET_KEY: import.meta.env.VITE_SECRET_KEY || "", // undefined in production
});

/**
 * Renders the app inside a single Insight.
 *
 * @component
 */
export const App = () => {
  return (
    <InsightProvider>
      <AppContextProvider>
        <Router />
      </AppContextProvider>
      <Toaster />
    </InsightProvider>
  );
};
```

## client/src/contexts/AppContext.tsx

The one place the pixel envelope is handled. Every feature calls `runPixel` from here.

```tsx
import { getSystemConfig, runPixel as runPixelSdk } from "@semoss/sdk";
import { useInsight } from "@semoss/sdk/react";
import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import { toast } from "sonner";

export interface AppContextType {
  runPixel: <T>(pixel: string, successMessage?: string) => Promise<T>;
  login: (username: string, password: string) => Promise<boolean>;
  logout: () => Promise<boolean>;
  userLoginName: string | null;
  isAppDataLoading: boolean;
}

const AppContext = createContext<AppContextType | undefined>(undefined);

export const useAppContext = (): AppContextType => {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error("useAppContext must be used within an AppContextProvider");
  }
  return context;
};

export const AppContextProvider = ({ children }: PropsWithChildren) => {
  const { actions, isReady, system, insightId } = useInsight();

  const [isAppDataLoading, setIsAppDataLoading] = useState(true);
  const [userLoginName, setUserLoginName] = useState<string | null>(null);

  // Always pass insightId; always surface the error.
  const runPixel = useCallback(
    async <T,>(pixel: string, successMessage?: string): Promise<T> => {
      try {
        const response = await runPixelSdk<[T]>(pixel, insightId);
        if (response.errors.length) {
          throw new Error(response.errors.join(", "));
        }
        if (successMessage) {
          toast.success(successMessage);
        }
        return response.pixelReturn[0].output;
      } catch (error) {
        toast.error(error.message ?? "Error during operation");
        throw error;
      }
    },
    [insightId],
  );

  const login = useCallback(
    async (username: string, password: string) => {
      try {
        await actions.login({ type: "native", username, password });
        const config = await getSystemConfig();
        setUserLoginName(
          Object.values(config?.logins ?? {})[0]?.toString() || null,
        );
        return true;
      } catch {
        return false;
      }
    },
    [actions],
  );

  const logout = useCallback(async () => {
    try {
      await actions.logout();
      setUserLoginName(null);
      return true;
    } catch {
      return false;
    }
  }, [actions]);

  // Load app data off isReady, not off mount. Before isReady there is no insight
  // to run against and the call would create a throwaway one.
  useEffect(() => {
    if (!isReady) {
      return;
    }

    const loadAppData = async () => {
      try {
        await Promise.all([
          // one entry per piece of startup data
        ]);
      } catch (e) {
        toast.error(`Error initializing app data: ${e.message ?? ""}`);
      } finally {
        setIsAppDataLoading(false);
      }
    };

    loadAppData();
  }, [isReady]);

  // Pick up an already-logged-in user on first paint.
  useEffect(() => {
    setUserLoginName(
      Object.values(system?.config?.logins ?? {})[0]?.toString() || null,
    );
  }, [system]);

  return (
    <AppContext.Provider
      value={{ runPixel, login, logout, userLoginName, isAppDataLoading }}
    >
      {children}
    </AppContext.Provider>
  );
};
```

## client/src/pages/Router.tsx

Hash routing, because the portal is served from a static path with no server-side rewrite.

```tsx
import { createHashRouter, Navigate, RouterProvider } from "react-router-dom";
import { ROUTE_PATH_LOGIN_PAGE } from "@/routes.constants";
import { ErrorPage } from "./ErrorPage";
import { HomePage } from "./HomePage";
import { LoginPage } from "./LoginPage";
import { AuthorizedLayout, InitializedLayout } from "./layouts";

const router = createHashRouter([
  {
    Component: InitializedLayout,
    ErrorBoundary: ErrorPage,
    children: [
      {
        Component: AuthorizedLayout,
        ErrorBoundary: ErrorPage,
        children: [{ index: true, Component: HomePage }],
      },
      { path: ROUTE_PATH_LOGIN_PAGE, Component: LoginPage },
      { path: "*", Component: () => <Navigate to="/" /> },
    ],
  },
]);

/**
 * Renders pages based on the url.
 *
 * @component
 */
export const Router = () => <RouterProvider router={router} />;
```

## client/src/pages/layouts/InitializedLayout.tsx

```tsx
import { useInsight } from "@semoss/sdk/react";
import { Outlet } from "react-router-dom";
import { LoadingScreen } from "@/components";
import { ErrorPage } from "../ErrorPage";

/**
 * Holds every route until the system config has loaded.
 *
 * @component
 */
export const InitializedLayout = () => {
  const { isInitialized, error } = useInsight();

  if (error) {
    return <ErrorPage />;
  }
  if (!isInitialized) {
    return <LoadingScreen />;
  }

  return <Outlet />;
};
```

## client/src/pages/layouts/AuthorizedLayout.tsx

```tsx
import { useInsight } from "@semoss/sdk/react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { LoadingScreen } from "@/components";
import { useAppContext } from "@/contexts";
import { ROUTE_PATH_LOGIN_PAGE } from "@/routes.constants";

/**
 * Sends unauthorized users to the login page and holds authorized ones until
 * the app data has loaded.
 *
 * @component
 */
export const AuthorizedLayout = () => {
  const { isAuthorized } = useInsight();
  const { pathname } = useLocation();
  const { isAppDataLoading } = useAppContext();

  if (!isAuthorized) {
    return <Navigate to={ROUTE_PATH_LOGIN_PAGE} state={{ target: pathname }} />;
  }
  if (isAppDataLoading) {
    return <LoadingScreen />;
  }

  return <Outlet />;
};
```

## client/src/pages/LoginPage.tsx

The login page is mounted inside `InitializedLayout` but outside `AuthorizedLayout`, so it can
render for a signed-out user. On success, send the user back to where they were headed:

```tsx
const { login } = useAppContext();
const { state } = useLocation();
const navigate = useNavigate();

const onSubmit = async () => {
  if (await login(username, password)) {
    navigate(state?.target ?? "/");
  }
};
```

## Build

Compile and publish with the `BuildAndPublishApp` tool. It writes `client/src` into `portals/`
and publishes the project, which is when the server injects the `semoss-env` tag. See the
`build-and-publish` skill. Do not run `pnpm build` or `node` from Bash.

## Without a build step

If the app does not need compiling there is no `client/` folder at all: put `index.html` straight
into `portals/`, import the SDK from the CDN, and publish with `PublishProject`. See
`references/vanilla-app.md` for the complete version.

```html
<script type="module">
  import { Insight } from "https://cdn.jsdelivr.net/npm/@semoss/sdk@latest/+esm";
</script>
```
