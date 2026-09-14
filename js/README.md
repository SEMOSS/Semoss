# SEMOSS Node.js Agent Execution Environment

This folder holds the agent-only Node.js code-execution worker, the Node analog
of `py/gaas_tcp_socket_server.py`. Unlike the python environment, it is NOT
user-facing: there is no Pixel reactor and no REST endpoint. The only entry
point is the `ExecuteNodeCode` agent platform tool (see
`prerna.reactor.agent.runtime.PlatformAgentToolHandlers`).

## Contents

- `gaas_node_worker.js` - the worker process. Speaks the same length-prefixed
  JSON protocol as the python gaas server, so the Java side reuses
  `NativePySocketClient`. Each insight gets its own `worker_threads` thread
  holding a persistent `vm` context; timeouts and interrupts are
  `worker.terminate()` (a hard kill).
- `node_env/` - the curated package environment. `package.json` +
  `package-lock.json` define exactly what agent code may `require()`. Install
  at deploy/image-build time with:

  ```
  cd node_env && npm ci --omit=dev
  ```

  `node_modules/` is not committed. There is deliberately no runtime
  `npm install` - to add a package, change `package.json`, regenerate the
  lockfile, and redeploy.

  Prefer pure-JS packages. The one native exception is `sharp` (image
  rasterization for the pptx skill's icon pipeline): it installs prebuilt
  binaries per platform - no compile step - but `npm ci` must run on the
  target platform (or in the target Docker image) with registry access so the
  right prebuild is fetched. The pptx authoring stack (`pptxgenjs`, `react`,
  `react-dom`, `react-icons`, `sharp`) mirrors the JS dependencies of the
  Anthropic pptx skill.

## Configuration (RDF_Map.prop)

- `NODE_HOME` - node install root; `/bin/node` (unix) or `/node.exe` (windows)
  is appended. Required.
- `AGENT_DEFAULT_TOOLS_ENABLE_NODE` - `true` to register the ExecuteNodeCode
  agent tool. Off by default. The blanket `DISABLE_TERMINAL` kill switch also
  disables it.
- `NODE_ENV_DIR` - override the curated environment folder (defaults to
  `<BaseFolder>/js/node_env`).
- `NODE_PERMISSION_FLAGS` - optional raw flags inserted before the worker
  script, e.g. the node permission model for your node version:
  `--permission --allow-fs-read=... --allow-fs-write=... --allow-worker`.
  The worker itself needs read access to this folder and read/write on its
  insight scratch folder, plus `--allow-worker` for its executor threads.
- `NODE_IDLE_TIMEOUT` - idle minutes before a user's worker self-exits
  (default 30).

## Sandboxing

On Linux with `SANDBOX_MODE=NAMESPACE`, the worker is launched through
`py/sandbox_launcher.py --exec-cmd <node> --exec-script gaas_node_worker.js`
inside the same unprivileged namespace + seccomp jail used for python, over a
unix domain socket. A `NODE_HOME` outside `/usr,/bin,/lib` is bound read-only
into the jail automatically. On macOS/Windows dev boxes there is no OS jail;
use `NODE_PERMISSION_FLAGS` for defense in depth.
