# Web Search system MCP

Project ID: `web-search`. It is off by default, because it needs a SearXNG service
that not every instance runs. With `WEB_SEARCH_ENABLED true` in `RDF_Map.prop`,
SEMOSS registers this project at startup as a global MCP with the `MCP` and `SYSTEM`
tags. It is not attached to any agent or to Collaboration; add it to an agent
workspace or room through the MCP catalog.

It gives any model web access, including models without a built-in search tool
(Laguna, Nova, Claude on Bedrock):

- `web_search` searches through a SearXNG instance and returns titles, URLs, and
  snippets.
- `web_fetch` reads a public web page or PDF and returns its text as markdown, in
  parts for long pages. It reads public addresses only: it refuses localhost,
  private networks, and cloud metadata addresses, including after redirects.

## Run SearXNG locally

From this folder:

```bash
docker run -d --name searxng -p 8888:8080 \
  -v "$PWD/searxng:/etc/searxng" \
  -e SEARXNG_SECRET="$(openssl rand -hex 32)" \
  docker.io/searxng/searxng:latest
```

Check that it answers JSON:

```bash
curl -s "http://localhost:8888/search?q=semoss&format=json" | head -c 300
```

`searxng/settings.yml` turns on the JSON format the tools need and turns off the
limiter, which would otherwise answer SEMOSS's calls with HTTP 429.

## Run SearXNG on Kubernetes

The Kubernetes manifests (a ConfigMap with these settings, a Secret, a Deployment,
and a Service) live in the [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment)
repo under `optional-services/third-party/searxng/`, and that repo's README has the
steps. There SEMOSS reaches SearXNG at
`http://searxng-service.semoss.svc.cluster.local:8080`. The ConfigMap carries a copy
of `searxng/settings.yml`, so change both together.

## Point SEMOSS at SearXNG

The driver reads `SEARXNG_URL` on every call, using this order:

1. `SEARXNG_URL` in `platform__web-search.smss.local` (ignored by Git).
2. `SEARXNG_URL` in `platform__web-search.smss`.
3. The SEMOSS server's `SEARXNG_URL` environment variable. `USE_PYTHON project` in
   the SMSS runs these tools in the project's own Python process, which starts without
   the per-user chroot and inherits SEMOSS's environment.
4. `http://localhost:8888`, which matches the docker command above.

If SEMOSS itself runs in Docker, `localhost` is the SEMOSS container; use
`http://host.docker.internal:8888` instead.

`web_fetch` checks each address with a DNS lookup before connecting, so the Python
runtime needs DNS for public hostnames. On a network that only allows web traffic
through a proxy, with no public DNS, it refuses every page.

## Installation

On each instance that can reach a SearXNG service, add this line to `RDF_Map.prop`
and restart SEMOSS to register the system project:

```properties
WEB_SEARCH_ENABLED	true
```

With the key missing or not `true`, SEMOSS skips the project at startup and while
running, so no agent or room can add its tools. A server that is not in cluster mode
also drops it from the catalog if an earlier start had registered it. Changes to the
driver or `SEARXNG_URL` are read on subsequent tool calls.

## Limits

SearXNG gets its results by querying public search engines (Google, Bing,
DuckDuckGo, Brave, and others). Those engines sometimes throttle it, especially
from cloud servers; searches then come back empty, and `web_search` lists which
engines did not respond. Automated querying is also against those engines' terms
of service, so confirm that is acceptable wherever this runs.
