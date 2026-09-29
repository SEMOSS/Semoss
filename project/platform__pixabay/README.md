# Pixabay system MCP

Project ID: `pixabay`. SEMOSS registers this project at startup as a global MCP
with the `MCP` and `SYSTEM` tags, alongside `node-builder` and the other platform
MCPs. It is attached to the system PPTX Agent by default and can be added to other
agent workspaces through the MCP catalog.

The `search_pixabay_images` tool searches Pixabay and returns image URLs,
metadata, and attribution links. API responses are cached for 24 hours.

## API key

Create `project/platform__pixabay.smss.local` beside the project's SMSS file:

```properties
API_KEY your-pixabay-api-key
```

This local override is ignored by Git. The driver reads the key on every call,
using this order:

1. `API_KEY` in `platform__pixabay.smss.local`.
2. `API_KEY` in `platform__pixabay.smss`.
3. The Python runtime's `PIXABAY_API_KEY` environment variable.

The Python runtime must be able to read the SMSS files. For deployments that
expose only project assets to Python, configure the environment variable.
Keep the tracked SMSS file free of credentials. The key is supplied by the driver
and is not part of the MCP tool's arguments.

## Installation

Deploy the platform project assets and SMSS together with the SEMOSS build that
includes `pixabay` in `SystemDefaultEngines.getSystemMCPs()`. Restart SEMOSS to
register the system project. Changes to the driver or key are read on subsequent
tool calls.
