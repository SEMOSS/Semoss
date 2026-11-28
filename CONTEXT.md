# SEMOSS

Shared vocabulary for the SEMOSS platform. Terms are listed once; use them exactly.

## Automation

**Automation**
A typed graph of nodes that the platform runs from a trigger, one node at a time.

**Node**
One step in an Automation. Each node publishes a single output under its output variable.

**API Request node**
A node that sends one HTTP request to an external service and publishes the response. The whole request (method, URL, parameters, headers, body) is defined on the node itself.
_Avoid_: REST node, HTTP node, webhook node.

**Function engine**
A registered, reusable callable resource. A REST function engine is a Function engine that wraps one HTTP endpoint. A Function engine is not an API Request.

**Connection**
The REST function engine an API Request node points at to supply its credentials and permitted host. Choosing a Connection is optional.

**API Response**
What an API Request node publishes: the status code, headers, and body. Every HTTP status code, including 4xx and 5xx, is a normal API Response, not a node failure. A request that never receives a response (timeout, unreachable host, blocked destination) is a node failure.

**Test request**
A one-off send of an API Request node's definition from the editor, before the Automation runs. It uses sample values for any references to other nodes' outputs.

**Allowed destination**
A host an API Request node is permitted to call. Internal addresses (loopback, link-local, private ranges) are never allowed. When an administrator has set an allowlist, the host must also be on it.

**Credential header**
A header that carries a secret, such as `Authorization`, `X-Api-Key` or `Cookie`. Its value on an API Request node must contain a reference to a value (such as a trigger input) or come from the Connection, never a secret typed in directly.

**Redirect**
A 3xx API Response. The node never follows it; the `Location` header is returned for the Automation to act on.
