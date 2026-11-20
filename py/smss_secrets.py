"""Secrets an app declares in its project .smss, as OpenShell placeholders.

The project declares SANDBOX_SECRETS (name + hosts) and sets SANDBOX_SECRET_<NAME>; SEMOSS
brokers each into the user's sandbox when the project is loaded. Code here only ever holds a
placeholder: OpenShell's supervisor swaps it for the value in requests to the secret's declared
hosts (headers, query string) and refuses it anywhere else.

    from smss_secrets import secret
    key = secret("OPENWEATHER_API_KEY")
    requests.get(url, params={"appid": key})
"""

import json
import re

# written by prerna.util.openshell.OpenShellSecrets after every attach
_PLACEHOLDERS = "/opt/semosshome/insight/sandbox-secrets/placeholders.json"


def _project_key(project_id):
    return re.sub(r"[^a-z0-9]", "", project_id.lower())[:8]


def secret(name, project=None):
    """The placeholder for a declared secret.

    Args:
        name: the name in the project's SANDBOX_SECRETS, e.g. "OPENWEATHER_API_KEY".
        project: the project id, needed only when several loaded projects declare the name.
    """
    try:
        with open(_PLACEHOLDERS) as f:
            loaded = json.load(f)
    except FileNotFoundError:
        raise KeyError(
            f"no sandbox secrets are available: declare {name} in the project's "
            "SANDBOX_SECRETS, set its value, and load the project"
        ) from None
    wanted = _project_key(project) if project else None
    found = {
        key: values[name]
        for key, values in loaded.items()
        if name in values and (wanted is None or key == wanted)
    }
    if not found:
        raise KeyError(f"{name} is not a secret of any loaded project")
    if len(found) > 1:
        raise KeyError(f"{name} is declared by several loaded projects; pass project=")
    return next(iter(found.values()))
