"""Secrets an app declares in its project .smss.

The project declares SANDBOX_SECRETS (name + hosts) and sets SANDBOX_SECRET_<NAME>; SEMOSS
hands each to the user's sandbox when the project is loaded:

- OpenShell: code only ever holds a placeholder. OpenShell's supervisor swaps it for the value in
  requests to the secret's declared hosts (headers, query string) and refuses it anywhere else.
- fakechroot and namespace jails: code gets the value itself, from a file in that user's jail.
  The declared hosts are not enforced there.

    from smss_secrets import secret
    key = secret("OPENWEATHER_API_KEY")
    requests.get(url, params={"appid": key})

Pass the value whole, as a header or query value. A value the code transforms first, such as a
password given to requests' auth=(user, password), is base64 encoded before it leaves, so
OpenShell never sees the placeholder to swap it; store the whole "Basic ..." header value instead.
"""

import json
import os
import re

# written by prerna.util.sandbox.JailSecrets: the SEMOSS home is the folder this module's py folder
# is in, and the file is under it as the jail sees it. SEMOSS makes the folder when it sets up a
# fakechroot or namespace jail, before any project is loaded, so the folder marks a jail.
_JAIL_SECRETS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sandbox-secrets", "secrets.json"
)
# written by prerna.util.openshell.OpenShellSecrets after every attach
_PLACEHOLDERS = "/opt/semosshome/insight/sandbox-secrets/placeholders.json"


def _project_key(project_id):
    return re.sub(r"[^a-z0-9]", "", project_id.lower())[:8]


def _load():
    # In a jail, read only the jail's own file. The OpenShell file is in the shared insight cache,
    # which other users can write to, so a jail never falls back to it.
    in_jail = os.path.isdir(os.path.dirname(_JAIL_SECRETS))
    path = _JAIL_SECRETS if in_jail else _PLACEHOLDERS
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def secret(name, project=None):
    """The value, or on OpenShell the placeholder, for a declared secret.

    Args:
        name: the name in the project's SANDBOX_SECRETS, e.g. "OPENWEATHER_API_KEY".
        project: the project id, needed only when several loaded projects declare the name.
    """
    loaded = _load()
    if loaded is None:
        raise KeyError(
            f"no sandbox secrets are available: declare {name} in the project's "
            "SANDBOX_SECRETS, set its value, and load the project"
        )
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
