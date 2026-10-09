"""Apply the upstream fsspec ReferenceFileSystem Jinja2 sandboxing fix
(GHSA-27vj-qcqg-25rc / CVE-2026-104851, fsspec commit 8643878) to the pinned
fsspec release, in lieu of forcing fsspec>=2026.6.0 through the full
datasets>=5.1.0 / pyarrow>=24.0.0 upgrade chain that version alone requires.
SEMOSS does not use fsspec's ReferenceFileSystem/Kerchunk implementation
anywhere, so this targeted patch closes the CVE without touching the
carefully-pinned pyarrow/datasets Python 3.14 compatibility chain.
"""
import hashlib
import sys
from pathlib import Path

EXPECTED_SHA256 = "c2d771b8894632e70d425cbc3fae326bca80bea6fee650655deca350381dbd66"

REPLACEMENTS = [
    (
        '''        if not self.simple_templates or self.templates:
            import jinja2
        self.references = {}
        self._process_templates(references.get("templates", {}))

        @lru_cache(1000)
        def _render_jinja(u):
            return jinja2.Template(u).render(**self.templates)
''',
        '''        if not self.simple_templates or self.templates:
            import jinja2.sandbox
        self.references = {}
        self._process_templates(references.get("templates", {}))

        @lru_cache(1000)
        def _render_jinja(u):
            return (
                jinja2.sandbox.SandboxedEnvironment()
                .from_string(u)
                .render(**self.templates)
            )
''',
    ),
    (
        '''            if "{{" in v:
                import jinja2

                self.templates[k] = lambda temp=v, **kwargs: jinja2.Template(
                    temp
                ).render(**kwargs)
            else:
''',
        '''            if "{{" in v:
                import jinja2.sandbox

                self.templates[k] = (
                    lambda temp=v, **kwargs: jinja2.sandbox.SandboxedEnvironment()
                    .from_string(temp)
                    .render(**kwargs)
                )
            else:
''',
    ),
    (
        '''            for pr in products:
                import jinja2

                key = jinja2.Template(gen["key"]).render(**pr, **self.templates)
                url = jinja2.Template(gen["url"]).render(**pr, **self.templates)
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.Template(gen["offset"]).render(**pr, **self.templates)
                    )
                    length = int(
                        jinja2.Template(gen["length"]).render(**pr, **self.templates)
                    )
''',
        '''            for pr in products:
                import jinja2.sandbox

                key = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["key"])
                    .render(**pr, **self.templates)
                )
                url = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["url"])
                    .render(**pr, **self.templates)
                )
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["offset"])
                        .render(**pr, **self.templates)
                    )
                    length = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["length"])
                        .render(**pr, **self.templates)
                    )
''',
    ),
]


def patch_reference(path):
    original = path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest != EXPECTED_SHA256:
        raise ValueError("Unexpected fsspec reference.py source hash: " + digest)
    text = original.decode("utf-8")
    for old, new in REPLACEMENTS:
        if text.count(old) != 1:
            raise ValueError("fsspec compatibility patch target must occur exactly once")
        text = text.replace(old, new, 1)
    compile(text, "reference.py", "exec")
    updated = text.encode("utf-8")
    path.write_bytes(updated)
    return {
        "path": "fsspec/implementations/reference.py",
        "change": "Sandbox the three unrestricted jinja2.Template(...).render(...) "
                  "calls in ReferenceFileSystem (GHSA-27vj-qcqg-25rc / CVE-2026-104851)",
        "original_sha256": digest,
        "patched_sha256": hashlib.sha256(updated).hexdigest(),
    }


if __name__ == "__main__":
    venv = sys.argv[1] if len(sys.argv) > 1 else "/opt/semoss-python"
    candidates = list(Path(venv).glob("lib/python3.*/site-packages/fsspec/implementations/reference.py"))
    if len(candidates) != 1:
        raise ValueError("Expected exactly one fsspec reference.py under " + venv + ", found " + str(len(candidates)))
    import json
    print(json.dumps(patch_reference(candidates[0])))
