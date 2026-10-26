"""
Reconcile the two truststore copies present in the SEMOSS python environment.

Why this module exists
----------------------
Two independent things in this environment want TLS verification to use the
operating system trust store rather than the static certifi bundle, and they
reach for a different copy of the same library to do it:

  * pip-system-certs runs at interpreter startup, via the pip_system_certs.pth
    file that site processes before __main__. It calls truststore's
    inject_into_ssl(), which rebinds ssl.SSLContext to truststore's subclass so
    that every library in the process picks up the system trust store. It uses
    pip's *vendored* copy, pip._vendor.truststore, whenever pip is importable.
    asksageclient depends on this being in place to reach DoD servers.

  * httpx2 / httpcore2, the transport underneath the openai and anthropic
    clients, build their default SSL context from the *standalone* truststore
    distribution.

The two copies are the same code but separate module objects with separate
module level state, and that is what makes the combination fatal. On import,
truststore snapshots the current ssl.SSLContext so it can always reach the real
implementation:

    _original_SSLContext = ssl.SSLContext
    _original_super_SSLContext = super(_original_SSLContext, _original_SSLContext)

The standalone copy is first imported by httpcore2, long after pip-system-certs
has already replaced ssl.SSLContext. So it snapshots the injected *subclass*
instead of the stdlib class, and super() on that subclass resolves back to
ssl.SSLContext.verify_mode, whose body sets the attribute through the module
global ssl.SSLContext, which is the same subclass again. Every TLS handshake
then drives that property into unbounded self recursion:

    File "ssl.py", line 679, in verify_mode
      super(SSLContext, SSLContext).verify_mode.__set__(self, value)
    [Previous line repeated 966 more times]
    RecursionError: maximum recursion depth exceeded

httpx2 reports that as a transport failure, so it surfaces at the far end as
openai.APIConnectionError("Connection error") and an opaque 500 from the model
engine, with nothing in the message pointing at certificates.

What this module does
---------------------
Nothing here concerns certificates. Both copies read the same operating system
trust store and behave identically; the defect is only that one of them holds a
reference to the wrong class. So the repair is to retake that snapshot with a
single copy involved:

  1. Ask the vendored copy to undo itself, which puts the stdlib ssl.SSLContext
     back into the ssl module.
  2. Drop the standalone copy from sys.modules, discarding the bad snapshot it
     may already have taken.
  3. Import the standalone copy again. Its snapshot now records the stdlib
     class, since that is what ssl.SSLContext currently holds.
  4. Let the standalone copy be the one that injects.

One copy ends up owning ssl.SSLContext, and it knows what the real class was.
Both copies remain installed and neither is modified. requests and
asksageclient still verify against the system trust store, and the httpx2 based
clients stop recursing.

Import this before anything that can open a TLS connection. It is idempotent
and is a no-op on environments where the two copies never collide, so it is
safe to import unconditionally.
"""

import ssl
import sys


def reconcile_truststore() -> bool:
    """Collapse the vendored and standalone truststore copies down to one.

    Returns True when the environment was reconciled, False when there was
    nothing to do.
    """
    try:
        from pip._vendor import truststore as vendored
    except ImportError:
        # No vendored copy is reachable, so pip-system-certs either did not run
        # or already injected the standalone copy. Either way it is consistent.
        return False

    if ssl.SSLContext is not vendored.SSLContext:
        # Something other than the vendored copy owns ssl.SSLContext. Leave it
        # alone rather than fighting whoever installed it.
        return False

    # Put the stdlib class back so the standalone copy snapshots the real one.
    vendored.extract_from_ssl()

    # Drop any snapshot the standalone copy already took of the injected class.
    for name in [
        name
        for name in sys.modules
        if name == "truststore" or name.startswith("truststore.")
    ]:
        del sys.modules[name]

    import truststore

    truststore.inject_into_ssl()
    return True


reconciled = reconcile_truststore()
