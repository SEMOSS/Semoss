# Python Code Execution Sandbox
#
# Provides secure sandboxed execution of untrusted Python code using
# nsjail, bubblewrap, or landlock depending on kernel/platform support.
#
# Architecture:
#   Supervisor (this process) -> nsjail/bwrap subprocess -> entrypoint.py
#
# The supervisor speaks the Semoss PayloadStruct protocol so Java can
# connect to it transparently.  Internally it manages one sandboxed
# Python process per user/insight, relaying execution requests and
# streaming results back.

__version__ = "0.1.0"
