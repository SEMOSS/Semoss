"""
Notebook style inline rendering of matplotlib figures for the SEMOSS python worker.

Why this module exists
----------------------
User scripts run on a socketserver worker thread inside a long lived process
(see TCPServerHandler.handle_python). Matplotlib, left to its own devices,
autoselects an *interactive* backend whenever one is importable - "macosx" on a
Mac, "TkAgg"/"QtAgg" on a workstation. Driving a GUI toolkit from a non main
thread is not merely unsupported, it is fatal:

  * macOS: plt.show() reaches Cocoa off the main thread and the process calls
    abort(). No python exception is raised, so the BaseException handler in
    execute_and_capture cannot report it - the whole worker just dies.
  * Tk/Qt: either "main thread is not in main loop", or a blocking show() that
    waits forever for a window nobody can close. Since user processes run with
    --max_count 1, one hung execution makes the worker look permanently dead.

So this module does two things:

  1. Pins the headless "Agg" backend before matplotlib can pick anything else.
  2. Reproduces jupyter's inline behaviour on top of Agg: plt.show() renders the
     open figures instead of popping a window, and any figure still open when
     the execution finishes is rendered too (the equivalent of ipykernel's
     post execute flush_figures hook).

Each rendered figure is emitted as a single stdout line holding an HTML img
element with a base64 data URI:

    <img src='data:image/png;base64,....'>

The emit callback is supplied per execution by the handler and writes to the
normal stdout channel, so images interleave with print() output in the order
they were produced.

Figures are always closed once rendered. That is what jupyter does, and it also
stops figures leaking across executions - insight globals are long lived, so
without it pyplot's global figure registry grows forever and one insight's
leftover figure bleeds into the next execution's output.
"""

import base64
import io
import os
import sys
import threading
import warnings

_HEADLESS_BACKEND = "Agg"

# Backends that render to a buffer or a file and never touch a GUI toolkit.
# Anything outside this set needs a window, and a window opened from a worker
# thread either hangs the execution or kills the process.
_SAFE_BACKENDS = frozenset(
    [
        "agg",
        "cairo",
        "pdf",
        "pgf",
        "ps",
        "svg",
        "template",
        "module://matplotlib_inline.backend_inline",
    ]
)

# Rendered PNGs travel inline over the socket as base64, so cap the size rather
# than letting a huge figure balloon the payload (base64 adds ~33% on top).
_MAX_IMAGE_BYTES = 8 * 1024 * 1024

# Guards the one time monkeypatching of pyplot / Figure.
_install_lock = threading.Lock()
_installed = False
# maybe_install() is reachable from builtins.__import__, and the patching below
# can itself trigger imports. This flag makes the re-entrant call a no-op -
# without it the nested call blocks on _install_lock, which the same thread
# already holds, and the worker deadlocks on the first matplotlib import.
_installing = False
# Stage one (the backend switch guard) completes independently of stage two
# (inline rendering), since matplotlib can be imported long before pyplot is.
_use_guarded = False

# Per execution state. Each worker thread runs one execution at a time, so the
# emit callback and the enabled flag are thread local by nature.
_local = threading.local()


def pin_headless_backend():
    """
    Force the non interactive Agg backend.

    Sets MPLBACKEND for any matplotlib that has not been imported yet, and
    calls matplotlib.use() for one that already has. Safe to call repeatedly -
    the socket server calls it again after the chroot path wipes os.environ.
    """
    os.environ["MPLBACKEND"] = _HEADLESS_BACKEND

    # Belt and braces for the case where show() somehow runs unpatched: Agg's
    # own show() warns rather than doing anything, and that warning would show
    # up in the user's console as noise they can do nothing about.
    warnings.filterwarnings(
        "ignore", message=".*is non-interactive, and thus cannot be shown.*"
    )

    mpl = sys.modules.get("matplotlib")
    if mpl is not None:
        try:
            mpl.use(_HEADLESS_BACKEND, force=True)
        except Exception:
            # An already drawn canvas can refuse a backend switch. The env var
            # still governs every later interpreter, so this is not fatal.
            pass


# Pin as early as possible - importing this module is enough.
pin_headless_backend()


def _state():
    if not hasattr(_local, "emit"):
        _local.emit = None
        _local.enabled = True
        _local.rendered = []
    return _local


def begin_execution(emit):
    """
    Arm inline display for the current thread's execution.

    Rendered images are collected rather than sent straight out, because they
    belong to the execution's return value - what PyReactor hands back - not to
    the console stream. The caller drains them with take_rendered(). The emit
    callback is still used for plain console messages, and as a fallback for
    images that cannot ride the return value.

    Args:
        emit (`callable`): called with a single already formatted console line.
    """
    state = _state()
    state.emit = emit
    state.enabled = True
    state.rendered = []
    # matplotlib may have been imported by an earlier execution, or through a
    # path that bypasses builtins.__import__ (importlib.import_module does not
    # go through it), so re-check here rather than relying on the import hook.
    maybe_install()


def end_execution():
    """Disarm inline display for the current thread."""
    state = _state()
    state.emit = None
    state.enabled = True
    state.rendered = []


def take_rendered():
    """
    Hand over everything rendered during this execution and reset.

    Returns:
        `list`: the html fragments, in the order they were produced.
    """
    state = _state()
    rendered = state.rendered
    state.rendered = []
    return rendered


def set_enabled(enabled=True):
    """
    Injected into insight globals as smss_inline_display().

    Lets a caller opt out of automatic figure rendering for the current
    execution - used by the legacy PyPlotReactor / CollectSeabornReactor paths,
    which save and return the figure themselves and would otherwise also get an
    inline copy on stdout.
    """
    _state().enabled = bool(enabled)


def is_armed():
    """True while a user execution is in progress on this thread."""
    return _state().emit is not None


def emit_text(message):
    """
    Write a plain console line for the current execution.

    Used by smss_headless_guards to explain why a popup was suppressed. No-op
    outside an execution - there is nowhere to send it.
    """
    state = _state()
    if state.emit is not None:
        state.emit(message)


def emit_image_bytes(raw, mime="image/png"):
    """
    Add already encoded image bytes to this execution's rendered output.

    The rendering entry point for every non matplotlib library - PIL, opencv,
    plotly and anything added later all encode to bytes and hand them here, so
    there is exactly one wire format for the frontend to recognise.

    Args:
        raw (`bytes`): the encoded image.
        mime (`str`): its mime type.

    Returns:
        `bool`: True if it was collected.
    """
    state = _state()
    if state.emit is None or not state.enabled or not raw:
        return False
    state.rendered.append(_image_html(raw, mime))
    return True


def is_installed():
    """True once the pyplot / Figure patches are in place."""
    return _installed


def maybe_install():
    """
    Patch matplotlib in two stages, each as soon as its prerequisite exists.

    Stage one guards backend switching and needs only the matplotlib package.
    Stage two installs the inline rendering and needs pyplot plus figure. They
    are separate because a script can call matplotlib.use("TkAgg") before it
    ever imports pyplot, and that call has to be refused too.

    A worker that never plots pays only a couple of dict lookups.
    """
    global _installed, _installing, _use_guarded
    if _installing or (_installed and _use_guarded):
        return

    # Everything is read out of sys.modules rather than imported. An import
    # statement here would re-enter this function through the patched
    # builtins.__import__ that calls it.
    matplotlib = sys.modules.get("matplotlib")
    if matplotlib is None:
        return
    plt = sys.modules.get("matplotlib.pyplot")
    mpl_figure = sys.modules.get("matplotlib.figure")

    with _install_lock:
        if _installing:
            return
        _installing = True
        try:
            # A module lands in sys.modules before it finishes executing, and
            # our caller is often matplotlib's own import graph, so a module
            # can be visible while the attribute we want does not exist yet.
            # Skipping leaves the stage flag False so a later call retries.
            if not _use_guarded and hasattr(matplotlib, "use"):
                _guard_backend_switching(matplotlib, plt)
                _use_guarded = True

            if (
                not _installed
                and plt is not None
                and mpl_figure is not None
                and hasattr(mpl_figure, "Figure")
                and hasattr(plt, "show")
            ):
                _install_inline_rendering(mpl_figure, plt)
                _installed = True
        except Exception:
            # Never let our patching break a user's import. flush_figures
            # still renders on its own since it only reads sys.modules.
            pass
        finally:
            _installing = False


def _is_safe_backend(backend):
    return str(backend).strip().lower() in _SAFE_BACKENDS


def _refuse_backend(backend, call):
    state = _state()
    if state.emit is not None:
        state.emit(
            "[inline display] ignoring "
            + call
            + "('"
            + str(backend)
            + "'): this python process has no display and interactive backends "
            + "crash it. Staying on "
            + _HEADLESS_BACKEND
            + " - plots are rendered inline automatically."
        )


def _guard_backend_switching(matplotlib, plt):
    """
    Refuse any switch to an interactive backend.

    A user calling matplotlib.use("TkAgg") would re-arm the exact crash this
    module exists to prevent, and it would take down every other insight
    sharing the process.

    use() and switch_backend() get separate wrappers on purpose: use()
    delegates to pyplot.switch_backend internally, so pointing both at the same
    wrapper would recurse forever.
    """
    try:
        matplotlib.use(_HEADLESS_BACKEND, force=True)
    except Exception:
        pass

    _orig_use = matplotlib.use

    def _guarded_use(backend, *args, **kwargs):
        if _is_safe_backend(backend):
            return _orig_use(backend, *args, **kwargs)
        _refuse_backend(backend, "matplotlib.use")

    matplotlib.use = _guarded_use

    # pyplot may not be imported yet. When it is, stage two picks this up.
    if plt is not None and hasattr(plt, "switch_backend"):
        _guard_switch_backend(plt)


def _guard_switch_backend(plt):
    _orig_switch_backend = plt.switch_backend

    def _guarded_switch_backend(backend, *args, **kwargs):
        if _is_safe_backend(backend):
            return _orig_switch_backend(backend, *args, **kwargs)
        _refuse_backend(backend, "plt.switch_backend")

    _guarded_switch_backend._smss_guarded = True
    plt.switch_backend = _guarded_switch_backend


def _install_inline_rendering(mpl_figure, plt):
    """Apply the inline rendering patches. Called once, under _install_lock."""
    # Stage one may have run before pyplot existed.
    if hasattr(plt, "switch_backend") and not getattr(
        plt.switch_backend, "_smss_guarded", False
    ):
        _guard_switch_backend(plt)

    # Stamp every figure with the thread that created it. Several insight
    # threads share this process and pyplot's figure registry is global, so
    # without an owner a flush would render and close figures belonging to a
    # concurrently running execution.
    _orig_figure_init = mpl_figure.Figure.__init__

    def _owning_figure_init(self, *args, **kwargs):
        _orig_figure_init(self, *args, **kwargs)
        try:
            self._smss_owner = threading.get_ident()
        except Exception:
            pass

    mpl_figure.Figure.__init__ = _owning_figure_init

    # plt.show() renders and closes, exactly like the notebook inline backend.
    # Under plain Agg it would be a no-op that only warns.
    def _inline_pyplot_show(*args, **kwargs):
        flush_figures()

    plt.show = _inline_pyplot_show

    # fig.show() renders just that figure.
    def _inline_figure_show(self, *args, **kwargs):
        _emit_figure(self)

    mpl_figure.Figure.show = _inline_figure_show


def _image_html(raw, mime="image/png"):
    """
    Wrap encoded image bytes in the one console format the frontend renders.

    Oversized images come back as a plain message instead - the payload travels
    inline over the socket, and base64 adds a third on top of it.
    """
    if len(raw) > _MAX_IMAGE_BYTES:
        return (
            "[inline display] image is "
            + str(len(raw))
            + " bytes, above the "
            + str(_MAX_IMAGE_BYTES)
            + " byte inline limit. Write it to a file instead."
        )
    encoded = base64.b64encode(raw).decode("ascii")
    return "<img src='data:" + mime + ";base64," + encoded + "'>"


def _render_png(fig):
    """Render a figure to an HTML img element, or to an error string."""
    buffer = io.BytesIO()
    try:
        fig.savefig(buffer, format="png", bbox_inches="tight")
    except Exception as e:
        return "[inline display] could not render figure: " + str(e)

    return _image_html(buffer.getvalue())


def _emit_figure(fig, close=True):
    """Render one figure into this execution's output and close it."""
    state = _state()
    if state.emit is None or not state.enabled:
        return

    # A bare plt.figure() with nothing drawn on it is not worth a blank image.
    if not getattr(fig, "axes", None):
        if close:
            _close_figure(fig)
        return

    try:
        state.rendered.append(_render_png(fig))
    finally:
        if close:
            _close_figure(fig)


def _close_figure(fig):
    plt = sys.modules.get("matplotlib.pyplot")
    if plt is None:
        return
    try:
        plt.close(fig)
    except Exception:
        pass


def flush_figures():
    """
    Render every figure this thread has open, then close them.

    Called by the patched plt.show() and again by the handler once the
    execution finishes, which is what makes a script that never calls show()
    still produce its chart - the notebook behaves the same way.
    """
    state = _state()
    if state.emit is None or not state.enabled:
        return

    plt = sys.modules.get("matplotlib.pyplot")
    if plt is None:
        return

    try:
        fignums = plt.get_fignums()
    except Exception:
        return
    if not fignums:
        return

    me = threading.get_ident()
    for num in fignums:
        fig = _figure_for_num(plt, num)
        if fig is None:
            continue
        owner = getattr(fig, "_smss_owner", None)
        # Unowned figures predate the patch, so claim them; anything owned by
        # another thread belongs to a concurrent execution and is left alone.
        if owner is not None and owner != me:
            continue
        _emit_figure(fig)


def _figure_for_num(plt, num):
    """
    Resolve a figure number without making it current.

    plt.figure(num) would work but has the side effect of changing the active
    figure, which we do not want while sweeping up after user code.
    """
    try:
        manager = plt._pylab_helpers.Gcf.get_fig_manager(num)
        if manager is not None:
            return manager.canvas.figure
    except Exception:
        pass
    try:
        return plt.figure(num)
    except Exception:
        return None
